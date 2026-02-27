import threading
import queue
import time
import os
from pathlib import Path
from typing import Iterable
from ..core.config import get
from ..utils.media_exts import get_media_extensions


class WatchService:
    """Observa rutas y encola paths nuevos/modificados para ser escaneados.

    - No parsea nombres
    - No llama a providers
    - Emite rutas a `out_queue`
    """

    def __init__(self, roots: Iterable[str] | None = None, out_queue: queue.Queue | None = None):
        self.roots = list(roots or get('media_roots') or [])
        self.out_queue = out_queue or queue.Queue()
        self._stop = threading.Event()
        self._thread = None
        self.poll = int(get('watch_poll_interval') or 5)
        self._exts = tuple(get_media_extensions())
        # Avoid a massive "first poll" storm on startup: we snapshot existing files per root
        # and only emit events for changes after initialization.
        self._initialized_roots: set[str] = set()
        # Track known media files to detect additions/modifications without rescanning everything downstream.
        # path -> (mtime, size)
        self._known: dict[str, tuple[int | None, int | None]] = {}
        # Debounce pending changes until file becomes stable (avoid Windows copy locks / partial sizes).
        # path -> (mtime, size, last_changed_ts)
        self._pending: dict[str, tuple[int | None, int | None, float]] = {}
        try:
            self.debounce_s = float(get('watch_debounce_seconds') or 3)
        except Exception:
            self.debounce_s = 3.0

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name='watcher')
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _is_media_file(self, path: str) -> bool:
        try:
            p = str(path).lower()
            return p.endswith(self._exts)
        except Exception:
            return False

    def _run(self):
        # Simple polling implementation to avoid an external dependency.
        # Detects:
        # - new media files (added)
        # - modified media files (mtime/size changed)
        #
        # Moved/renamed files are seen as "deleted + added". Deletions are not emitted here
        # (scanner can optionally reconcile missing files separately if desired).
        while not self._stop.is_set():
            now = time.time()
            for root in list(self.roots):
                try:
                    p = Path(root)
                    if not p.exists():
                        continue

                    try:
                        root_norm = str(p.resolve())
                    except Exception:
                        root_norm = os.path.abspath(str(p))

                    seen_this_poll: set[str] = set()

                    for dirpath, dirnames, filenames in os.walk(p):
                        if self._stop.is_set():
                            break
                        for name in filenames:
                            full = os.path.join(dirpath, name)
                            if not self._is_media_file(full):
                                continue
                            try:
                                norm = str(Path(full).resolve())
                            except Exception:
                                norm = os.path.abspath(full)
                            seen_this_poll.add(norm)

                            try:
                                st = os.stat(norm)
                                mtime = int(st.st_mtime)
                                size = int(st.st_size)
                            except Exception:
                                # If file is locked/not ready, keep it pending but do not emit yet.
                                mtime, size = None, None

                            prev = self._known.get(norm)
                            if prev is not None and prev[0] == mtime and prev[1] == size:
                                continue

                            # First time we see this root, just snapshot current media files
                            # (so watcher doesn't enqueue everything right after startup).
                            if root_norm not in self._initialized_roots:
                                self._known[norm] = (mtime, size)
                                # drop any pending state for this path (fresh snapshot)
                                self._pending.pop(norm, None)
                                continue

                            # Debounce: only emit after file has been stable for debounce_s seconds.
                            pend = self._pending.get(norm)
                            if pend is None:
                                self._pending[norm] = (mtime, size, now)
                                continue
                            pm, ps, last_ts = pend
                            if pm != mtime or ps != size:
                                self._pending[norm] = (mtime, size, now)
                                continue
                            # stable snapshot, but require non-None stat to avoid emitting locked file
                            if (now - last_ts) >= self.debounce_s and mtime is not None and size is not None:
                                self._known[norm] = (mtime, size)
                                self._pending.pop(norm, None)
                                self.out_queue.put(norm)

                    # Mark this root initialized once we've completed a full walk.
                    try:
                        self._initialized_roots.add(root_norm)
                    except Exception:
                        pass

                    # Drop entries that no longer exist on disk to prevent unbounded growth.
                    # Emit missing paths so the worker can cleanup DB records.
                    try:
                        missing = [k for k in self._known.keys() if k.startswith(str(p.resolve())) and k not in seen_this_poll]
                        if missing:
                            try:
                                self.out_queue.put({"action": "missing_batch", "paths": list(missing)})
                            except Exception:
                                pass
                        for k in missing:
                            self._known.pop(k, None)
                    except Exception:
                        pass

                    # Drop pending entries that disappeared
                    try:
                        missing_p = [k for k in self._pending.keys() if k.startswith(str(p.resolve())) and k not in seen_this_poll]
                        for k in missing_p:
                            self._pending.pop(k, None)
                    except Exception:
                        pass
                except Exception:
                    pass
            time.sleep(self.poll)

    def add_root(self, path: str):
        if path not in self.roots:
            self.roots.append(path)

    def set_roots(self, paths: Iterable[str]):
        self.roots = list(paths)
