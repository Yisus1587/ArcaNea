from __future__ import annotations

import logging
import queue as _queue
import threading
import time
from typing import Iterable

from sqlalchemy.orm import Session

from ..db import SessionLocal, models
from ..scanner.scanner import Scanner
from ..services.enrichment_runner import enrich_one_serialized
from ..services.scan_state import set_state as set_scan_state
from ..utils.path_hash import hash_path

_watcher_service = None
_watcher_queue: _queue.Queue | None = None
_watcher_worker_thread: threading.Thread | None = None
_watcher_stop = threading.Event()
_watcher_pause = threading.Event()
_watcher_last_enrich: dict[int, float] = {}
try:
    _WATCHER_ENRICH_DEBOUNCE_SEC = float(__import__("os").environ.get("ARCANEA_WATCH_ENRICH_DEBOUNCE_SEC", "5") or 5)
except Exception:
    _WATCHER_ENRICH_DEBOUNCE_SEC = 5.0


def watcher_is_paused() -> bool:
    return bool(_watcher_pause.is_set())


def watcher_pause() -> None:
    _watcher_pause.set()


def watcher_resume() -> None:
    _watcher_pause.clear()


def watcher_set_roots(paths: Iterable[str]) -> None:
    global _watcher_service
    try:
        if _watcher_service is not None:
            _watcher_service.set_roots([p for p in paths if p])
    except Exception:
        logging.getLogger(__name__).exception("watcher.set_roots failed")


def watcher_set_roots_from_db(db: Session) -> None:
    try:
        items = db.query(models.MediaRoot).all()
        watcher_set_roots([str(i.path) for i in items if getattr(i, "path", None)])
    except Exception:
        logging.getLogger(__name__).exception("watcher_set_roots_from_db failed")


def watcher_start_if_enabled() -> None:
    """Start the filesystem watcher + background worker if ARCANEA_WATCH is enabled."""
    try:
        # Env var override (explicit). If not set, fall back to DB setting `watch_enabled`.
        watch_env = __import__("os").environ.get("ARCANEA_WATCH")
        watch_mode = str(watch_env).strip().lower() if watch_env is not None else ""
    except Exception:
        watch_mode = ""

    if watch_mode:
        enabled = watch_mode in ("1", "true", "yes", "on")
    else:
        # DB fallback: enable watcher when onboarding is complete unless explicitly disabled.
        enabled = True
        try:
            db0 = SessionLocal()
            try:
                row = db0.query(models.Setting).filter(models.Setting.key == "watch_enabled").first()
                if row and getattr(row, "value", None) is not None:
                    import json as _json

                    raw = getattr(row, "value", "")
                    try:
                        v = _json.loads(raw) if isinstance(raw, str) else raw
                    except Exception:
                        v = raw
                    if isinstance(v, bool):
                        enabled = bool(v)
                    elif isinstance(v, str) and v.strip():
                        enabled = v.strip().lower() in ("1", "true", "yes", "on")
            finally:
                db0.close()
        except Exception:
            enabled = True

    if not enabled:
        logging.getLogger(__name__).info("Watcher disabled")
        return

    global _watcher_service, _watcher_queue, _watcher_worker_thread
    if _watcher_worker_thread and _watcher_worker_thread.is_alive():
        return

    try:
        from ..watcher.service import WatchService
    except Exception:
        logging.getLogger(__name__).exception("Watcher requested but WatchService import failed")
        return

    _watcher_stop.clear()
    _watcher_queue = _queue.Queue()
    _watcher_service = WatchService(out_queue=_watcher_queue)
    _watcher_service.start()

    def _cleanup_missing_paths(paths: Iterable[str]) -> None:
        log = logging.getLogger(__name__)
        db = SessionLocal()
        try:
            for p in paths:
                try:
                    fr = db.query(models.FileRecord).filter(models.FileRecord.path == str(p)).first()
                    if not fr:
                        continue
                    mid = getattr(fr, "media_item_id", None)
                    try:
                        db.delete(fr)
                        db.flush()
                    except Exception:
                        db.rollback()
                        continue

                    if mid:
                        remaining = db.query(models.FileRecord.id).filter(models.FileRecord.media_item_id == mid).first()
                        if not remaining:
                            db.query(models.MediaMetadata).filter(models.MediaMetadata.media_item_id == mid).delete(
                                synchronize_session=False
                            )
                            db.query(models.MediaImage).filter(models.MediaImage.media_item_id == mid).delete(
                                synchronize_session=False
                            )
                            db.query(models.MediaItem).filter(models.MediaItem.id == mid).delete(
                                synchronize_session=False
                            )
                except Exception:
                    log.exception("Failed cleanup for missing path %s", p)
            db.commit()
        except Exception:
            db.rollback()
            log.exception("Missing-path cleanup failed")
        finally:
            db.close()

    def _watcher_worker() -> None:
        log = logging.getLogger(__name__)
        scanner = Scanner()
        while not _watcher_stop.is_set():
            while _watcher_pause.is_set() and not _watcher_stop.is_set():
                time.sleep(0.25)
            try:
                item = _watcher_queue.get(timeout=0.5) if _watcher_queue is not None else None
            except Exception:
                continue
            if item is None:
                break
            if isinstance(item, dict) and item.get("action") == "missing_batch":
                paths = item.get("paths") or []
                if isinstance(paths, list) and paths:
                    _cleanup_missing_paths(paths)
                continue
            try:
                # Refresh roots before each scan.
                try:
                    if _watcher_service is not None and getattr(_watcher_service, "roots", None):
                        from pathlib import Path as _Path

                        scanner.roots = [_Path(r).resolve() for r in list(getattr(_watcher_service, "roots", [])) if r]
                except Exception:
                    pass
                scanner.scan_path(str(item))
                try:
                    set_scan_state({"watch": {"path": str(item), "ts": int(time.time())}})
                except Exception:
                    pass
                try:
                    from pathlib import Path as _Path

                    resolved = None
                    try:
                        resolved = str(_Path(str(item)).resolve())
                    except Exception:
                        resolved = str(item)

                    mid = None
                    dbx = SessionLocal()
                    try:
                        if resolved:
                            fr = dbx.query(models.FileRecord).filter(models.FileRecord.path == resolved).first()
                        else:
                            fr = None
                        if fr and getattr(fr, "media_item_id", None):
                            try:
                                mid = int(getattr(fr, "media_item_id"))
                            except Exception:
                                mid = None
                        if mid is None and resolved:
                            mi = dbx.query(models.MediaItem).filter(models.MediaItem.canonical_path == resolved).first()
                            if not mi and resolved:
                                h = hash_path(resolved)
                                if h:
                                    mi = dbx.query(models.MediaItem).filter(models.MediaItem.canonical_path_hash == h).first()
                            if mi is not None:
                                try:
                                    mid = int(getattr(mi, "id"))
                                except Exception:
                                    mid = None
                    finally:
                        dbx.close()
                    if mid is not None:
                        now = time.time()
                        last = _watcher_last_enrich.get(mid, 0.0)
                        if (now - last) >= _WATCHER_ENRICH_DEBOUNCE_SEC:
                            _watcher_last_enrich[mid] = now
                            try:
                                enrich_one_serialized(mid)
                            except Exception:
                                log.exception("Watcher per-item enrich failed for %s", mid)
                except Exception:
                    log.exception("Watcher per-item enrich lookup failed for %s", item)
            except Exception:
                log.exception("Watcher worker failed for %s", item)

    _watcher_worker_thread = threading.Thread(target=_watcher_worker, daemon=True, name="watcher-worker")
    _watcher_worker_thread.start()
    logging.getLogger(__name__).info("Watcher enabled")


def watcher_stop() -> None:
    """Stop watcher gracefully (if enabled)."""
    global _watcher_service, _watcher_queue, _watcher_worker_thread
    try:
        _watcher_stop.set()
        try:
            if _watcher_queue is not None:
                _watcher_queue.put(None)
        except Exception:
            pass
        try:
            if _watcher_service is not None:
                _watcher_service.stop()
        except Exception:
            pass
        _watcher_service = None
        _watcher_queue = None
        _watcher_worker_thread = None
    except Exception:
        logging.getLogger(__name__).exception("Failed stopping watcher")
