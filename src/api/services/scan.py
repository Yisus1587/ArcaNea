from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from ...db import SessionLocal, models
from ...scanner.scanner import Scanner
from ...services.dispatch_enrichment import dispatch_enrichment_by_roots
from ...services.enrich_state import set_state as set_enrich_state
from ...services.enrichment_runner import is_running, start_enrichment_job
from ...services.localize_runner import start_localization_job
from ...services import enrichment as enrichment_service
from ...services.scan_state import set_state as set_scan_state


def scan_single_path(path: str) -> dict:
    """Scan a single path (file or folder) without touching other roots."""
    log = logging.getLogger(__name__)
    if not path:
        return {"status": "bad_request", "detail": "path_required"}
    scanner = Scanner()
    try:
        p = Path(str(path)).resolve()
    except Exception:
        p = Path(str(path))
    try:
        if not scanner._is_under_roots(p):
            # Allow scan if path is a configured MediaRoot (freshly added) even
            # when runtime config hasn't picked it up yet.
            try:
                dbx = SessionLocal()
                try:
                    rows = dbx.query(models.MediaRoot).all()
                finally:
                    dbx.close()
                root_hits = []
                for r in rows:
                    try:
                        rp = Path(str(getattr(r, "path", "") or "")).resolve()
                    except Exception:
                        rp = Path(str(getattr(r, "path", "") or ""))
                    if not str(rp):
                        continue
                    try:
                        if str(p).startswith(str(rp)):
                            root_hits.append(rp)
                    except Exception:
                        pass
                if root_hits:
                    for rp in root_hits:
                        try:
                            if rp not in scanner.roots:
                                scanner.roots.append(rp)
                        except Exception:
                            pass
                if not root_hits:
                    return {"status": "not_under_roots", "detail": str(p)}
            except Exception:
                return {"status": "not_under_roots", "detail": str(p)}
    except Exception:
        return {"status": "not_under_roots", "detail": str(p)}
    total = 0
    try:
        if p.is_file():
            total = 1
        elif p.is_dir():
            for root, _dirs, files in os.walk(p):
                try:
                    if any(os.path.join(root, f).lower().endswith(tuple(scanner.media_exts)) for f in files):
                        total += 1
                except Exception:
                    continue
    except Exception:
        total = 0

    set_scan_state(
        {
            "status": "scanning",
            "total": total,
            "processed": 0,
            "current": str(p),
            "started_at": int(time.time()),
            "finished_at": None,
        }
    )
    try:
        scanner.scan_path(str(p))
        set_scan_state({"status": "idle", "current": None, "finished_at": int(time.time())})
        return {"status": "ok", "path": str(p)}
    except Exception as e:
        log.exception("scan_single_path failed for %s", p)
        set_scan_state({"status": "idle", "current": None, "finished_at": int(time.time())})
        return {"status": "error", "detail": str(e), "path": str(p)}

def scan_path_with_pipeline(path: str) -> dict:
    """Scan a single path and then run enrichment + localization pipeline."""
    res = scan_single_path(path)
    if not isinstance(res, dict) or res.get("status") != "ok":
        return res
    try:
        set_enrich_state({"running": True, "current_step": "dispatching", "current_id": None, "current_title": None})
    except Exception:
        pass
    try:
        report = dispatch_enrichment_by_roots()
        logging.getLogger(__name__).info("Dispatch enrichment report (single path): %s", report)
    except Exception:
        logging.getLogger(__name__).exception("Phased dispatch failed (single path)")
    try:
        if not is_running():
            started = bool(start_enrichment_job())
            if not started:
                try:
                    start_localization_job()
                except Exception:
                    logging.getLogger(__name__).exception("Localization job failed to start after dispatch (single path)")
    except Exception:
        logging.getLogger(__name__).exception("Failed starting enrichment runner (single path)")
    return res


def scan_all_roots() -> None:
    """Run a scan over configured roots (blocking).

    Default behavior is *incremental* to avoid re-processing every file on every scan.
    Set `ARCANEA_SCAN_MODE=full` to force a full scan.
    """
    log = logging.getLogger(__name__)
    scanner = Scanner()
    db = SessionLocal()
    last_total: int | None = None
    last_processed: int | None = None
    try:
        rows = db.query(models.MediaRoot).all()

        scan_debug = str(os.environ.get("ARCANEA_SCAN_DEBUG", "")).strip().lower() in ("1", "true", "yes", "on", "debug")

        mode = str(os.environ.get("ARCANEA_SCAN_MODE", "incremental")).strip().lower()
        if scan_debug:
            log.debug("[scan-debug] scan_all_roots mode=%s roots=%d exts=%s", mode, len(rows), scanner.media_exts)

        if mode in ("full", "all"):
            files_to_process: list[str] = []
            for r in rows:
                try:
                    for root, _dirs, files in os.walk(r.path):
                        for f in files:
                            full = os.path.join(root, f)
                            try:
                                if any(full.lower().endswith(ext) for ext in scanner.media_exts):
                                    files_to_process.append(full)
                            except Exception:
                                continue
                except Exception:
                    log.exception("Failed enumerating files under %s", r.path)

            set_scan_state(
                {
                    "status": "scanning",
                    "total": len(files_to_process),
                    "processed": 0,
                    "current": None,
                    "started_at": int(time.time()),
                    "finished_at": None,
                }
            )
            last_total = len(files_to_process)
            last_processed = 0

            for idx, full in enumerate(files_to_process):
                set_scan_state({"current": full})
                try:
                    scanner._upsert_file(full)
                except Exception:
                    log.exception("Failed processing file %s", full)
                set_scan_state({"processed": idx + 1})
                last_processed = idx + 1
        else:
            set_scan_state(
                {
                    "status": "scanning",
                    "total": 0,
                    "processed": 0,
                    "current": None,
                    "started_at": int(time.time()),
                    "finished_at": None,
                }
            )
            last_total = 0
            last_processed = 0

            total = 0
            processed = 0

            def _chunks(seq: list[int], size: int = 500):
                for i in range(0, len(seq), size):
                    yield seq[i : i + size]

            for r in rows:
                root_path = getattr(r, "path", None)
                if not root_path:
                    continue
                try:
                    root_abs = str(Path(str(root_path)).resolve())
                except Exception:
                    root_abs = str(root_path)

                existing: dict[str, tuple[int | None, int | None, int | None, int | None]] = {}
                try:
                    like_prefix = f"{root_abs}%"
                    q = db.query(
                        models.FileRecord.path,
                        models.FileRecord.mtime,
                        models.FileRecord.size,
                        models.FileRecord.id,
                        models.FileRecord.media_item_id,
                    ).filter(models.FileRecord.path.like(like_prefix))
                    for p, mtime, size, fid, mid in q.yield_per(5000):
                        try:
                            existing[str(p)] = (
                                int(mtime) if mtime is not None else None,
                                int(size) if size is not None else None,
                                int(fid) if fid is not None else None,
                                int(mid) if mid is not None else None,
                            )
                        except Exception:
                            existing[str(p)] = (None, None, None, None)
                except Exception:
                    existing = {}

                seen_files: set[str] = set()

                for dirpath, _dirnames, filenames in os.walk(root_abs):
                    try:
                        changed_files: list[str] = []
                        for fn in filenames:
                            full = os.path.join(dirpath, fn)
                            try:
                                if not any(full.lower().endswith(ext) for ext in scanner.media_exts):
                                    continue
                            except Exception:
                                continue

                            try:
                                norm = str(Path(full).resolve())
                            except Exception:
                                norm = os.path.abspath(full)
                            seen_files.add(norm)

                            try:
                                st = os.stat(norm)
                                mtime = int(st.st_mtime)
                                size = int(st.st_size)
                            except Exception:
                                mtime, size = None, None

                            prev = existing.get(norm)
                            if prev is not None and prev[0] == mtime and prev[1] == size:
                                continue
                            changed_files.append(norm)

                        if not changed_files:
                            continue
                        if scan_debug:
                            log.debug("[scan-debug] dir changed dir=%s files=%d", dirpath, len(changed_files))

                        total += len(changed_files)
                        last_total = total
                        set_scan_state({"total": total, "current": dirpath})

                        try:
                            scanner._upsert_folder(dirpath, changed_files)
                        except Exception:
                            log.exception("Failed processing folder %s", dirpath)

                        processed += len(changed_files)
                        last_processed = processed
                        set_scan_state({"processed": processed})
                    except Exception:
                        log.exception("Failed scanning directory %s", dirpath)

                # Cleanup missing files (deleted or moved/renamed)
                try:
                    if existing:
                        stale = [info for path, info in existing.items() if path not in seen_files]
                    else:
                        stale = []
                    if stale:
                        stale_ids = [info[2] for info in stale if info[2] is not None]
                        stale_media_ids = {info[3] for info in stale if info[3] is not None}
                        if stale_ids:
                            for chunk in _chunks(stale_ids, 500):
                                db.query(models.FileRecord).filter(models.FileRecord.id.in_(chunk)).delete(
                                    synchronize_session=False
                                )
                        if stale_media_ids:
                            orphan_ids: list[int] = []
                            for mid in stale_media_ids:
                                if mid is None:
                                    continue
                                remaining = (
                                    db.query(models.FileRecord.id)
                                    .filter(models.FileRecord.media_item_id == mid)
                                    .first()
                                )
                                if not remaining:
                                    orphan_ids.append(int(mid))
                            for chunk in _chunks(orphan_ids, 200):
                                db.query(models.MediaMetadata).filter(
                                    models.MediaMetadata.media_item_id.in_(chunk)
                                ).delete(synchronize_session=False)
                                db.query(models.MediaImage).filter(
                                    models.MediaImage.media_item_id.in_(chunk)
                                ).delete(synchronize_session=False)
                                db.query(models.MediaItem).filter(models.MediaItem.id.in_(chunk)).delete(
                                    synchronize_session=False
                                )
                        if stale_ids:
                            db.commit()
                except Exception:
                    db.rollback()
                    log.exception("Failed cleanup of missing files under %s", root_abs)

        set_scan_state({"status": "idle", "current": None, "finished_at": int(time.time())})
        try:
            set_enrich_state({"running": True, "current_step": "dispatching", "current_id": None, "current_title": None})
        except Exception:
            pass

        try:
            report = dispatch_enrichment_by_roots()
            logging.getLogger(__name__).info("Dispatch enrichment report: %s", report)
        except Exception:
            logging.getLogger(__name__).exception("Phased dispatch failed")

        try:
            if not is_running():
                started = bool(start_enrichment_job())
                # If the dispatcher already processed everything (no pending items),
                # run TMDB sync + episode backfill once so localization happens automatically.
                if not started:
                    try:
                        start_localization_job()
                    except Exception:
                        logging.getLogger(__name__).exception("Localization job failed to start after dispatch")
        except Exception:
            logging.getLogger(__name__).exception("Failed starting enrichment runner")
    finally:
        try:
            payload = {"status": "idle", "current": None, "finished_at": int(time.time())}
            if last_total is not None:
                payload["total"] = last_total
            if last_processed is not None:
                payload["processed"] = last_processed
            set_scan_state(payload)
        except Exception:
            pass
        db.close()
