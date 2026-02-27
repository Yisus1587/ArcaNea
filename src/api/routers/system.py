from __future__ import annotations

import io
import logging
import os
import time
import zipfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from ...core import config as core_config
from ...db import SessionLocal
from ...services.enrichment_runner import (
    is_paused as is_enrich_paused,
    set_paused as set_enrich_paused,
)
from .. import runtime
from ..deps import admin_required

router = APIRouter()


@router.get("/api/system/status")
def system_status():
    return {
        "watcher_paused": runtime.watcher_is_paused(),
        "enrich_paused": bool(is_enrich_paused()),
    }


@router.post("/api/system/pause")
def system_pause():
    """Pause watcher + enrichment (used by system tray)."""
    try:
        runtime.watcher_pause()
    except Exception:
        pass
    try:
        set_enrich_paused(True)
    except Exception:
        pass
    return system_status()


@router.post("/api/system/resume")
def system_resume():
    try:
        runtime.watcher_resume()
    except Exception:
        pass
    try:
        set_enrich_paused(False)
    except Exception:
        pass
    return system_status()


@router.get("/api/health")
def healthcheck():
    """Lightweight health endpoint for UI/tray diagnostics."""
    try:
        db_ok = True
        db_detail = None
        try:
            db = SessionLocal()
            try:
                db.execute(text("SELECT 1"))
            finally:
                db.close()
        except Exception as e:
            db_ok = False
            db_detail = str(e)

        data_dir = str(core_config.DATA_DIR)
        runtime_root = str(Path(core_config.DATA_DIR).resolve().parent)
        return {
            "ok": bool(db_ok),
            "db_ok": bool(db_ok),
            "db_detail": db_detail,
            "data_dir": data_dir,
            "runtime_root": runtime_root,
            "watcher_paused": runtime.watcher_is_paused(),
            "enrich_paused": bool(is_enrich_paused()),
            "ts": int(time.time()),
        }
    except Exception as e:
        return {"ok": False, "detail": str(e), "ts": int(time.time())}


def _default_log_path() -> Path:
    try:
        rr = Path(core_config.DATA_DIR).resolve().parent
    except Exception:
        rr = Path.cwd().resolve()
    return (rr / "logs" / "arcanea.log").resolve()


@router.get("/api/logs/info")
def logs_info(_: bool = Depends(admin_required)):
    p = _default_log_path()
    return {"path": str(p), "exists": bool(p.exists()), "size": int(p.stat().st_size) if p.exists() else 0}


@router.get("/api/logs/tail")
def logs_tail(lines: int = 200, _: bool = Depends(admin_required)):
    """Return the last N lines of the main log file (best-effort)."""
    try:
        n = int(lines) if lines is not None else 200
        if n <= 0:
            n = 200
        n = min(n, 2000)
    except Exception:
        n = 200

    p = _default_log_path()
    if not p.exists():
        return {"path": str(p), "lines": [], "error": "not_found"}

    try:
        with open(p, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            block = 8192
            data = b""
            while size > 0 and data.count(b"\n") <= n + 5:
                step = block if size >= block else size
                size -= step
                fh.seek(size, os.SEEK_SET)
                data = fh.read(step) + data
                if size == 0:
                    break
        text_out = data.decode("utf-8", errors="replace")
        parts = [ln for ln in text_out.splitlines() if ln is not None]
        out_lines = parts[-n:]
        return {"path": str(p), "lines": out_lines}
    except Exception as e:
        logging.getLogger(__name__).exception("Failed tailing log")
        return {"path": str(p), "lines": [], "error": str(e)}


def _sqlite_path_from_url(url: str) -> Path | None:
    try:
        if not isinstance(url, str):
            return None
        u = url.strip()
        if not u.lower().startswith("sqlite:///"):
            return None
        raw = u[len("sqlite:///") :]
        return Path(raw).resolve()
    except Exception:
        return None


@router.get("/api/backup/export")
def export_backup(_: bool = Depends(admin_required)):
    """Download a ZIP backup (db + app_config)."""
    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            try:
                cfg = Path(core_config.DATA_DIR).resolve() / "app_config.json"
                if cfg.exists():
                    z.write(str(cfg), arcname="data/app_config.json")
            except Exception:
                pass
            try:
                db_url = core_config.config.get("db_url") or ""
                dbp = _sqlite_path_from_url(str(db_url))
                if dbp and dbp.exists():
                    z.write(str(dbp), arcname="db/arcanea.db")
            except Exception:
                pass
        buf.seek(0)
        ts = time.strftime("%Y%m%d-%H%M%S")
        fn = f"arcanea-backup-{ts}.zip"
        return StreamingResponse(
            buf,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename=\"{fn}\"'},
        )
    except Exception as e:
        logging.getLogger(__name__).exception("Failed exporting backup")
        raise HTTPException(status_code=500, detail=str(e))
