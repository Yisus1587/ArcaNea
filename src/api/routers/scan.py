from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import APIRouter, BackgroundTasks, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse

from ...db import SessionLocal
from ...services.enrich_state import get_state as get_enrich_state
from ...services.scan_state import get_state as get_scan_state, set_state as set_scan_state
from ...tools.apply_db_migrations import main as apply_db_migrations_main
from ..services import scan as scan_service
from ..services import status as status_service
from ..deps import admin_required
from ..admin_auth import validate_admin_token
import os

router = APIRouter()


@router.post("/api/scan")
def trigger_scan(background_tasks: BackgroundTasks, _: bool = Depends(admin_required)):
    try:
        try:
            apply_db_migrations_main()
        except Exception as e:
            logging.getLogger(__name__).exception("DB migration failed before scan")
            return JSONResponse({"status": "migration_failed", "detail": str(e)}, status_code=500)
    except Exception:
        logging.getLogger(__name__).exception("Unexpected error while attempting migrations")

    try:
        s = get_scan_state()
    except Exception:
        s = {}
    if s.get("status") == "scanning":
        return JSONResponse({"status": "already_running"}, status_code=202)
    set_scan_state({"status": "queued"})
    background_tasks.add_task(scan_service.scan_all_roots)
    return {"status": "accepted"}


@router.post("/api/scan/path")
def scan_path(payload: dict, _: bool = Depends(admin_required)):
    try:
        s = get_scan_state()
    except Exception:
        s = {}
    if s.get("status") == "scanning":
        return JSONResponse({"status": "already_running"}, status_code=202)
    try:
        path = str(payload.get("path") or "").strip()
    except Exception:
        path = ""
    if not path:
        return JSONResponse({"status": "bad_request", "detail": "path_required"}, status_code=400)
    res = scan_service.scan_single_path(path)
    if res.get("status") == "not_under_roots":
        return JSONResponse(res, status_code=400)
    if res.get("status") == "error":
        return JSONResponse(res, status_code=500)
    return res


@router.get("/api/scan/status")
def scan_status(_: bool = Depends(admin_required)):
    try:
        s = get_scan_state()
    except Exception:
        s = {"status": "idle"}
    try:
        enrich = get_enrich_state()
        s["enrichment"] = enrich
    except Exception:
        s["enrichment"] = {"running": False}
    return s


@router.websocket("/api/ws/status")
async def ws_status(ws: WebSocket):
    """Push scan/enrichment status snapshots in real-time."""
    pin_required = (os.environ.get("ARCANEA_ADMIN_PIN") or "").strip()
    if pin_required:
        token = (ws.headers.get("x-arcanea-admin-token") or ws.query_params.get("admin_token") or "").strip()
        if not validate_admin_token(token):
            await ws.close(code=1008)
            return
    await ws.accept()
    db = SessionLocal()
    last_payload = None
    last_counts_at = 0.0
    counts_cache: dict = {}
    try:
        while True:
            now = time.time()
            if (now - last_counts_at) >= 2.0:
                try:
                    counts_cache = status_service.compute_enrich_counts(db)
                except Exception:
                    counts_cache = {}
                last_counts_at = now
            snap = status_service.status_snapshot(None)
            snap["enrich_counts"] = counts_cache
            payload = json.dumps(snap, ensure_ascii=False, separators=(",", ":"))
            if payload != last_payload:
                await ws.send_text(payload)
                last_payload = payload
            await asyncio.sleep(0.75)
    except WebSocketDisconnect:
        return
    except Exception:
        logging.getLogger(__name__).exception("ws_status crashed")
    finally:
        try:
            db.close()
        except Exception:
            pass
