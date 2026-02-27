from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Depends

from ...tools.apply_db_migrations import main as apply_db_migrations_main
from ...services.localize_runner import start_localization_job, request_stop, is_running
from ...services.localize_state import get_state
from ..deps import admin_required


router = APIRouter()


@router.get("/api/localize/status")
def localize_status():
    try:
        st = get_state()
        st["running"] = bool(is_running() or st.get("running"))
        return st
    except Exception:
        logging.getLogger(__name__).exception("Failed computing localize status")
        return {"running": False}


@router.post("/api/localize/start")
def localize_start(limit_series: int | None = None, limit_seasons: int | None = None, _: bool = Depends(admin_required)):
    """Start the TMDB localization job (non-blocking)."""
    try:
        try:
            apply_db_migrations_main()
        except Exception:
            logging.getLogger(__name__).exception("DB migration failed before localization")
        started = start_localization_job(limit_series=limit_series, limit_seasons=limit_seasons)
        return {"started": bool(started)}
    except Exception:
        logging.getLogger(__name__).exception("Unexpected error starting localization")
        raise HTTPException(status_code=500, detail="Unexpected error")


@router.post("/api/localize/stop")
def localize_stop(_: bool = Depends(admin_required)):
    try:
        request_stop()
        return {"stop_requested": True}
    except Exception as e:
        return {"stop_requested": False, "error": str(e)}
