from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles


class SPAStaticFiles(StaticFiles):
    """StaticFiles with SPA fallback.

    - Serves real files from dist (including PWA assets: sw.js, manifest, icons)
    - Falls back to index.html for client-side routes (no extension)
    - Does NOT interfere with /api, /docs, /openapi.json
    """

    async def get_response(self, path: str, scope):  # type: ignore[override]
        resp = await super().get_response(path, scope)
        try:
            if getattr(resp, "status_code", None) != 404:
                return resp
        except Exception:
            return resp

        try:
            req_path = (scope or {}).get("path") or ""
        except Exception:
            req_path = ""

        if req_path.startswith("/api") or req_path.startswith("/docs") or req_path.startswith("/openapi.json"):
            return resp

        try:
            base = os.path.basename(req_path)
            if "." in base:
                return resp
        except Exception:
            pass

        try:
            return await super().get_response("index.html", scope)
        except Exception:
            return resp


def maybe_mount_frontend(app: FastAPI) -> None:
    """Mount frontend dist at '/' if enabled and available.

    Enable with: ARCANEA_SERVE_FRONTEND=1
    Optional path: ARCANEA_FRONTEND_DIST=<abs path to dist>
    """
    try:
        enabled = str(os.environ.get("ARCANEA_SERVE_FRONTEND", "0")).strip().lower() in ("1", "true", "yes")
    except Exception:
        enabled = False
    if not enabled:
        return

    try:
        dist_env = os.environ.get("ARCANEA_FRONTEND_DIST", "") or ""
    except Exception:
        dist_env = ""

    here = Path(__file__).resolve()
    repo_root = here.parent.parent.parent  # src/api/*.py -> repo root
    default_dist = (repo_root / "arcanea-media-server" / "dist").resolve()

    dist_dir = Path(dist_env).resolve() if dist_env else default_dist
    try:
        if not dist_dir.is_dir():
            logging.getLogger(__name__).warning("ARCANEA_SERVE_FRONTEND=1 but dist not found: %s", dist_dir)
            return
    except Exception:
        return

    try:
        app.mount("/", SPAStaticFiles(directory=str(dist_dir), html=True), name="frontend")
        logging.getLogger(__name__).info("Serving frontend from %s", dist_dir)
    except Exception:
        logging.getLogger(__name__).exception("Failed mounting frontend dist")

