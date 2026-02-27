from __future__ import annotations

from fastapi import FastAPI

from . import server as legacy


def create_app() -> FastAPI:
    """App factory (PR1).

    Keep behavior *identical* by returning the existing FastAPI app defined in
    `src.api.server`. This avoids route duplication / double-mount edge cases.
    """
    return legacy.app
