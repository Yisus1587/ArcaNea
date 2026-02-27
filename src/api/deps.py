from __future__ import annotations

from typing import Iterator

from sqlalchemy.orm import Session
from fastapi import Header, HTTPException

from ..db import SessionLocal
import os
from .admin_auth import validate_admin_token, get_required_admin_pin


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def admin_required(
    x_arcanea_manager: str | None = Header(default=None, alias="X-Arcanea-Manager"),
    x_arcanea_role: str | None = Header(default=None, alias="X-Arcanea-Role"),
    x_arcanea_admin_token: str | None = Header(default=None, alias="X-Arcanea-Admin-Token"),
    admin_token: str | None = None,
):
    role = (x_arcanea_role or "").strip().lower()
    mgr = (x_arcanea_manager or "").strip().lower()
    pin_required = get_required_admin_pin()
    if pin_required:
        supplied = (x_arcanea_admin_token or admin_token or "").strip()
        if not validate_admin_token(supplied):
            raise HTTPException(status_code=403, detail="forbidden")
        return True
    if role == "admin" or mgr in ("1", "true", "yes"):
        return True
    raise HTTPException(status_code=403, detail="forbidden")
