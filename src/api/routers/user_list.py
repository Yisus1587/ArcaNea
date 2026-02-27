from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from ...db import models
from ..deps import get_db

router = APIRouter()


def _require_profile_id(x_arcanea_profile_id: str | None = Header(default=None, alias="X-Arcanea-Profile-Id")) -> str:
    pid = str(x_arcanea_profile_id or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="profile_id_required")
    return pid


@router.get("/api/user-list")
def get_user_list(db: Session = Depends(get_db), profile_id: str = Depends(_require_profile_id)):
    rows = (
        db.query(models.UserList)
        .filter(models.UserList.profile_id == profile_id)
        .all()
    )
    ids = [r.media_item_id for r in rows]
    return {"ok": True, "items": ids}


@router.get("/api/user-list/{item_id}")
def get_user_list_item(item_id: int, db: Session = Depends(get_db), profile_id: str = Depends(_require_profile_id)):
    row = (
        db.query(models.UserList)
        .filter(models.UserList.profile_id == profile_id)
        .filter(models.UserList.media_item_id == item_id)
        .first()
    )
    return {"ok": True, "in_list": bool(row)}


@router.post("/api/user-list/{item_id}/toggle")
def toggle_user_list(item_id: int, db: Session = Depends(get_db), profile_id: str = Depends(_require_profile_id)):
    row = (
        db.query(models.UserList)
        .filter(models.UserList.profile_id == profile_id)
        .filter(models.UserList.media_item_id == item_id)
        .first()
    )
    if row:
        db.delete(row)
        try:
            db.commit()
        except Exception:
            db.rollback()
            raise HTTPException(status_code=500, detail="db_commit_failed")
        return {"ok": True, "in_list": False}
    row = models.UserList(profile_id=profile_id, media_item_id=item_id)
    db.add(row)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="db_commit_failed")
    return {"ok": True, "in_list": True}
