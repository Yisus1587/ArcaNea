from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ...db import models
from ...services import manual_mapping
from ..deps import get_db, admin_required

router = APIRouter()


def _require_key():
    try:
        if not manual_mapping._get_tmdb_api_key():
            raise HTTPException(status_code=400, detail="tmdb_api_key_missing")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="tmdb_api_key_missing")


@router.get("/api/manual-mapping/{item_id}")
def get_manual_mapping(item_id: int, db: Session = Depends(get_db), _: bool = Depends(admin_required)):
    mm = db.query(models.ManualMapping).filter(models.ManualMapping.media_item_id == item_id).first()
    rows = (
        db.query(models.ManualOverride)
        .filter(models.ManualOverride.media_item_id == item_id)
        .order_by(models.ManualOverride.updated_at.desc())
        .all()
    )
    overrides = []
    for r in rows:
        overrides.append(
            {
                "language": r.language,
                "title": r.title,
                "overview": r.overview,
                "genres": r.genres,
                "episode_overrides": r.episode_overrides,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
        )
    return {
        "ok": True,
        "mapping": {
            "tmdb_id": getattr(mm, "tmdb_id", None),
            "media_type": getattr(mm, "media_type", None),
            "season_number": getattr(mm, "season_number", None),
            "poster_url": getattr(mm, "poster_url", None),
            "backdrop_url": getattr(mm, "backdrop_url", None),
        }
        if mm
        else None,
        "overrides": overrides,
    }


@router.get("/api/manual-mapping/tmdb/search")
def manual_tmdb_search(query: str, language: str = "es-MX", _: bool = Depends(admin_required)):
    _require_key()
    if not str(query or "").strip():
        return {"ok": True, "results": []}
    return manual_mapping.tmdb_search_multi(str(query), language=language)


@router.get("/api/manual-mapping/tmdb/{tmdb_id}/details")
def manual_tmdb_details(tmdb_id: str, media_type: str = "tv", language: str = "es-MX", _: bool = Depends(admin_required)):
    _require_key()
    media_type = str(media_type or "tv").strip().lower()
    if media_type not in ("tv", "movie"):
        raise HTTPException(status_code=400, detail="invalid_media_type")
    return manual_mapping.tmdb_details(tmdb_id, media_type=media_type, language=language)


@router.get("/api/manual-mapping/tmdb/{tmdb_id}/seasons")
def manual_tmdb_seasons(tmdb_id: str, language: str = "es-MX", _: bool = Depends(admin_required)):
    _require_key()
    return manual_mapping.tmdb_seasons(tmdb_id, language=language)


@router.get("/api/manual-mapping/tmdb/{tmdb_id}/season/{season_number}")
def manual_tmdb_season(
    tmdb_id: str,
    season_number: int,
    language: str = "es-MX",
    _: bool = Depends(admin_required),
):
    _require_key()
    return manual_mapping.tmdb_season_details(tmdb_id, season_number=season_number, language=language)


@router.post("/api/manual-mapping/{item_id}/apply")
def apply_manual_mapping(item_id: int, payload: dict[str, Any], db: Session = Depends(get_db), _: bool = Depends(admin_required)):
    try:
        tmdb_id = str(payload.get("tmdb_id") or "").strip() or None
        media_type = str(payload.get("media_type") or "").strip() or None
        season_number = payload.get("season_number")
        try:
            season_number = int(season_number) if season_number is not None else None
        except Exception:
            season_number = None
        language = str(payload.get("language") or "es-MX").strip()
        title = payload.get("title") or None
        overview = payload.get("overview") or None
        genres = payload.get("genres") if isinstance(payload.get("genres"), list) else None
        poster_url = payload.get("poster_url") or None
        backdrop_url = payload.get("backdrop_url") or None
        season_title = payload.get("season_title") or None
        episode_overrides = payload.get("episode_overrides") if isinstance(payload.get("episode_overrides"), list) else None
        download_assets = bool(payload.get("download_assets")) if payload.get("download_assets") is not None else False
        translation_only = bool(payload.get("translation_only")) if payload.get("translation_only") is not None else False
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_payload")

    return manual_mapping.apply_manual_mapping(
        db=db,
        media_item_id=int(item_id),
        tmdb_id=tmdb_id,
        media_type=media_type,
        season_number=season_number,
        language=language,
        title=title,
        overview=overview,
        genres=genres,
        poster_url=poster_url,
        backdrop_url=backdrop_url,
        season_title=season_title,
        episode_overrides=episode_overrides,
        download_assets=download_assets,
        translation_only=translation_only,
    )
