from __future__ import annotations

import json
import logging
import mimetypes
import os
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi import BackgroundTasks
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from sqlalchemy.orm import Session

from ...db import SessionLocal, models
from ...core.config import get as get_config
from ...services.translations import preferred_lang_chain_from_config, resolve_translation_fields
from ...services import enrichment_runner
from ...services import recommendations
from ...services import search_suggest
from ...services.enrich_state import get_state as get_enrich_state
from ...services.enrichment_runner import enrich_one_serialized
from ..deps import get_db, admin_required

router = APIRouter()


def _pick_primary_metadata(metadata_out: list[dict[str, Any]] | None):
    try:
        for m in metadata_out or []:
            if m.get("provider"):
                return m
        if metadata_out:
            return metadata_out[-1]
    except Exception:
        return None
    return None


@router.get("/api/media")
def list_media(
    skip: int = 0,
    limit: int = 100,
    search: str | None = None,
    types: str | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(models.MediaItem)
    try:
        if search and str(search).strip():
            s = f"%{str(search).strip()}%"
            q = q.filter(
                (models.MediaItem.title.ilike(s))
                | (models.MediaItem.base_title.ilike(s))
                | (models.MediaItem.canonical_path.ilike(s))
            )
    except Exception:
        pass

    try:
        if types and str(types).strip():
            raw = [t.strip().lower() for t in str(types).split(",") if t and t.strip()]
            raw = [t for t in raw if t]
            if raw:
                cond = None
                for t in raw:
                    c = (models.MediaItem.media_type.ilike(t)) | (models.MediaItem.library_type.ilike(t))
                    cond = c if cond is None else (cond | c)
                if cond is not None:
                    q = q.filter(cond)
    except Exception:
        pass

    try:
        if status and str(status).strip():
            raw = [s.strip().upper() for s in str(status).split(",") if s and s.strip()]
            raw = [s for s in raw if s]
            if raw:
                cond = None
                for s in raw:
                    if s in ("NULL", "NONE", "UNSET"):
                        c = (models.MediaItem.status == None)  # noqa: E711
                    else:
                        c = (models.MediaItem.status == s)
                    cond = c if cond is None else (cond | c)
                if cond is not None:
                    q = q.filter(cond)
    except Exception:
        pass

    try:
        total = int(q.count() or 0)
    except Exception:
        total = 0

    try:
        q = q.order_by(models.MediaItem.id.desc())
    except Exception:
        pass

    items = q.offset(skip).limit(limit).all()

    ids: list[int] = []
    try:
        ids = [int(getattr(it, "id", 0)) for it in items if getattr(it, "id", None) is not None]
    except Exception:
        ids = []

    # Resolve localized UI fields via media_translations with deterministic fallback chain.
    preferred_langs = preferred_lang_chain_from_config(get_config)
    translations_by_item: dict[int, dict[str, str | None]] = {}
    try:
        if ids:
            rows_tr = db.query(models.MediaTranslation).filter(models.MediaTranslation.path_id.in_(ids)).all()
            grouped: dict[int, list[models.MediaTranslation]] = {}
            for r in rows_tr:
                try:
                    grouped.setdefault(int(getattr(r, "path_id", 0)), []).append(r)
                except Exception:
                    continue
            for mid in ids:
                trs = grouped.get(mid) or []
                translations_by_item[mid] = resolve_translation_fields(trs, preferred_langs=preferred_langs)
    except Exception:
        translations_by_item = {}
    manual_by_item: dict[int, dict[str, str | None]] = {}
    try:
        if ids:
            rows_mm = db.query(models.ManualMapping).filter(models.ManualMapping.media_item_id.in_(ids)).all()
            for r in rows_mm:
                try:
                    manual_by_item[int(getattr(r, "media_item_id", 0))] = {
                        "poster": getattr(r, "poster_url", None),
                        "backdrop": getattr(r, "backdrop_url", None),
                    }
                except Exception:
                    continue
    except Exception:
        manual_by_item = {}

    files_by_item: dict[int, list[models.FileRecord]] = {}
    metadata_by_item: dict[int, list[models.MediaMetadata]] = {}
    relations_by_from: dict[int, list[models.MediaRelation]] = {}
    related_by_id: dict[int, models.MediaItem] = {}
    try:
        if ids:
            rows_fr = db.query(models.FileRecord).filter(models.FileRecord.media_item_id.in_(ids)).all()
            for r in rows_fr:
                try:
                    mid = int(getattr(r, "media_item_id", 0))
                except Exception:
                    continue
                files_by_item.setdefault(mid, []).append(r)
    except Exception:
        files_by_item = {}
    try:
        if ids:
            rows_md = (
                db.query(models.MediaMetadata)
                .filter(models.MediaMetadata.media_item_id.in_(ids))
                .order_by(
                    models.MediaMetadata.media_item_id,
                    models.MediaMetadata.version.desc(),
                    models.MediaMetadata.created_at.desc(),
                    models.MediaMetadata.id.desc(),
                )
                .all()
            )
            for r in rows_md:
                try:
                    mid = int(getattr(r, "media_item_id", 0))
                except Exception:
                    continue
                metadata_by_item.setdefault(mid, []).append(r)
    except Exception:
        metadata_by_item = {}
    try:
        if ids:
            rows_rel = db.query(models.MediaRelation).filter(models.MediaRelation.from_item_id.in_(ids)).all()
            rel_targets: set[int] = set()
            for r in rows_rel:
                try:
                    frm = int(getattr(r, "from_item_id", 0))
                except Exception:
                    continue
                relations_by_from.setdefault(frm, []).append(r)
                try:
                    tid = getattr(r, "to_item_id", None)
                    if tid is not None:
                        rel_targets.add(int(tid))
                except Exception:
                    pass
            if rel_targets:
                rows_t = db.query(models.MediaItem).filter(models.MediaItem.id.in_(list(rel_targets))).all()
                for t in rows_t:
                    try:
                        related_by_id[int(getattr(t, "id", 0))] = t
                    except Exception:
                        continue
    except Exception:
        relations_by_from = {}
        related_by_id = {}
    result = []
    for it in items:
        tr = translations_by_item.get(int(it.id)) if getattr(it, "id", None) is not None else None
        files = files_by_item.get(int(it.id), []) if getattr(it, "id", None) is not None else []
        files_out = []
        for f in files:
            path = f.path or ""
            filename = path.split("\\")[-1].split("/")[-1] if path else None
            fmt = None
            if filename and "." in filename:
                fmt = filename.rsplit(".", 1)[1].lower()
            files_out.append(
                {"filename": filename, "path": path, "size": f.size, "mtime": f.mtime, "format": fmt, "index": getattr(f, "file_index", None)}
            )
        try:
            files_out.sort(
                key=lambda x: (
                    x.get("index") is None,
                    x.get("index") if x.get("index") is not None else 0,
                    x.get("filename") or "",
                )
            )
        except Exception:
            pass

        metadata = metadata_by_item.get(int(it.id), []) if getattr(it, "id", None) is not None else []
        metadata_out = []
        poster_url = None
        try:
            pp = getattr(it, "poster_path", None)
            if isinstance(pp, str) and pp.strip():
                if pp.startswith("/media-assets/") or pp.startswith("http://") or pp.startswith("https://"):
                    poster_url = pp
        except Exception:
            poster_url = None
        try:
            if not poster_url:
                mm = manual_by_item.get(int(it.id)) if getattr(it, "id", None) is not None else None
                mp = mm.get("poster") if isinstance(mm, dict) else None
                if isinstance(mp, str) and mp.strip():
                    poster_url = mp
        except Exception:
            pass
        for m in metadata:
            try:
                data = json.loads(m.data) if m.data else None
            except Exception:
                data = m.data
            metadata_out.append(
                {"provider": m.provider, "provider_id": m.provider_id, "data": data, "created_at": m.created_at.isoformat()}
            )
            if not poster_url and data:
                try:
                    imgs = data.get("images") if isinstance(data, dict) else None
                except Exception:
                    imgs = None
                if imgs:
                    try:
                        if isinstance(imgs, dict) and imgs.get("jpg", {}).get("large_image_url"):
                            poster_url = imgs.get("jpg").get("large_image_url")
                    except Exception:
                        pass
                    try:
                        if isinstance(imgs, dict) and "posters" in imgs and len(imgs.get("posters") or []) > 0:
                            p = imgs.get("posters")[0].get("file_path")
                            if p:
                                poster_url = p
                    except Exception:
                        pass
                    try:
                        if isinstance(imgs, str) and imgs:
                            poster_url = imgs
                    except Exception:
                        pass

        if not poster_url:
            poster_url = f"/api/media/{it.id}/poster"

        primary_metadata = _pick_primary_metadata(metadata_out)
        related_out = []
        try:
            rels = relations_by_from.get(int(it.id), []) if getattr(it, "id", None) is not None else []
            for r in rels:
                if not r.to_item_id:
                    continue
                tgt = related_by_id.get(int(r.to_item_id))
                if not tgt:
                    continue
                related_out.append(
                    {
                        "id": tgt.id,
                        "title": tgt.title or tgt.base_title,
                        "relation": r.relation_type,
                        "mal_id": getattr(tgt, "mal_id", None),
                        "poster_url": f"/api/media/{tgt.id}/poster",
                    }
                )
        except Exception:
            related_out = []
        cast_list = None
        try:
            c = getattr(it, "cast", None)
            if isinstance(c, str) and c.strip():
                cast_list = [x.strip() for x in c.split(",") if x.strip()]
            elif isinstance(c, list):
                cast_list = c
        except Exception:
            cast_list = None
        genres_list = None
        try:
            g = getattr(it, "genres", None)
            if isinstance(g, str) and g.strip():
                genres_list = [x.strip() for x in g.split(",") if x.strip()]
            elif isinstance(g, list):
                genres_list = g
        except Exception:
            genres_list = None

        result.append(
            {
                "id": it.id,
                "media_id": getattr(it, "media_id", None),
                "canonical_path": getattr(it, "canonical_path", None),
                "canonical_path_hash": getattr(it, "canonical_path_hash", None),
                "base_title": getattr(it, "base_title", None) or it.title,
                "title": it.title,
                "title_en": getattr(it, "title_en", None),
                "title_localized": (tr.get("title") if isinstance(tr, dict) else None) or getattr(it, "title_localized", None),
                "synopsis_localized": (tr.get("overview") if isinstance(tr, dict) else None) or getattr(it, "synopsis_localized", None),
                "media_type": it.media_type,
                "status": getattr(it, "status", None),
                "media_root": getattr(it, "media_root", None),
                "provider": it.provider,
                "provider_id": it.provider_id,
                "mal_id": getattr(it, "mal_id", None),
                "tmdb_id": getattr(it, "tmdb_id", None),
                "poster_path": getattr(it, "poster_path", None),
                "backdrop_path": (manual_by_item.get(int(it.id), {}).get("backdrop") if isinstance(manual_by_item.get(int(it.id), {}), dict) else None)
                or getattr(it, "backdrop_path", None),
                "rating": getattr(it, "rating", None),
                "genres": genres_list,
                "is_animated": getattr(it, "is_animated", None),
                "origin": getattr(it, "origin", None),
                "release_year": getattr(it, "release_year", None),
                "runtime": getattr(it, "runtime", None),
                "cast": cast_list,
                "created_at": it.created_at.isoformat() if it.created_at else None,
                "files": files_out,
                "metadata": metadata_out,
                "primary_metadata": primary_metadata,
                "poster_url": poster_url,
                "related": related_out,
            }
        )
    return {"total": total, "items": result}


@router.get("/api/media/search-suggest")
def search_suggest_media(
    query: str,
    limit: int = 20,
    types: str | None = None,
    db: Session = Depends(get_db),
):
    raw_types: list[str] | None = None
    try:
        if types and str(types).strip():
            raw = [t.strip().lower() for t in str(types).split(",") if t and t.strip()]
            raw_types = raw if raw else None
    except Exception:
        raw_types = None

    ids = search_suggest.suggest_media(db=db, query=str(query or ""), limit=limit, types=raw_types)
    if not ids:
        return {"items": [], "total": 0}

    items = db.query(models.MediaItem).filter(models.MediaItem.id.in_(ids)).all()
    preferred_langs = preferred_lang_chain_from_config(get_config)
    translations_by_item: dict[int, dict[str, str | None]] = {}
    try:
        rows_tr = db.query(models.MediaTranslation).filter(models.MediaTranslation.path_id.in_(ids)).all()
        grouped: dict[int, list[models.MediaTranslation]] = {}
        for r in rows_tr:
            try:
                grouped.setdefault(int(getattr(r, "path_id", 0)), []).append(r)
            except Exception:
                continue
        for mid in ids:
            trs = grouped.get(mid) or []
            translations_by_item[mid] = resolve_translation_fields(trs, preferred_langs=preferred_langs)
    except Exception:
        translations_by_item = {}

    by_id = {int(getattr(it, "id", 0)): it for it in items if getattr(it, "id", None) is not None}
    result = []
    for mid in ids:
        it = by_id.get(int(mid))
        if not it:
            continue
        tr = translations_by_item.get(int(it.id)) if getattr(it, "id", None) is not None else None
        poster_url = None
        try:
            pp = getattr(it, "poster_path", None)
            if isinstance(pp, str) and pp.strip():
                if pp.startswith("/media-assets/") or pp.startswith("http://") or pp.startswith("https://"):
                    poster_url = pp
        except Exception:
            poster_url = None
        if not poster_url:
            poster_url = f"/api/media/{it.id}/poster"
        result.append(
            {
                "id": it.id,
                "title": it.title,
                "base_title": getattr(it, "base_title", None),
                "title_en": getattr(it, "title_en", None),
                "title_localized": (tr.get("title") if isinstance(tr, dict) else None) or getattr(it, "title_localized", None),
                "synopsis_localized": (tr.get("overview") if isinstance(tr, dict) else None) or getattr(it, "synopsis_localized", None),
                "media_type": it.media_type,
                "library_type": getattr(it, "library_type", None),
                "poster_path": getattr(it, "poster_path", None),
                "poster_url": poster_url,
                "backdrop_path": getattr(it, "backdrop_path", None),
                "release_year": getattr(it, "release_year", None),
                "rating": getattr(it, "rating", None),
                "genres": getattr(it, "genres", None),
                "created_at": it.created_at.isoformat() if it.created_at else None,
                "mal_id": getattr(it, "mal_id", None),
            }
        )
    return {"items": result, "total": len(result)}


@router.get("/api/media/recommendations")
def get_recommendations(
    limit: int = 20,
    profile_id: str | None = None,
    profile_header: str | None = Header(None, alias="X-Arcanea-Profile-Id"),
    db: Session = Depends(get_db),
):
    pid = profile_id or profile_header
    items = recommendations.get_recommendations(db, profile_id=pid, limit=limit)
    preferred_langs = preferred_lang_chain_from_config(get_config)
    translations_by_item: dict[int, dict[str, str | None]] = {}
    try:
        ids = [int(getattr(it, "id", 0)) for it in items if getattr(it, "id", None) is not None]
        if ids:
            rows_tr = db.query(models.MediaTranslation).filter(models.MediaTranslation.path_id.in_(ids)).all()
            grouped: dict[int, list[models.MediaTranslation]] = {}
            for r in rows_tr:
                try:
                    grouped.setdefault(int(getattr(r, "path_id", 0)), []).append(r)
                except Exception:
                    continue
            for mid in ids:
                trs = grouped.get(mid) or []
                translations_by_item[mid] = resolve_translation_fields(trs, preferred_langs=preferred_langs)
    except Exception:
        translations_by_item = {}

    out = []
    for it in items:
        tr = translations_by_item.get(int(it.id)) if getattr(it, "id", None) is not None else None
        poster_url = None
        try:
            pp = getattr(it, "poster_path", None)
            if isinstance(pp, str) and pp.strip():
                if pp.startswith("/media-assets/") or pp.startswith("http://") or pp.startswith("https://"):
                    poster_url = pp
        except Exception:
            poster_url = None
        if not poster_url:
            poster_url = f"/api/media/{it.id}/poster"
        out.append(
            {
                "id": it.id,
                "title": it.title,
                "title_localized": (tr.get("title") if isinstance(tr, dict) else None) or getattr(it, "title_localized", None),
                "synopsis_localized": (tr.get("overview") if isinstance(tr, dict) else None) or getattr(it, "synopsis_localized", None),
                "media_type": it.media_type,
                "poster_url": poster_url,
                "backdrop_path": getattr(it, "backdrop_path", None),
                "rating": getattr(it, "rating", None),
                "genres": getattr(it, "genres", None),
                "created_at": it.created_at.isoformat() if it.created_at else None,
            }
        )
    return {"ok": True, "items": out}


@router.post("/api/media/{item_id}/play")
def record_play(
    item_id: int,
    profile_id: str | None = None,
    profile_header: str | None = Header(None, alias="X-Arcanea-Profile-Id"),
    db: Session = Depends(get_db),
):
    it = db.query(models.MediaItem).filter(models.MediaItem.id == item_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="Not found")
    pid = profile_id or profile_header
    return recommendations.record_play(db, media_item_id=item_id, profile_id=pid)


@router.get("/api/media/{item_id}")
def get_media(item_id: int, db: Session = Depends(get_db)):
    it = db.query(models.MediaItem).filter(models.MediaItem.id == item_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="Not found")

    preferred_langs = preferred_lang_chain_from_config(get_config)
    tr = None
    try:
        rows_tr = db.query(models.MediaTranslation).filter(models.MediaTranslation.path_id == int(it.id)).all()
        tr = resolve_translation_fields(rows_tr or [], preferred_langs=preferred_langs)
    except Exception:
        tr = None
    files = db.query(models.FileRecord).filter(models.FileRecord.media_item_id == it.id).all()
    files_out = []
    for f in files:
        path = f.path or ""
        filename = path.split("\\")[-1].split("/")[-1] if path else None
        fmt = None
        if filename and "." in filename:
            fmt = filename.rsplit(".", 1)[1].lower()
        files_out.append({"filename": filename, "path": path, "size": f.size, "mtime": f.mtime, "format": fmt, "index": getattr(f, "file_index", None)})
    try:
        files_out.sort(
            key=lambda x: (
                x.get("index") is None,
                x.get("index") if x.get("index") is not None else 0,
                x.get("filename") or "",
            )
        )
    except Exception:
        pass

    metadata = (
        db.query(models.MediaMetadata)
        .filter(models.MediaMetadata.media_item_id == it.id)
        .order_by(models.MediaMetadata.version.desc(), models.MediaMetadata.created_at.desc(), models.MediaMetadata.id.desc())
        .all()
    )
    metadata_out = []
    poster_url = None
    try:
        pp = getattr(it, "poster_path", None)
        if isinstance(pp, str) and pp.strip():
            if pp.startswith("/media-assets/") or pp.startswith("http://") or pp.startswith("https://"):
                poster_url = pp
    except Exception:
        poster_url = None
    manual_backdrop = None
    try:
        if not poster_url:
            mm = db.query(models.ManualMapping).filter(models.ManualMapping.media_item_id == it.id).first()
            mp = getattr(mm, "poster_url", None) if mm else None
            if isinstance(mp, str) and mp.strip():
                poster_url = mp
            manual_backdrop = getattr(mm, "backdrop_url", None) if mm else None
    except Exception:
        manual_backdrop = None
    for m in metadata:
        try:
            data = json.loads(m.data) if m.data else None
        except Exception:
            data = m.data
        metadata_out.append({"provider": m.provider, "provider_id": m.provider_id, "data": data, "created_at": m.created_at.isoformat()})
        if not poster_url and data:
            try:
                imgs = data.get("images") if isinstance(data, dict) else None
            except Exception:
                imgs = None
            if imgs:
                try:
                    if isinstance(imgs, dict) and imgs.get("jpg", {}).get("large_image_url"):
                        poster_url = imgs.get("jpg").get("large_image_url")
                except Exception:
                    pass
                try:
                    if isinstance(imgs, dict) and "posters" in imgs and len(imgs.get("posters") or []) > 0:
                        p = imgs.get("posters")[0].get("file_path")
                        if p:
                            poster_url = p
                except Exception:
                    pass
                try:
                    if isinstance(imgs, str) and imgs:
                        poster_url = imgs
                except Exception:
                    pass

    related_out = []
    try:
        rels = (
            db.query(models.MediaRelation)
            .filter(models.MediaRelation.from_item_id == it.id)
            .all()
        )
        for r in rels:
            if not r.to_item_id:
                continue
            tgt = db.query(models.MediaItem).filter(models.MediaItem.id == r.to_item_id).first()
            if not tgt:
                continue
            related_out.append(
                {
                    "id": tgt.id,
                    "title": tgt.title or tgt.base_title,
                    "relation": r.relation_type,
                    "mal_id": getattr(tgt, "mal_id", None),
                    "poster_url": f"/api/media/{tgt.id}/poster",
                }
            )
    except Exception:
        related_out = []

    if not poster_url:
        poster_url = f"/api/media/{it.id}/poster"

    primary_metadata = _pick_primary_metadata(metadata_out)
    cast_list = None
    try:
        c = getattr(it, "cast", None)
        if isinstance(c, str) and c.strip():
            cast_list = [x.strip() for x in c.split(",") if x.strip()]
        elif isinstance(c, list):
            cast_list = c
    except Exception:
        cast_list = None
    genres_list = None
    try:
        g = getattr(it, "genres", None)
        if isinstance(g, str) and g.strip():
            genres_list = [x.strip() for x in g.split(",") if x.strip()]
        elif isinstance(g, list):
            genres_list = g
    except Exception:
        genres_list = None
    return {
        "id": it.id,
        "media_id": getattr(it, "media_id", None),
        "canonical_path": getattr(it, "canonical_path", None),
        "canonical_path_hash": getattr(it, "canonical_path_hash", None),
        "base_title": getattr(it, "base_title", None) or it.title,
        "title": it.title,
        "title_en": getattr(it, "title_en", None),
        "title_localized": (tr.get("title") if isinstance(tr, dict) else None) or getattr(it, "title_localized", None),
        "synopsis_localized": (tr.get("overview") if isinstance(tr, dict) else None) or getattr(it, "synopsis_localized", None),
        "media_type": it.media_type,
        "status": getattr(it, "status", None),
        "media_root": getattr(it, "media_root", None),
        "provider": it.provider,
        "provider_id": it.provider_id,
        "mal_id": getattr(it, "mal_id", None),
        "tmdb_id": getattr(it, "tmdb_id", None),
        "poster_path": getattr(it, "poster_path", None),
        "backdrop_path": manual_backdrop or getattr(it, "backdrop_path", None),
        "rating": getattr(it, "rating", None),
        "genres": genres_list,
        "is_animated": getattr(it, "is_animated", None),
        "origin": getattr(it, "origin", None),
        "release_year": getattr(it, "release_year", None),
        "runtime": getattr(it, "runtime", None),
        "cast": cast_list,
        "created_at": it.created_at.isoformat() if it.created_at else None,
        "files": files_out,
        "metadata": metadata_out,
        "primary_metadata": primary_metadata,
        "poster_url": poster_url,
        "related": related_out,
    }


@router.post("/api/media/{item_id}/enrich")
def enrich_media_item(item_id: int, _: bool = Depends(admin_required)):
    try:
        db = SessionLocal()
        try:
            it = db.query(models.MediaItem).filter(models.MediaItem.id == int(item_id)).first()
        finally:
            db.close()
        if not it:
            raise HTTPException(status_code=404, detail="Not found")

        ok = bool(enrich_one_serialized(int(item_id)))
        if not ok:
            db2 = SessionLocal()
            try:
                it2 = db2.query(models.MediaItem).filter(models.MediaItem.id == int(item_id)).first()
                st = getattr(it2, "status", None) if it2 else None
            finally:
                db2.close()
            if st == "NO_MATCH":
                return {"ok": False, "detail": "No match (Jikan/TMDB) para este título."}
            return {"ok": False, "detail": "No se pudo consultar el proveedor (Jikan/TMDB). Intenta de nuevo en unos minutos."}
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        logging.getLogger(__name__).exception("Failed forcing enrichment for media item %s", item_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/media/{item_id}/omit")
def omit_media_item(item_id: int, db: Session = Depends(get_db), _: bool = Depends(admin_required)):
    it = db.query(models.MediaItem).filter(models.MediaItem.id == item_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="Not found")
    try:
        it.status = "OMITTED"
        db.add(it)
        db.commit()
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="failed updating status")
    return {"ok": True, "id": item_id, "status": "OMITTED"}


@router.get("/api/media/{item_id}/poster")
def media_poster(item_id: int, db: Session = Depends(get_db)):
    it = db.query(models.MediaItem).filter(models.MediaItem.id == item_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="Not found")
    try:
        pp = getattr(it, "poster_path", None)
        if isinstance(pp, str) and pp.strip():
            if pp.startswith("/media-assets/") or pp.startswith("http://") or pp.startswith("https://"):
                return RedirectResponse(pp)
        if pp and os.path.isfile(str(pp)):
            return FileResponse(str(pp))
    except Exception:
        pass
    metadata = db.query(models.MediaMetadata).filter(models.MediaMetadata.media_item_id == it.id).all()
    for m in metadata:
        try:
            data = json.loads(m.data) if m.data else None
        except Exception:
            data = m.data
        if not data:
            continue
        imgs = None
        try:
            imgs = data.get("images") if isinstance(data, dict) else None
        except Exception:
            imgs = None
        if imgs:
            try:
                if isinstance(imgs, dict) and imgs.get("jpg", {}).get("large_image_url"):
                    return RedirectResponse(imgs.get("jpg").get("large_image_url"))
            except Exception:
                pass
            try:
                if isinstance(imgs, dict) and "posters" in imgs and len(imgs.get("posters") or []) > 0:
                    p = imgs.get("posters")[0].get("file_path")
                    if p:
                        return RedirectResponse(p)
            except Exception:
                pass
            try:
                if isinstance(imgs, str) and imgs:
                    return RedirectResponse(imgs)
            except Exception:
                pass
    try:
        cp = getattr(it, "canonical_path", None)
        if cp and os.path.isdir(cp):
            candidates = ["poster.jpg", "poster.png", "folder.jpg", "folder.png", "cover.jpg", "cover.png"]
            for c in candidates:
                p = os.path.join(cp, c)
                if os.path.exists(p) and os.path.isfile(p):
                    ct, _ = mimetypes.guess_type(p)
                    ct = ct or "image/jpeg"
                    return StreamingResponse(open(p, "rb"), media_type=ct)
            for entry in os.listdir(cp):
                ln = entry.lower()
                if ln in ["poster.jpg", "poster.png", "folder.jpg", "folder.png", "cover.jpg", "cover.png", "1.jpg", "1.webp", "1.png"]:
                    p = os.path.join(cp, entry)
                    if os.path.exists(p) and os.path.isfile(p):
                        ct, _ = mimetypes.guess_type(p)
                        ct = ct or "image/jpeg"
                        return StreamingResponse(open(p, "rb"), media_type=ct)
    except Exception:
        pass

    raise HTTPException(status_code=404, detail="No poster available")


@router.get("/api/media/{item_id}/episode-seasons")
def get_media_episode_seasons(
    item_id: int,
    background_tasks: BackgroundTasks,
    include_related: bool = True,
    db: Session = Depends(get_db),
):
    """Return JOINed seasons/episodes for a media item.

    The frontend should use this instead of reading episode lists from `media_metadata.data` blobs.
    """
    it = db.query(models.MediaItem).filter(models.MediaItem.id == item_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="Not found")

    si = db.query(models.SeasonItem).filter(models.SeasonItem.media_item_id == item_id).first()
    if not si:
        return {"ok": True, "series": None, "seasons": [], "related": []}

    season = db.query(models.Season).filter(models.Season.id == si.season_id).first()
    if not season:
        return {"ok": True, "series": None, "seasons": [], "related": []}

    series = None
    try:
        if getattr(season, "series_id", None) is not None:
            series = db.query(models.Series).filter(models.Series.id == season.series_id).first()
    except Exception:
        series = None

    seasons_out: list[dict[str, Any]] = []
    preferred_langs = preferred_lang_chain_from_config(get_config)
    manual_episode_overrides: list[dict[str, Any]] | None = None
    try:
        rows_mo = (
            db.query(models.ManualOverride)
            .filter(models.ManualOverride.media_item_id == item_id)
            .order_by(models.ManualOverride.updated_at.desc())
            .all()
        )
        if rows_mo:
            chosen = None
            for lang in preferred_langs:
                for r in rows_mo:
                    if str(getattr(r, "language", "")).lower() == str(lang).lower():
                        chosen = r
                        break
                if chosen:
                    break
            if not chosen:
                chosen = rows_mo[0]
            raw = getattr(chosen, "episode_overrides", None)
            try:
                manual_episode_overrides = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                manual_episode_overrides = None
            if not isinstance(manual_episode_overrides, list):
                manual_episode_overrides = None
    except Exception:
        manual_episode_overrides = None

    def build_season_payload(season_row: Any, media_item_id: int | None) -> dict[str, Any]:
        eps = (
            db.query(models.Episode, models.FileRecord)
            .join(models.EpisodeFile, models.EpisodeFile.episode_id == models.Episode.id)
            .join(models.FileRecord, models.FileRecord.id == models.EpisodeFile.file_record_id)
            .filter(models.Episode.season_id == season_row.id)
            .order_by(models.Episode.episode_number.asc(), models.Episode.id.asc())
            .all()
        )
        episodes_out = []
        for ep, fr in eps:
            path = getattr(fr, "path", None)
            filename = None
            try:
                filename = path.split("\\")[-1].split("/")[-1] if path else None
            except Exception:
                filename = None
            try:
                title_best = getattr(ep, "title_localized", None) or getattr(ep, "title_en", None) or getattr(ep, "title", None)
            except Exception:
                title_best = getattr(ep, "title", None)
            episodes_out.append(
                {
                    "id": ep.id,
                    "episode_number": getattr(ep, "episode_number", None),
                    "title": title_best,
                    "title_en": getattr(ep, "title_en", None),
                    "title_localized": getattr(ep, "title_localized", None),
                    "synopsis_localized": getattr(ep, "synopsis_localized", None),
                    "file": {
                        "id": fr.id,
                        "path": path,
                        "filename": filename,
                        "file_index": getattr(fr, "file_index", None),
                        "size": getattr(fr, "size", None),
                        "mtime": getattr(fr, "mtime", None),
                    },
                }
            )
        if manual_episode_overrides:
            def _norm_title(v: str | None) -> str:
                try:
                    return "".join(ch.lower() for ch in str(v or "").strip() if ch.isalnum())
                except Exception:
                    return ""

            ep_by_num = {}
            ep_by_title = {}
            for ep_row in episodes_out:
                n = ep_row.get("episode_number")
                try:
                    if n is not None:
                        ep_by_num[int(n)] = ep_row
                except Exception:
                    pass
                t = ep_row.get("title_localized") or ep_row.get("title_en") or ep_row.get("title")
                nt = _norm_title(t)
                if nt:
                    ep_by_title.setdefault(nt, []).append(ep_row)

            remapped = []
            used_ids: set[int] = set()
            for idx, ov in enumerate(manual_episode_overrides):
                if not isinstance(ov, dict):
                    continue
                original_num = ov.get("original_episode_number")
                target = None
                if original_num is not None:
                    try:
                        target = ep_by_num.get(int(original_num))
                    except Exception:
                        target = None
                if not target:
                    t = _norm_title(ov.get("title"))
                    if t and t in ep_by_title:
                        for cand in ep_by_title[t]:
                            try:
                                if int(cand.get("id")) in used_ids:
                                    continue
                            except Exception:
                                pass
                            target = cand
                            break
                if not target:
                    try:
                        target = ep_by_num.get(int(ov.get("episode_number")))
                    except Exception:
                        target = None
                if not target:
                    continue
                try:
                    used_ids.add(int(target.get("id")))
                except Exception:
                    pass
                new_num = ov.get("episode_number")
                try:
                    new_num = int(new_num) if new_num is not None else idx + 1
                except Exception:
                    new_num = idx + 1
                ep_out = dict(target)
                ep_out["episode_number"] = new_num
                if ov.get("title"):
                    ep_out["title"] = ov.get("title")
                    ep_out["title_localized"] = ov.get("title")
                if ov.get("overview"):
                    ep_out["synopsis_localized"] = ov.get("overview")
                try:
                    if isinstance(ep_out.get("file"), dict):
                        ep_out["file"] = dict(ep_out["file"])
                        ep_out["file"]["file_index"] = new_num
                except Exception:
                    pass
                remapped.append(ep_out)
            if remapped:
                episodes_out = remapped
        try:
            season_title_best = getattr(season_row, "title_localized", None) or getattr(season_row, "title_en", None) or None
        except Exception:
            season_title_best = None
        return {
            "season_id": season_row.id,
            "season_number": getattr(season_row, "season_number", None),
            "title": season_title_best,
            "title_en": getattr(season_row, "title_en", None),
            "title_localized": getattr(season_row, "title_localized", None),
            "media_item_id": media_item_id,
            "episodes": episodes_out,
        }

    def _is_generic_episode_title(v: str | None) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        if not s:
            return True
        if re.fullmatch(r"\\d{1,4}", s):
            return True
        if s.lower().startswith("episode ") or s.lower().startswith("episodio "):
            return True
        return False

    # Primary: seasons that belong to the same logical series (when available).
    try:
        if series and getattr(series, "id", None) is not None:
            srows = (
                db.query(models.Season)
                .filter(models.Season.series_id == series.id)
                .order_by(models.Season.season_number.asc(), models.Season.id.asc())
                .all()
            )
        else:
            srows = [season]
    except Exception:
        srows = [season]

    for srow in srows:
        mid = None
        try:
            sm = db.query(models.SeasonItem).filter(models.SeasonItem.season_id == srow.id).first()
            if sm:
                mid = sm.media_item_id
        except Exception:
            mid = None
        seasons_out.append(build_season_payload(srow, mid))

    # Lazy episode title backfill: if we detect generic titles, schedule a background backfill for this item.
    # This keeps UI "self-healing" for cases where scan rebuilt normalized tables after enrichment.
    backfill_scheduled = False
    try:
        if background_tasks is not None and seasons_out:
            # If any episode in the primary season has a generic title, request a backfill.
            primary = seasons_out[0]
            eps0 = primary.get("episodes") if isinstance(primary, dict) else None
            needs = False
            if isinstance(eps0, list) and eps0:
                for e in eps0:
                    tloc = e.get("title_localized") if isinstance(e, dict) else None
                    ten = e.get("title_en") if isinstance(e, dict) else None
                    tbest = e.get("title") if isinstance(e, dict) else None
                    if (not (str(tloc or "").strip())) and (_is_generic_episode_title(str(ten or tbest or ""))):
                        needs = True
                        break
            if needs:
                try:
                    st = get_enrich_state()
                    if not st.get("running"):
                        background_tasks.add_task(enrichment_runner.backfill_episodes_for_media_item_serialized, int(item_id))
                        backfill_scheduled = True
                except Exception:
                    # If we can't read state, still attempt in background.
                    background_tasks.add_task(enrichment_runner.backfill_episodes_for_media_item_serialized, int(item_id))
                    backfill_scheduled = True
    except Exception:
        backfill_scheduled = False

    related_out: list[dict[str, Any]] = []
    if include_related:
        try:
            rels = (
                db.query(models.MediaRelation)
                .filter(models.MediaRelation.from_item_id == item_id)
                .all()
            )
            seen_season_ids = {s.get("season_id") for s in seasons_out if s.get("season_id")}
            for r in rels:
                if not getattr(r, "to_item_id", None):
                    continue
                t_si = db.query(models.SeasonItem).filter(models.SeasonItem.media_item_id == r.to_item_id).first()
                if not t_si:
                    continue
                t_season = db.query(models.Season).filter(models.Season.id == t_si.season_id).first()
                if not t_season:
                    continue
                if t_season.id in seen_season_ids:
                    continue
                tgt = db.query(models.MediaItem).filter(models.MediaItem.id == r.to_item_id).first()
                related_out.append(
                    {
                        "relation": getattr(r, "relation_type", None),
                        "media_item_id": getattr(r, "to_item_id", None),
                        "title": (getattr(tgt, "title", None) or getattr(tgt, "base_title", None)) if tgt else None,
                        "season_id": t_season.id,
                        "season_number": getattr(t_season, "season_number", None),
                    }
                )
        except Exception:
            related_out = []

    return {
        "ok": True,
        "series": {"id": series.id, "title": series.title} if series else None,
        "seasons": seasons_out,
        "related": related_out,
        "backfill_scheduled": bool(backfill_scheduled),
    }
