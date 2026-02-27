from __future__ import annotations

import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any, cast

import requests

from ..core.config import get as get_config
from ..core.config import DATA_DIR
from ..db import models

logger = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/"


def _guess_ext(url: str, content_type: str | None = None) -> str:
    try:
        _, ext = os.path.splitext(url)
        if ext and len(ext) <= 5:
            return ext
    except Exception:
        pass
    if content_type:
        if "png" in content_type:
            return ".png"
        if "webp" in content_type:
            return ".webp"
    return ".jpg"


def _download_to(url: str, target_path: Path) -> Path | None:
    try:
        resp = requests.get(url, stream=True, timeout=25)
        resp.raise_for_status()
        ct = resp.headers.get("content-type")
        ext = _guess_ext(url, ct)
        target_path = target_path.with_suffix(ext)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
        return target_path
    except Exception as e:
        logger.warning("Failed downloading asset %s: %s", url, e)
        return None


def _get_tmdb_api_key() -> str:
    try:
        return str(get_config("tmdb_api_key") or "").strip()
    except Exception:
        return ""


def _tmdb_get(path: str, *, language: str | None = None, params: dict[str, Any] | None = None) -> dict:
    key = _get_tmdb_api_key()
    if not key:
        raise RuntimeError("tmdb_api_key_missing")
    url = f"{TMDB_BASE}/{path.lstrip('/')}"
    q = {"api_key": key}
    if language:
        q["language"] = language
    if params:
        q.update(params)
    res = requests.get(url, params=q, timeout=20)
    res.raise_for_status()
    return res.json() if res.content else {}


def _merge_tmdb_images(*image_payloads: dict) -> tuple[list[str], list[str]]:
    posters: list[str] = []
    backdrops: list[str] = []
    seen_posters: set[str] = set()
    seen_backdrops: set[str] = set()
    for images in image_payloads:
        for p in images.get("posters") or []:
            fp = p.get("file_path")
            if not fp or fp in seen_posters:
                continue
            seen_posters.add(fp)
            posters.append(f"{TMDB_IMAGE_BASE}w780{fp}")
        for b in images.get("backdrops") or []:
            fp = b.get("file_path")
            if not fp or fp in seen_backdrops:
                continue
            seen_backdrops.add(fp)
            backdrops.append(f"{TMDB_IMAGE_BASE}original{fp}")
    return posters, backdrops


def _tmdb_images(media_type: str, tmdb_id: str, *, language: str | None) -> tuple[list[str], list[str]]:
    # TMDB images endpoint supports include_image_language to broaden results.
    lang = (language or "").strip()
    langs = []
    if lang:
        langs.append(lang)
    # Always add common sources for richer choices.
    for fallback in ("en", "en-US", "ja", "ja-JP", "null"):
        if fallback not in langs:
            langs.append(fallback)
    include_lang = ",".join(langs)
    region = None
    if "-" in lang:
        region = lang.split("-", 1)[1].upper()
    images = _tmdb_get(
        f"{media_type}/{tmdb_id}/images",
        params={"include_image_language": include_lang, **({"image_region": region} if region else {})},
    )
    return _merge_tmdb_images(images)


def tmdb_search_multi(query: str, *, language: str) -> dict:
    data = _tmdb_get("search/multi", language=language, params={"query": query, "include_adult": "false"})
    out = []
    for r in data.get("results") or []:
        if not r or r.get("media_type") not in ("tv", "movie"):
            continue
        out.append(
            {
                "tmdb_id": r.get("id"),
                "media_type": r.get("media_type"),
                "title": r.get("title") or r.get("name") or "",
                "year": (r.get("release_date") or r.get("first_air_date") or "")[:4],
                "poster": f"{TMDB_IMAGE_BASE}w342{r['poster_path']}" if r.get("poster_path") else None,
            }
        )
    return {"ok": True, "results": out}


def tmdb_details(tmdb_id: str, *, media_type: str, language: str) -> dict:
    d = _tmdb_get(f"{media_type}/{tmdb_id}", language=language)
    posters, backdrops = _tmdb_images(media_type, tmdb_id, language=language)
    return {
        "ok": True,
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": d.get("title") or d.get("name") or "",
        "overview": d.get("overview") or "",
        "genres": [g.get("name") for g in (d.get("genres") or []) if g and g.get("name")],
        "backdrop": f"{TMDB_IMAGE_BASE}original{d['backdrop_path']}" if d.get("backdrop_path") else None,
        "poster": f"{TMDB_IMAGE_BASE}w780{d['poster_path']}" if d.get("poster_path") else None,
        "posters": posters,
        "backdrops": backdrops,
        "number_of_seasons": d.get("number_of_seasons"),
    }


def tmdb_seasons(tmdb_id: str, *, language: str) -> dict:
    d = _tmdb_get(f"tv/{tmdb_id}", language=language)
    seasons = []
    for s in d.get("seasons") or []:
        if s is None:
            continue
        sn = s.get("season_number")
        if sn is None:
            continue
        seasons.append({"season_number": sn, "name": s.get("name")})
    return {"ok": True, "tmdb_id": tmdb_id, "seasons": seasons}


def tmdb_season_details(tmdb_id: str, *, season_number: int, language: str) -> dict:
    season = _tmdb_get(f"tv/{tmdb_id}/season/{season_number}", language=language)
    lang = (language or "").strip()
    langs = []
    if lang:
        langs.append(lang)
    for fallback in ("en", "en-US", "ja", "ja-JP", "null"):
        if fallback not in langs:
            langs.append(fallback)
    include_lang = ",".join(langs)
    region = None
    if "-" in lang:
        region = lang.split("-", 1)[1].upper()
    images = _tmdb_get(
        f"tv/{tmdb_id}/season/{season_number}/images",
        params={"include_image_language": include_lang, **({"image_region": region} if region else {})},
    )
    episodes = []
    for ep in season.get("episodes") or []:
        episodes.append(
            {
                "episode_number": ep.get("episode_number"),
                "title": ep.get("name") or "",
                "overview": ep.get("overview") or "",
            }
        )
    posters, backdrops = _merge_tmdb_images(images)
    return {
        "ok": True,
        "tmdb_id": tmdb_id,
        "season_number": season_number,
        "title": season.get("name") or "",
        "overview": season.get("overview") or "",
        "episodes": episodes,
        "posters": posters,
        "backdrops": backdrops,
    }


def apply_manual_mapping(
    *,
    db,
    media_item_id: int,
    tmdb_id: str | None,
    media_type: str | None,
    season_number: int | None,
    language: str,
    title: str | None,
    overview: str | None,
    genres: list[str] | None,
    poster_url: str | None,
    backdrop_url: str | None,
    season_title: str | None,
    episode_overrides: list[dict[str, Any]] | None,
    download_assets: bool | None = None,
    translation_only: bool | None = None,
) -> dict:
    now = datetime.datetime.utcnow()
    translation_only = bool(translation_only)
    mi = db.query(models.MediaItem).filter(models.MediaItem.id == media_item_id).first()
    if not mi:
        return {"ok": False, "detail": "media_item_not_found"}

    mm = db.query(models.ManualMapping).filter(models.ManualMapping.media_item_id == media_item_id).first()
    if not mm:
        mm = models.ManualMapping(media_item_id=media_item_id)
    try:
        if poster_url is None:
            poster_url = getattr(mm, "poster_url", None)
        if backdrop_url is None:
            backdrop_url = getattr(mm, "backdrop_url", None)
    except Exception:
        pass
    mm_any = cast(Any, mm)
    mm_any.tmdb_id = tmdb_id
    mm_any.media_type = media_type
    mm_any.season_number = season_number
    mm_any.poster_url = poster_url
    mm_any.backdrop_url = backdrop_url
    mm_any.updated_at = now
    db.add(mm_any)

    mo = (
        db.query(models.ManualOverride)
        .filter(models.ManualOverride.media_item_id == media_item_id)
        .filter(models.ManualOverride.language == language)
        .first()
    )
    if not mo:
        mo = models.ManualOverride(media_item_id=media_item_id, language=language, source="manual")
    mo_any = cast(Any, mo)
    mo_any.title = title
    mo_any.overview = overview
    mo_any.genres = json.dumps(genres or [], ensure_ascii=False)
    mo_any.episode_overrides = json.dumps(episode_overrides or [], ensure_ascii=False)
    mo_any.updated_at = now
    db.add(mo_any)

    try:
        tr = (
            db.query(models.MediaTranslation)
            .filter(models.MediaTranslation.path_id == media_item_id)
            .filter(models.MediaTranslation.language == language)
            .filter(models.MediaTranslation.source == "manual")
            .first()
        )
        if not tr:
            tr = models.MediaTranslation(path_id=media_item_id, language=language, source="manual")
        tr_any = cast(Any, tr)
        tr_any.title = title
        tr_any.overview = overview
        tr_any.updated_at = now
        db.add(tr_any)
    except Exception:
        pass

    if not translation_only:
        try:
            mi_any = cast(Any, mi)
            if tmdb_id:
                mi_any.tmdb_id = tmdb_id
            if media_type:
                mi_any.media_type = media_type
            if title:
                mi_any.title_localized = title
            if overview:
                mi_any.synopsis_localized = overview
            if poster_url:
                mi_any.poster_path = poster_url
            if backdrop_url:
                mi_any.backdrop_path = backdrop_url
            if genres:
                mi_any.genres = ",".join(genres)
            try:
                mi_any.is_identified = True
            except Exception:
                pass
            try:
                mi_any.status = "MANUAL"
            except Exception:
                pass
            db.add(mi_any)
        except Exception:
            pass

    # Optional: download assets locally and re-point to /assets paths
    if download_assets and not translation_only:
        try:
            base_dir = Path(DATA_DIR) / "assets" / str(media_item_id)
            web_base = f"/media-assets/{media_item_id}"
            local_poster = _download_to(poster_url, base_dir / "poster.jpg") if (poster_url and str(poster_url).startswith("http")) else None
            local_backdrop = _download_to(backdrop_url, base_dir / "backdrop.jpg") if (backdrop_url and str(backdrop_url).startswith("http")) else None

            if local_poster:
                web_poster = f"{web_base}/{local_poster.name}"
                mm_any.poster_url = web_poster
                try:
                    mi_any = cast(Any, mi)
                    mi_any.poster_path = web_poster
                    db.add(mi_any)
                except Exception:
                    pass
            if local_backdrop:
                web_backdrop = f"{web_base}/{local_backdrop.name}"
                mm_any.backdrop_url = web_backdrop
                try:
                    mi_any = cast(Any, mi)
                    mi_any.backdrop_path = web_backdrop
                    db.add(mi_any)
                except Exception:
                    pass
            db.add(mm_any)
        except Exception:
            logger.exception("Failed downloading assets for %s", media_item_id)

    season_id = None
    if season_number is not None and not translation_only:
        try:
            si = db.query(models.SeasonItem).filter(models.SeasonItem.media_item_id == media_item_id).first()
            if si:
                season_id = si.season_id
            if season_id:
                s = db.query(models.Season).filter(models.Season.id == season_id).first()
                if s:
                    s_any = cast(Any, s)
                    s_any.season_number = season_number
                    if season_title:
                        if language.lower().startswith("en") and not s_any.title_en:
                            s_any.title_en = season_title
                        s_any.title_localized = season_title
                    db.add(s_any)
        except Exception:
            season_id = None

    if season_id and episode_overrides and not translation_only:
        try:
            overrides_list = [e for e in episode_overrides if isinstance(e, dict)]

            eps = (
                db.query(models.Episode, models.FileRecord)
                .join(models.EpisodeFile, models.EpisodeFile.episode_id == models.Episode.id)
                .join(models.FileRecord, models.FileRecord.id == models.EpisodeFile.file_record_id)
                .filter(models.Episode.season_id == season_id)
                .order_by(models.Episode.episode_number.asc(), models.Episode.id.asc())
                .all()
            )

            def _norm_title(v: str | None) -> str:
                try:
                    return "".join(ch.lower() for ch in str(v or "").strip() if ch.isalnum())
                except Exception:
                    return ""

            ep_by_num: dict[int, tuple[Any, Any]] = {}
            ep_by_title: dict[str, list[tuple[Any, Any]]] = {}
            for ep, fr in eps:
                try:
                    n = getattr(ep, "episode_number", None)
                    if n is not None:
                        ep_by_num[int(n)] = (ep, fr)
                except Exception:
                    pass
                try:
                    title_best = getattr(ep, "title_localized", None) or getattr(ep, "title_en", None) or getattr(ep, "title", None)
                except Exception:
                    title_best = getattr(ep, "title", None)
                nt = _norm_title(title_best)
                if nt:
                    ep_by_title.setdefault(nt, []).append((ep, fr))

            used_ids: set[int] = set()
            updated_any = False
            for idx, ov in enumerate(overrides_list):
                original_num = ov.get("original_episode_number")
                target: tuple[Any, Any] | None = None
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
                                if int(getattr(cand[0], "id", 0)) in used_ids:
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

                ep, fr = target
                try:
                    used_ids.add(int(getattr(ep, "id", 0)))
                except Exception:
                    pass

                try:
                    new_num = int(ov.get("episode_number")) if ov.get("episode_number") is not None else idx + 1
                except Exception:
                    new_num = idx + 1
                t = ov.get("title")
                ov_text = ov.get("overview")
                ep_any = cast(Any, ep)
                if t:
                    ep_any.title_localized = t
                if ov_text:
                    ep_any.synopsis_localized = ov_text
                if new_num:
                    ep_any.episode_number = new_num
                db.add(ep_any)
                try:
                    fr_any = cast(Any, fr)
                    if new_num:
                        fr_any.file_index = new_num
                        db.add(fr_any)
                except Exception:
                    pass
                updated_any = True

            if updated_any:
                for ep, fr in eps:
                    try:
                        if int(getattr(ep, "id", 0)) in used_ids:
                            continue
                    except Exception:
                        pass
                    try:
                        old_num = getattr(ep, "episode_number", None)
                        new_hidden = 10000 + (int(old_num) if old_num is not None else 0)
                        ep_any = cast(Any, ep)
                        ep_any.episode_number = new_hidden
                        db.add(ep_any)
                        fr_any = cast(Any, fr)
                        fr_any.file_index = new_hidden
                        db.add(fr_any)
                    except Exception:
                        pass

            if not updated_any:
                by_num = {}
                for e in overrides_list:
                    try:
                        n_raw = e.get("episode_number")
                        if n_raw is None:
                            continue
                        n = int(n_raw)
                    except Exception:
                        continue
                    by_num[n] = e
                eps_only = db.query(models.Episode).filter(models.Episode.season_id == season_id).all()
                for ep in eps_only:
                    n = getattr(ep, "episode_number", None)
                    if n is None:
                        continue
                    data = by_num.get(int(n))
                    if not data:
                        continue
                    t = data.get("title")
                    ov_text = data.get("overview")
                    ep_any = cast(Any, ep)
                    if t:
                        ep_any.title_localized = t
                    if ov_text:
                        ep_any.synopsis_localized = ov_text
                    db.add(ep_any)
        except Exception:
            pass

    try:
        db.commit()
    except Exception:
        db.rollback()
        return {"ok": False, "detail": "db_commit_failed"}

    try:
        poster_out = None
        backdrop_out = None
        try:
            poster_out = getattr(mi, "poster_path", None)
            backdrop_out = getattr(mi, "backdrop_path", None)
        except Exception:
            poster_out = None
            backdrop_out = None
        if not poster_out:
            try:
                poster_out = getattr(mm, "poster_url", None)
            except Exception:
                poster_out = None
        if not backdrop_out:
            try:
                backdrop_out = getattr(mm, "backdrop_url", None)
            except Exception:
                backdrop_out = None
        return {
            "ok": True,
            "item": {
                "id": media_item_id,
                "poster_path": poster_out,
                "backdrop_path": backdrop_out,
            },
        }
    except Exception:
        return {"ok": True}
