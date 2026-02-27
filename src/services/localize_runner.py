from __future__ import annotations

import logging
import threading
from typing import Optional

from ..core.config import get
from ..db import SessionLocal, models
from ..providers import provider_tmdb as tmdb
from . import enrichment as enrichment_service
from .localize_state import set_state, get_state
from .translations import upsert_translation, normalize_translation_language


logger = logging.getLogger(__name__)

_running = False
_run_lock = threading.Lock()
_stop = threading.Event()


def is_running() -> bool:
    with _run_lock:
        return bool(_running)


def request_stop() -> None:
    try:
        _stop.set()
        set_state({"stop_requested": True})
    except Exception:
        return


def _should_localize() -> tuple[bool, str]:
    try:
        lang = str(get("metadata_language") or "").strip() or "en-US"
    except Exception:
        lang = "en-US"
    try:
        enabled = bool(get("tmdb_localization")) and bool(lang) and not str(lang).lower().startswith("en")
    except Exception:
        enabled = False
    return enabled, lang


def _update_series_localized_fields(*, lang: str, limit_series: Optional[int] = None) -> dict:
    """Fill Series.title_localized and MediaItem.synopsis_localized/title_localized for TMDB-identified series."""
    db = SessionLocal()
    series_updated = 0
    items_updated = 0
    try:
        # Focus on series that either:
        # - lack a localized title, OR
        # - have at least one season media_item missing localized/global synopsis fields.
        try:
            needs_series_ids = (
                db.query(models.Season.series_id)
                .join(models.SeasonItem, models.SeasonItem.season_id == models.Season.id)
                .join(models.MediaItem, models.MediaItem.id == models.SeasonItem.media_item_id)
                .filter(
                    (models.MediaItem.synopsis_localized == None)  # noqa: E711
                    | (models.MediaItem.synopsis_localized == "")
                    | (models.MediaItem.title_localized == None)  # noqa: E711
                    | (models.MediaItem.title_localized == "")
                )
                .distinct()
            )
        except Exception:
            needs_series_ids = None

        q = db.query(models.Series).filter(models.Series.tmdb_id != None).filter(models.Series.tmdb_id != "")  # noqa: E711
        try:
            if needs_series_ids is not None:
                q = q.filter(
                    (models.Series.title_localized == None)  # noqa: E711
                    | (models.Series.title_localized == "")
                    | (models.Series.id.in_(needs_series_ids))
                )
            else:
                q = q.filter((models.Series.title_localized == None) | (models.Series.title_localized == ""))  # noqa: E711
        except Exception:
            pass
        if limit_series and int(limit_series) > 0:
            q = q.limit(int(limit_series))
        rows = q.all()
        for s in rows:
            if _stop.is_set():
                break
            tid = str(getattr(s, "tmdb_id", "") or "").strip()
            if not tid:
                continue
            try:
                d = tmdb.tmdb_fetch_by_id(tid, media_type="tv", language=lang)
            except Exception:
                d = None
            d_en = None
            # Fallback requirement: if localized fields are missing in requested language,
            # prefer TMDB English, not Jikan.
            try:
                if not (isinstance(d, dict) and (d.get("synopsis") or "").strip()):
                    d_en = tmdb.tmdb_fetch_by_id(tid, media_type="tv", language="en-US")
            except Exception:
                d_en = None
            if not isinstance(d, dict):
                continue

            # Optional Spanish fallback track (es-ES) when user language is a different Spanish region.
            d_es = None
            try:
                if normalize_translation_language(lang).lower().startswith("es-") and normalize_translation_language(lang) != "es-ES":
                    d_es = tmdb.tmdb_fetch_by_id(tid, media_type="tv", language="es-ES")
            except Exception:
                d_es = None
            try:
                tloc = (d.get("title") or d.get("series_title") or "").strip()
                if tloc:
                    s.title_localized = tloc
                    # Back-compat display title
                    s.title = tloc
            except Exception:
                pass
            synopsis = ""
            try:
                synopsis = (d.get("synopsis") or "").strip()
            except Exception:
                synopsis = ""
            if not synopsis and isinstance(d_en, dict):
                try:
                    synopsis = (d_en.get("synopsis") or "").strip()
                except Exception:
                    synopsis = ""

            try:
                year = None
                fad = d.get("first_air_date") or ""
                if isinstance(fad, str) and len(fad) >= 4 and fad[:4].isdigit():
                    year = int(fad[:4])
                if year:
                    s.year = year
            except Exception:
                pass
            try:
                img = d.get("images") or {}
                jpg = img.get("jpg") if isinstance(img, dict) else None
                if isinstance(jpg, dict):
                    poster = (jpg.get("large_image_url") or "").strip()
                    if poster:
                        s.main_poster = poster
            except Exception:
                pass

            # Also propagate to all season media_items of this series (so list/detail UI has localized fields)
            try:
                season_items = (
                    db.query(models.SeasonItem, models.Season, models.MediaItem)
                    .join(models.Season, models.Season.id == models.SeasonItem.season_id)
                    .join(models.MediaItem, models.MediaItem.id == models.SeasonItem.media_item_id)
                    .filter(models.Season.series_id == s.id)
                    .all()
                )
            except Exception:
                season_items = []
            for _si, _season, mi in season_items:
                try:
                    if not (getattr(mi, "title_localized", None) or "").strip() and (s.title_localized or "").strip():
                        mi.title_localized = s.title_localized
                        mi.title = s.title_localized
                        items_updated += 1
                    if synopsis and ((getattr(mi, "synopsis_localized", None) or "").strip() != synopsis):
                        mi.synopsis_localized = synopsis
                        items_updated += 1
                    # Store translation rows per season media_item_id so UI can resolve deterministically.
                    try:
                        upsert_translation(
                            db,
                            path_id=int(mi.id),
                            language=lang,
                            source="tmdb",
                            title=(s.title_localized or None),
                            overview=(synopsis or None),
                        )
                        # If available, persist es-ES track for better fallbacks when es-MX is incomplete.
                        try:
                            if isinstance(d_es, dict):
                                es_title = (d_es.get("title") or d_es.get("series_title") or "").strip() or None
                                es_ov = (d_es.get("synopsis") or "").strip() or None
                                upsert_translation(db, path_id=int(mi.id), language="es-ES", source="tmdb", title=es_title, overview=es_ov)
                                upsert_translation(db, path_id=int(mi.id), language="es", source="tmdb", title=es_title, overview=es_ov)
                        except Exception:
                            pass
                        # Also store base language key for fallbacks (e.g. es from es-MX).
                        try:
                            base_lang = normalize_translation_language(lang).split("-", 1)[0]
                            if base_lang and base_lang not in ("en", normalize_translation_language(lang)):
                                upsert_translation(
                                    db,
                                    path_id=int(mi.id),
                                    language=base_lang,
                                    source="tmdb",
                                    title=(s.title_localized or None),
                                    overview=(synopsis or None),
                                )
                        except Exception:
                            pass
                        if isinstance(d_en, dict):
                            en_title = (d_en.get("title") or d_en.get("series_title") or "").strip() or None
                            en_ov = (d_en.get("synopsis") or "").strip() or None
                            upsert_translation(db, path_id=int(mi.id), language="en", source="tmdb", title=en_title, overview=en_ov)
                    except Exception:
                        pass
                    db.add(mi)
                except Exception:
                    continue

            try:
                db.add(s)
                db.commit()
                series_updated += 1
            except Exception:
                db.rollback()
                continue
        return {"ok": True, "series_updated": int(series_updated), "items_updated": int(items_updated)}
    finally:
        db.close()


def _update_movie_translations(*, lang: str, limit_items: Optional[int] = None) -> dict:
    """Fill media_translations for TMDB-identified movies (series table doesn't cover them)."""
    db = SessionLocal()
    updated = 0
    try:
        q = (
            db.query(models.MediaItem)
            .filter(models.MediaItem.media_type != None)  # noqa: E711
            .filter(models.MediaItem.media_type.ilike("movie"))
            .filter(models.MediaItem.tmdb_id != None)  # noqa: E711
            .filter(models.MediaItem.tmdb_id != "")
        )
        if limit_items and int(limit_items) > 0:
            q = q.limit(int(limit_items))
        rows = q.all()
        for mi in rows:
            if _stop.is_set():
                break
            tid = str(getattr(mi, "tmdb_id", "") or "").strip()
            if not tid:
                continue
            try:
                d_loc = tmdb.tmdb_fetch_by_id(tid, media_type="movie", language=lang)
            except Exception:
                d_loc = None
            try:
                d_en = tmdb.tmdb_fetch_by_id(tid, media_type="movie", language="en-US")
            except Exception:
                d_en = None
            d_es = None
            try:
                if normalize_translation_language(lang).lower().startswith("es-") and normalize_translation_language(lang) != "es-ES":
                    d_es = tmdb.tmdb_fetch_by_id(tid, media_type="movie", language="es-ES")
            except Exception:
                d_es = None

            tloc = (d_loc.get("title") if isinstance(d_loc, dict) else None) or None
            ovloc = (d_loc.get("synopsis") if isinstance(d_loc, dict) else None) or None
            try:
                upsert_translation(db, path_id=int(mi.id), language=lang, source="tmdb", title=tloc, overview=ovloc)
                try:
                    if isinstance(d_es, dict):
                        es_title = (d_es.get("title") or d_es.get("series_title") or "").strip() or None
                        es_ov = (d_es.get("synopsis") or "").strip() or None
                        upsert_translation(db, path_id=int(mi.id), language="es-ES", source="tmdb", title=es_title, overview=es_ov)
                        upsert_translation(db, path_id=int(mi.id), language="es", source="tmdb", title=es_title, overview=es_ov)
                except Exception:
                    pass
                try:
                    base_lang = normalize_translation_language(lang).split("-", 1)[0]
                    if base_lang and base_lang not in ("en", normalize_translation_language(lang)):
                        upsert_translation(db, path_id=int(mi.id), language=base_lang, source="tmdb", title=tloc, overview=ovloc)
                except Exception:
                    pass
                if isinstance(d_en, dict):
                    upsert_translation(db, path_id=int(mi.id), language="en", source="tmdb", title=d_en.get("title"), overview=d_en.get("synopsis"))
            except Exception:
                pass
            try:
                if isinstance(tloc, str) and tloc.strip():
                    mi.title_localized = tloc.strip()
                    mi.title = tloc.strip()
                if isinstance(ovloc, str) and ovloc.strip():
                    mi.synopsis_localized = ovloc.strip()
                db.add(mi)
                db.commit()
                updated += 1
            except Exception:
                db.rollback()
                continue
        return {"ok": True, "items_updated": int(updated)}
    finally:
        db.close()


def _localize_worker(limit_series: Optional[int], limit_seasons: Optional[int]) -> None:
    global _running
    try:
        res = run_localization_once(limit_series=limit_series, limit_seasons=limit_seasons, manage_state=True)
        if not isinstance(res, dict):
            set_state({"running": False, "current_step": "idle", "current_title": None})
    except Exception as e:
        logger.exception("Localization runner failed")
        set_state({"running": False, "current_step": "error", "error": str(e)})
    finally:
        with _run_lock:
            _running = False
        try:
            _stop.clear()
        except Exception:
            pass


def start_localization_job(limit_series: Optional[int] = None, limit_seasons: Optional[int] = None) -> bool:
    """Start TMDB localization job in a background thread (non-blocking)."""
    global _running
    with _run_lock:
        if _running:
            return False
        _running = True
    try:
        _stop.clear()
    except Exception:
        pass
    try:
        prev = get_state()
        if prev.get("running"):
            return False
    except Exception:
        pass
    set_state({"running": True, "current_step": "starting", "current_title": None, "error": None})
    t = threading.Thread(
        target=_localize_worker,
        args=(limit_series, limit_seasons),
        daemon=True,
        name="localization-runner",
    )
    t.start()
    return True


def run_localization_once(
    *,
    limit_series: Optional[int] = None,
    limit_seasons: Optional[int] = None,
    manage_state: bool = False,
) -> dict:
    """Run the localization flow synchronously (useful for CLI/tests)."""
    enabled, lang = _should_localize()
    if not enabled:
        out = {"ok": True, "detail": "localization_disabled", "lang": lang}
        if manage_state:
            set_state({"running": False, "current_step": "idle", "current_title": None, "result": out})
        return out

    if manage_state:
        set_state({"running": True, "current_step": "tmdb_sync", "current_title": "TMDB: sync", "error": None})

    sync_res = enrichment_service.tmdb_sync_from_jikan_title_en(limit_series=limit_series, manage_state=False)
    if _stop.is_set():
        out = {"ok": False, "detail": "stopped"}
        if manage_state:
            set_state({"running": False, "current_step": "stopped", "result": out})
        return out
    # If TMDB sync fails (e.g., not configured/available), skip the rest to avoid looping.
    try:
        if not bool(sync_res.get("ok", True)):
            out = {"ok": False, "detail": "tmdb_sync_failed", "lang": lang, "tmdb_sync": sync_res}
            if manage_state:
                set_state({"running": False, "current_step": "idle", "current_title": None, "result": out})
            return out
    except Exception:
        pass

    if manage_state:
        set_state({"current_step": "series_localize", "current_title": "TMDB: series i18n"})
    series_res = _update_series_localized_fields(lang=lang, limit_series=limit_series)
    if _stop.is_set():
        out = {"ok": False, "detail": "stopped"}
        if manage_state:
            set_state({"running": False, "current_step": "stopped", "result": out})
        return out

    if manage_state:
        set_state({"current_step": "movie_localize", "current_title": "TMDB: movies i18n"})
    movie_res = _update_movie_translations(lang=lang, limit_items=limit_series)

    if manage_state:
        set_state({"current_step": "backfill_episodes", "current_title": "TMDB: episodes i18n"})
    ep_res = enrichment_service.backfill_tmdb_episode_titles(limit_seasons=limit_seasons, manage_state=False)

    out = {
        "ok": True,
        "lang": lang,
        "tmdb_sync": sync_res,
        "series_localize": series_res,
        "movie_localize": movie_res,
        "backfill_episodes": ep_res,
    }
    if manage_state:
        set_state({"running": False, "current_step": "idle", "current_title": None, "result": out})
    return out
