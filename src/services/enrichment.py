"""Enrichment service: fetch and persist metadata from providers.

This module is the project's enrichment logic adapted from the provided
`enrichment.py` attachment. It locates pending `MediaItem` rows without
metadata, calls providers (Jikan/TMDB) to obtain metadata, normalizes the
responses and persists them into `media_metadata` and optional `media_image`.

Key public functions:
- `enrich_pending(limit=None) -> int` : process up to `limit` pending items.
- `enrich_one(media_item_id, providers_override=None) -> bool` : enrich a single item.
- `enrich_pending_detailed(limit=None, dry_run=False) -> dict` : detailed report.
"""
from typing import Optional, Iterable, Any
import json
import logging
import time
import hashlib
import os
import re
import threading
from pathlib import Path, PurePath
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy import func

from ..db import SessionLocal
from ..db.models import (
    MediaItem,
    FileRecord,
    MediaMetadata,
    MediaImage,
    MediaRelation,
    Series,
    Season,
    Episode,
    SeasonItem,
)
from typing import Any as _Any, Optional as _Optional, cast as _cast
try:
    from ..services.classifier_enhanced import classify_metadata
except Exception:
    def classify_metadata(normalized: dict) -> _Optional[str]:
        return None
from .enrich_state import set_state, clear_state
from ..core.config import get

# Providers: prefer package-relative imports, but fall back to absolute `src.*` when running in odd contexts.
try:
    from ..providers import provider_jikan as jikan
except Exception:
    try:
        from src.providers import provider_jikan as jikan  # type: ignore
    except Exception:
        jikan = None
try:
    # dynamic import; static analyzer may not know the symbol exists
    from ..providers import provider_tmdb as tmdb  # type: ignore
except Exception:
    try:
        from src.providers import provider_tmdb as tmdb  # type: ignore
    except Exception:
        tmdb = None

logger = logging.getLogger(__name__)

# Throttle image downloads to reduce network spikes.
_IMG_CONCURRENCY = max(1, int(os.environ.get("ARCANEA_IMAGE_CONCURRENCY", "2") or 2))
_IMG_DELAY_SEC = max(0.0, float(os.environ.get("ARCANEA_IMAGE_DELAY_MS", "100") or 100) / 1000.0)
_img_sema = None
try:
    _img_sema = threading.BoundedSemaphore(_IMG_CONCURRENCY)
except Exception:
    _img_sema = None
_image_session = requests.Session()
_image_session.mount('http://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504))))
_image_session.mount('https://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504))))

# Best-effort TMDB series cache to improve season-folder resolution.
# Keyed by normalized base title -> tmdb_id (TV).
_TMDB_SERIES_CACHE: dict[str, str] = {}
_TMDB_SERIES_CACHE_LOCK = threading.Lock()


def _normalize_provider_name(p: str | None, default: str) -> str:
    try:
        pn = (p or '').strip().lower()
    except Exception:
        pn = ''
    return pn or default


def _fallback_title_for_item(mi: MediaItem) -> str:
    try:
        if getattr(mi, "title", None):
            return str(getattr(mi, "title") or "")
    except Exception:
        pass
    try:
        bt = getattr(mi, "base_title", None)
        if isinstance(bt, str) and bt.strip():
            return bt.strip()
    except Exception:
        pass
    try:
        cp = getattr(mi, "canonical_path", None)
        if cp:
            p = Path(str(cp))
            name = p.stem or p.name
            if name:
                return str(name)
    except Exception:
        pass
    return "Sin Nombre"

def _candidate_titles_for_item(mi: MediaItem) -> list[str]:
    """Build candidate titles in priority order.
    1) search_titles (if present)
    2) canonical_path basename
    3) title (if present)
    """
    candidates: list[str] = []
    # If the canonical folder looks like a season subfolder (e.g. ".../Halo/Temporada 2"),
    # prefer using "<parent> <basename>" so TMDB can resolve the correct series+season.
    try:
        cp0 = getattr(mi, 'canonical_path', None)
        if cp0:
            cp = str(cp0)
            base = os.path.basename(cp).strip()
            parent = os.path.basename(os.path.dirname(cp)).strip()
            if base and parent:
                # basename-only season folders (e.g. "Temporada 2", "Season 2", "S2")
                if re.search(r"^(?:temporada|season|temp(?:\.|orada)?|s)\s*\d{1,2}$", base, re.IGNORECASE):
                    combined = f"{parent} {base}".strip()
                    if combined and combined not in candidates:
                        candidates.append(combined)
                    if parent and parent not in candidates:
                        candidates.append(parent)
    except Exception:
        pass
    try:
        st = getattr(mi, 'search_titles', None)
        parsed = None
        if st:
            if isinstance(st, str):
                try:
                    parsed = json.loads(st)
                except Exception:
                    parsed = None
            else:
                parsed = st
        if isinstance(parsed, list):
            for t in parsed:
                if isinstance(t, str) and t and t not in candidates:
                    candidates.append(t)
    except Exception:
        pass
    try:
        cp = getattr(mi, 'canonical_path', None)
        if cp:
            base = os.path.basename(str(cp))
            if base and base not in candidates:
                candidates.append(base)
    except Exception:
        pass
    try:
        title = getattr(mi, 'title', None)
        if isinstance(title, str) and title and title not in candidates:
            candidates.append(title)
    except Exception:
        pass

    # Extra hardening: ensure the first candidate is a "clean" base title to improve Jikan matches.
    # This helps titles like "Aldnoah Zero Part 2" or "... Especiales" where the suffix confuses search.
    try:
        def _clean_title(s: str) -> str:
            t = str(s or "").strip()
            if not t:
                return ""
            t = re.sub(r"\[.*?\]|\(.*?\)|\{.*?\}", " ", t)
            t = re.sub(r"(?i)\b(?:part|cour|season|temporada|temp(?:\.|orada)?)\s*0*\d{1,2}\b", " ", t)
            t = re.sub(r"(?i)\b(?:especial(?:es)?|specials?|ova(?:s)?)\b", " ", t)
            t = re.sub(r"[\-_]+", " ", t)
            t = re.sub(r"\s{2,}", " ", t).strip()
            return t

        for raw in list(candidates)[:3]:
            cleaned = _clean_title(raw)
            if cleaned and cleaned not in candidates:
                candidates.insert(0, cleaned)
    except Exception:
        pass
    return candidates


def _extract_season_from_title(title: str | None) -> tuple[str, int | None]:
    """Return (base_title, season_number) if the title looks like a season folder."""
    try:
        t0 = (title or "").strip()
    except Exception:
        return "", None
    if not t0:
        return "", None

    t = t0
    season = None
    try:
        m = re.search(r"\b(?:temporada|season|temp(?:\.|orada)?)\s*(\d{1,2})\b", t, re.IGNORECASE)
        if m:
            season = int(m.group(1))
            t = re.sub(r"\b(?:temporada|season|temp(?:\.|orada)?)\s*\d{1,2}\b", "", t, flags=re.IGNORECASE)
    except Exception:
        season = None
    if season is None:
        try:
            m = re.search(r"\bs(\d{1,2})\b", t, re.IGNORECASE)
            if m:
                season = int(m.group(1))
                t = re.sub(r"\bs\d{1,2}\b", "", t, flags=re.IGNORECASE)
        except Exception:
            season = None

    try:
        t = re.sub(r"[\-_]+", " ", t).strip()
        t = re.sub(r"\s{2,}", " ", t).strip()
    except Exception:
        pass

    return t or t0, season


def _inherit_tmdb_identity_from_parent_if_season(mi: MediaItem, session) -> tuple[str, int] | None:
    """If `mi` looks like a season subfolder, inherit TMDB identity from its parent folder.

    Rule: if parent MediaItem (canonical_path parent) already has tmdb_id and is tv/series, we must NOT
    perform a new TMDB search for the child. We directly fetch `/tv/{tmdb_id}/season/{season_number}`.
    """
    try:
        cp = getattr(mi, "canonical_path", None)
        if not cp:
            return None
        cp_s = str(cp)
    except Exception:
        return None

    try:
        folder_name = os.path.basename(cp_s).strip()
    except Exception:
        folder_name = ""
    if not folder_name:
        return None

    try:
        _base, season = _extract_season_from_title(folder_name)
    except Exception:
        season = None
    if not season or int(season) <= 0:
        return None

    parent_path = None
    try:
        parent_path = os.path.dirname(cp_s)
    except Exception:
        parent_path = None
    if not parent_path:
        return None

    parent = None
    try:
        parent = session.query(MediaItem).filter(MediaItem.canonical_path == str(Path(parent_path).resolve())).first()
    except Exception:
        parent = None
    if not parent:
        return None

    try:
        tmdb_id = getattr(parent, "tmdb_id", None)
        if not tmdb_id:
            return None
        pmt = (getattr(parent, "media_type", None) or "").strip().lower()
        if pmt not in ("tv", "series", "show"):
            return None
    except Exception:
        return None

    return (str(tmdb_id), int(season))


def _tmdb_search_with_optional_season(candidate: str, media_type: str | None) -> dict | None:
    """TMDB search that upgrades season-folder names into season metadata (episodes, synopsis)."""
    if tmdb is None:
        return None
    base, season = _extract_season_from_title(candidate)
    # If it looks like a season folder, force a tv search on the base name.
    if season is not None and season > 0 and hasattr(tmdb, 'search_by_type'):
        cache_key = (base or "").strip().lower()
        if cache_key:
            with _TMDB_SERIES_CACHE_LOCK:
                cached_id = _TMDB_SERIES_CACHE.get(cache_key)
            if cached_id and hasattr(tmdb, 'tmdb_fetch_season'):
                sd = tmdb.tmdb_fetch_season(str(cached_id), season_number=season)
                if sd:
                    return sd
        series_res = tmdb.search_by_type(base, media_preference='tv')
        if isinstance(series_res, list) and series_res:
            series_res = series_res[0]
        if isinstance(series_res, dict):
            try:
                tid_cache = series_res.get('tmdb_id') or series_res.get('provider_id')
                if tid_cache and cache_key:
                    with _TMDB_SERIES_CACHE_LOCK:
                        _TMDB_SERIES_CACHE[cache_key] = str(tid_cache)
            except Exception:
                pass
            # Fetch season-level metadata if possible
            tid = series_res.get('tmdb_id') or series_res.get('provider_id')
            if tid and hasattr(tmdb, 'tmdb_fetch_season'):
                sd = tmdb.tmdb_fetch_season(str(tid), season_number=season)
                if sd:
                    return sd
            return series_res
    # Default behavior
    if hasattr(tmdb, 'search_by_type'):
        res = tmdb.search_by_type(candidate, media_preference=media_type)
    else:
        res = tmdb.search(candidate) if hasattr(tmdb, 'search') else None
    # Cache successful TV ids by base title to help future season items.
    try:
        if isinstance(res, dict) and (res.get('media_type') or '').lower().startswith('tv'):
            tid_cache = res.get('tmdb_id') or res.get('provider_id')
            b2, _s2 = _extract_season_from_title(candidate)
            ck = (b2 or "").strip().lower()
            if tid_cache and ck:
                with _TMDB_SERIES_CACHE_LOCK:
                    _TMDB_SERIES_CACHE[ck] = str(tid_cache)
    except Exception:
        pass
    return res


def _provider_order_for_item(media_type: str | None, mal_id: Any, cfg_movies: str | None, cfg_anime: str | None, media_root: str | None = None) -> list[str]:
    mt = (media_type or '').strip().lower()
    try:
        if media_root:
            base = os.path.basename(str(media_root)).lower()
            if 'anime' in base or 'animes' in base:
                mt = 'anime'
    except Exception:
        pass
    if mal_id or mt == 'anime':
        primary = _normalize_provider_name(cfg_anime, 'jikan')
        if primary not in ('jikan', 'tmdb'):
            primary = 'jikan'
        # For anime libraries (or when MAL id exists), avoid falling back to TMDB by default.
        # TMDB matches are often incorrect for anime folders and can replace good anime metadata.
        return [primary]
    primary = _normalize_provider_name(cfg_movies, 'tmdb')
    if primary not in ('tmdb', 'jikan'):
        primary = 'tmdb'
    order = [primary]
    if primary == 'jikan':
        order.append('tmdb')
    return order


def _decide_media_type(mi: MediaItem, session) -> str:
    try:
        count = session.query(FileRecord).filter(FileRecord.media_item_id == mi.id).count()
        if count > 1:
            return 'series'
    except Exception:
        pass
    return 'movie'


def _normalize_tmdb(res: dict) -> dict:
    if not res:
        return {}
    tmdb_id = res.get('tmdb_id') or res.get('provider_id') or res.get('id')
    genres = []
    try:
        if isinstance(res.get('genres'), list):
            for g in res.get('genres') or []:
                if isinstance(g, dict):
                    if g.get('name'):
                        genres.append(g.get('name'))
                elif isinstance(g, str):
                    genres.append(g)
        elif isinstance(res.get('genre_ids'), list):
            genres = res.get('genre_ids')
    except Exception:
        genres = res.get('genres') or []
    return {
        'provider': 'tmdb',
        'provider_id': str(tmdb_id) if tmdb_id is not None else '',
        'tmdb_id': tmdb_id,
        'title': res.get('title') or res.get('series_title') or res.get('name') or '',
        'series_title': res.get('series_title') or res.get('name') or res.get('title') or '',
        'synopsis': res.get('synopsis') or res.get('overview') or '',
        'genres': genres,
        'images': res.get('images') or {},
        'media_type': (res.get('media_type') or res.get('media_type') or '').lower() or None,
        'season_number': res.get('season_number') if res.get('season_number') is not None else None,
        'total_seasons': res.get('total_seasons') if res.get('total_seasons') is not None else None,
        'release_date': res.get('release_date') or res.get('first_air_date') or None,
        'resolved_from_folder': True if (res.get('season_number') is not None or res.get('total_seasons') is not None) else False,
        'raw': res,
    }


def _normalize_jikan(res: dict) -> dict:
    if not res:
        return {}
    provider_id = None
    title = ''
    synopsis = ''
    genres = []
    images = {}
    url = None
    titles = []
    title_english = None
    title_japanese = None
    title_synonyms = []
    anime_type = None
    source = None
    episodes_count = None
    status = None
    airing = None
    aired = None
    duration = None
    rating = None
    background = None
    season = None
    year = None
    broadcast = None
    explicit_genres = []
    themes = []
    demographics = []
    relations = []
    theme_songs = None
    studios = []
    producers = []
    licensors = []
    score = None
    runtime = None
    try:
        provider_id = str(res.get('mal_id') or res.get('id') or '')
    except Exception:
        provider_id = ''
    try:
        title = res.get('title') or res.get('title_english') or res.get('title_japanese') or ''
    except Exception:
        title = ''
    try:
        synopsis = res.get('synopsis') or res.get('overview') or ''
    except Exception:
        synopsis = ''
    # Clean synopsis only for Jikan: strip trailing source/credits and bad attributions.
    try:
        from ..utils.utils import limpiar_traduccion  # local import to avoid import-order issues
        synopsis = limpiar_traduccion(synopsis, silent=True)
    except Exception:
        pass
    try:
        url = res.get('url') or None
    except Exception:
        url = None
    try:
        tts = res.get('titles') or []
        if isinstance(tts, list):
            titles = [x for x in tts if isinstance(x, dict) and x.get('title')]
    except Exception:
        titles = []
    try:
        te = res.get('title_english')
        title_english = str(te) if te else None
    except Exception:
        title_english = None
    try:
        tj = res.get('title_japanese')
        title_japanese = str(tj) if tj else None
    except Exception:
        title_japanese = None
    try:
        syns = res.get('title_synonyms') or []
        if isinstance(syns, list):
            title_synonyms = [str(x) for x in syns if x]
    except Exception:
        title_synonyms = []
    try:
        if res.get('type'):
            anime_type = str(res.get('type'))
    except Exception:
        anime_type = None
    try:
        if res.get('source'):
            source = str(res.get('source'))
    except Exception:
        source = None
    try:
        genres = [g.get('name') if isinstance(g, dict) else str(g) for g in (res.get('genres') or [])]
    except Exception:
        genres = []
    try:
        img = res.get('images')
        if isinstance(img, dict) and img:
            # Keep provider shape (jpg/webp with image_url/large_image_url) for the UI.
            images = img
        else:
            images = {}
    except Exception:
        images = {}
    try:
        s = res.get('score')
        if s is not None:
            score = float(s)
    except Exception:
        score = None
    try:
        if res.get('status'):
            status = str(res.get('status'))
    except Exception:
        status = None
    try:
        if res.get('episodes') is not None:
            ev = res.get('episodes')
            try:
                episodes_count = int(ev)  # type: ignore[arg-type]
            except Exception:
                episodes_count = None
    except Exception:
        episodes_count = None
    try:
        if res.get('airing') is not None:
            airing = bool(res.get('airing'))
    except Exception:
        airing = None
    try:
        a = res.get('aired')
        aired = a if isinstance(a, dict) else None
    except Exception:
        aired = None
    try:
        d = res.get('duration')
        duration = str(d) if d else None
    except Exception:
        duration = None
    try:
        r = res.get('rating')
        rating = str(r) if r else None
    except Exception:
        rating = None
    try:
        b = res.get('background')
        background = str(b) if b else None
    except Exception:
        background = None
    try:
        if res.get('season'):
            season = str(res.get('season'))
    except Exception:
        season = None
    try:
        if res.get('year') is not None:
            yv = res.get('year')
            try:
                year = int(yv)  # type: ignore[arg-type]
            except Exception:
                year = None
    except Exception:
        year = None
    try:
        br = res.get('broadcast')
        broadcast = br if isinstance(br, dict) else None
    except Exception:
        broadcast = None
    try:
        eg = res.get('explicit_genres') or []
        if isinstance(eg, list):
            explicit_genres = [x.get('name') if isinstance(x, dict) else str(x) for x in eg if x]
    except Exception:
        explicit_genres = []
    try:
        th = res.get('themes') or []
        if isinstance(th, list):
            themes = [x.get('name') if isinstance(x, dict) else str(x) for x in th if x]
    except Exception:
        themes = []
    try:
        demo = res.get('demographics') or []
        if isinstance(demo, list):
            demographics = [x.get('name') if isinstance(x, dict) else str(x) for x in demo if x]
    except Exception:
        demographics = []
    try:
        rels = res.get('relations') or []
        if isinstance(rels, list):
            for rel in rels:
                if not isinstance(rel, dict):
                    continue
                rel_name = rel.get('relation')
                entries = []
                ent = rel.get('entry') or []
                if isinstance(ent, list):
                    for e in ent:
                        if not isinstance(e, dict):
                            continue
                        entries.append({
                            'mal_id': e.get('mal_id'),
                            'type': e.get('type'),
                            'name': e.get('name'),
                            'url': e.get('url'),
                        })
                relations.append({'relation': rel_name, 'entry': entries})
    except Exception:
        relations = []
    try:
        thm = res.get('theme')
        if isinstance(thm, dict):
            ops = thm.get('openings') if isinstance(thm.get('openings'), list) else []
            ens = thm.get('endings') if isinstance(thm.get('endings'), list) else []
            # Avoid bloating the DB; keep a reasonable sample.
            theme_songs = {
                'openings': [str(x) for x in (ops or []) if x][:20],
                'endings': [str(x) for x in (ens or []) if x][:20],
            }
    except Exception:
        theme_songs = None
    try:
        st = res.get('studios') or []
        if isinstance(st, list):
            studios = [x.get('name') if isinstance(x, dict) else str(x) for x in st if x]
    except Exception:
        studios = []
    try:
        pr = res.get('producers') or []
        if isinstance(pr, list):
            producers = [x.get('name') if isinstance(x, dict) else str(x) for x in pr if x]
    except Exception:
        producers = []
    try:
        lc = res.get('licensors') or []
        if isinstance(lc, list):
            licensors = [x.get('name') if isinstance(x, dict) else str(x) for x in lc if x]
    except Exception:
        licensors = []
    try:
        # Jikan duration is often like "24 min per ep"
        dur = res.get('duration')
        if isinstance(dur, str):
            import re as _re
            m = _re.search(r'(\d+)\s*min', dur.lower())
            if m:
                runtime = int(m.group(1))
    except Exception:
        runtime = None
    # Map Jikan type to app media_type (movie vs series). Keep original in `anime_type`.
    norm_media_type = 'series'
    try:
        if isinstance(anime_type, str) and anime_type.strip().lower() == 'movie':
            norm_media_type = 'movie'
    except Exception:
        norm_media_type = 'series'
    return {
        'provider': 'jikan',
        'provider_id': provider_id,
        'mal_id': provider_id,
        'url': url,
        'titles': titles,
        'title': title,
        'title_english': title_english,
        'title_japanese': title_japanese,
        'title_synonyms': title_synonyms,
        'anime_type': anime_type,
        'source': source,
        'synopsis': synopsis,
        'background': background,
        'genres': genres,
        'explicit_genres': explicit_genres,
        'themes': themes,
        'demographics': demographics,
        'relations': relations,
        'theme': theme_songs,
        'aired': aired,
        'broadcast': broadcast,
        'duration': duration,
        'rating': rating,
        'images': images,
        'score': score,
        'status': status,
        'episodes_count': episodes_count,
        'season': season,
        'year': year,
        'runtime': runtime,
        'studios': studios,
        'producers': producers,
        'licensors': licensors,
        'media_type': norm_media_type,
        'raw': res,
    }


def _normalize_result(provider_name: str, res: dict) -> dict:
    if not res:
        return {}
    if provider_name == 'tmdb' or (isinstance(res, dict) and res.get('provider') == 'tmdb'):
        return _normalize_tmdb(res)
    return _normalize_jikan(res)


def _attach_episodes(provider_name: str, normalized: dict, media_type: str | None, media_item: MediaItem) -> None:
    """Attach an episode listing (best-effort) to the normalized payload.

    Stored inside MediaMetadata.data JSON; no DB schema change required.
    """
    try:
        if not isinstance(normalized, dict) or normalized.get('episodes') is not None:
            return

        mt = (media_type or normalized.get('media_type') or '').strip().lower()
        is_series = mt in ('series', 'tv', 'show')
        # Be conservative for TMDB: avoid fetching episodes for movies.
        if provider_name == 'tmdb' and not is_series:
            return
        # For Jikan: only fetch episodes for series-like items (avoid movie/anime movies).
        if provider_name == 'jikan' and not is_series:
            return

        if provider_name == 'jikan' and jikan is not None and hasattr(jikan, 'obtener_episodios'):
            mal_id = None
            try:
                mal_id = getattr(media_item, 'mal_id', None) or normalized.get('provider_id')
            except Exception:
                mal_id = normalized.get('provider_id')
            if not mal_id:
                return
            raw_eps = jikan.obtener_episodios(str(mal_id))
            if not raw_eps:
                return
            episodes = []
            for ep in raw_eps:
                if not isinstance(ep, dict):
                    continue
                episodes.append({
                    'title': ep.get('title') or ep.get('title_romanji') or ep.get('title_japanese') or '',
                    'aired': ep.get('aired'),
                    'filler': ep.get('filler'),
                    'recap': ep.get('recap'),
                    'url': ep.get('url') or ep.get('forum_url'),
                    'id': ep.get('mal_id') or ep.get('id'),
                })
            normalized['episodes'] = episodes
            return

        if provider_name == 'tmdb' and tmdb is not None and hasattr(tmdb, 'tmdb_get_episodes'):
            tmdb_id = normalized.get('tmdb_id') or normalized.get('provider_id')
            if not tmdb_id:
                return
            # Prefer i18n-based episodes list if available (allows per-season lists)
            try:
                raw = normalized.get('raw') or {}
                i18n = raw.get('i18n') if isinstance(raw, dict) else None
                primary = None
                if isinstance(raw, dict):
                    langs = raw.get('languages') or {}
                    if isinstance(langs, dict):
                        primary = langs.get('primary')
                primary_key = 'en' if str(primary or '').lower().startswith('en') else ('es' if str(primary or '').lower().startswith('es') else 'en')
                if isinstance(i18n, dict):
                    lang_obj = i18n.get(primary_key) or {}
                    seasons = lang_obj.get('seasons') if isinstance(lang_obj, dict) else None
                    if isinstance(seasons, dict):
                        season_number = normalized.get('season_number')
                        out = []
                        if season_number is not None:
                            s = seasons.get(str(season_number))
                            if isinstance(s, dict):
                                for ep in s.get('episodes') or []:
                                    if isinstance(ep, dict) and ep.get('title'):
                                        out.append({'title': ep.get('title')})
                        else:
                            # Flatten all seasons in order
                            def _sn_sort(x):
                                try:
                                    return int(x)
                                except Exception:
                                    return 0
                            for sn in sorted(seasons.keys(), key=_sn_sort):
                                s = seasons.get(sn)
                                if not isinstance(s, dict):
                                    continue
                                for ep in s.get('episodes') or []:
                                    if isinstance(ep, dict) and ep.get('title'):
                                        out.append({'title': ep.get('title')})
                        if out:
                            normalized['episodes'] = out
                            return
            except Exception:
                pass

            eps = tmdb.tmdb_get_episodes(str(tmdb_id))
            if eps:
                normalized['episodes'] = eps
            return
    except Exception:
        return


def _attach_tmdb_i18n(normalized: dict, media_type: str | None) -> None:
    """Attach TMDB i18n payload (ES/EN) into normalized['raw'] (best-effort)."""
    try:
        if not isinstance(normalized, dict):
            return
        if (normalized.get('provider') or '').strip().lower() != 'tmdb':
            return

        mt = (media_type or normalized.get('media_type') or '').strip().lower()
        is_series = mt in ('series', 'tv', 'show')
        if not is_series:
            return

        tmdb_id = normalized.get('tmdb_id') or normalized.get('provider_id')
        if not tmdb_id:
            return

        season_number = None
        try:
            if normalized.get('season_number') is not None:
                season_number = int(normalized.get('season_number'))
        except Exception:
            season_number = None

        try:
            lang = get('metadata_language') or 'en-US'
        except Exception:
            lang = 'en-US'
        primary = 'es' if str(lang).lower().startswith('es') else 'en'

        include_all = True if season_number is None else False
        sns = [season_number] if season_number is not None else None

        try:
            from .tmdb_i18n import fetch_tmdb_tv_i18n_clean
        except Exception:
            return

        cleaned = fetch_tmdb_tv_i18n_clean(str(tmdb_id), primary=primary, season_numbers=sns, include_all_seasons=include_all)
        if not isinstance(cleaned, dict):
            return

        raw = normalized.get('raw')
        if not isinstance(raw, dict):
            raw = {}
            normalized['raw'] = raw
        raw['languages'] = cleaned.get('languages')
        raw['i18n'] = cleaned.get('i18n')
        raw['modal'] = cleaned.get('modal')
    except Exception:
        return


def _tmdb_configured() -> bool:
    try:
        key = get('tmdb_api_key') or os.environ.get('TMDB_API_KEY')
        token = get('tmdb_access_token') or os.environ.get('TMDB_ACCESS_TOKEN')
        use_v4 = bool(get('tmdb_use_v4'))
        if use_v4 and token:
            return True
        if key:
            return True
        try:
            from ..core.config import DATA_DIR
            import json as _json
            app_cfg_path = DATA_DIR / 'app_config.json'
            if app_cfg_path.exists():
                with open(app_cfg_path, 'r', encoding='utf-8') as fh:
                    ac = _json.load(fh) or {}
                md = ac.get('metadata') or {}
                if md.get('tmdb_api_key'):
                    return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def _download_image_and_record(session, media_item: MediaItem, image_url: str, source: str = 'provider') -> str | None:
    try:
        if not image_url:
            return None
        posters_dir = Path('data') / 'posters'
        posters_dir.mkdir(parents=True, exist_ok=True)
        h = hashlib.sha1(image_url.encode('utf-8')).hexdigest()
        ext = os.path.splitext(PurePath(image_url).name)[1] or '.jpg'
        fn = f"{h}{ext}"
        local_path = str((posters_dir / fn).resolve())
        if not os.path.exists(local_path):
            try:
                resp = None
                if _img_sema:
                    _img_sema.acquire()
                try:
                    resp = _image_session.get(image_url, timeout=15, stream=True)
                    resp.raise_for_status()
                    with open(local_path, 'wb') as fh:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                fh.write(chunk)
                finally:
                    try:
                        if resp is not None:
                            resp.close()
                    except Exception:
                        pass
                    if _img_sema:
                        _img_sema.release()
                    if _IMG_DELAY_SEC > 0:
                        time.sleep(_IMG_DELAY_SEC)
            except Exception:
                return None
        try:
            mi_obj = media_item
            existing = session.query(MediaImage).filter(MediaImage.media_item_id == mi_obj.id, MediaImage.local_path == local_path).first()
            if not existing:
                mi_img = MediaImage(media_item_id=mi_obj.id, source=source, source_url=image_url, local_path=local_path, priority=50)
                session.add(mi_img)
                session.commit()
            if not getattr(mi_obj, 'poster_path', None):
                mi_obj.poster_path = local_path # pyright: ignore[reportAttributeAccessIssue]
                session.add(mi_obj)
                session.commit()
        except Exception:
            session.rollback()
        return local_path
    except Exception:
        return None


def _persist_metadata(session, media_item: MediaItem, normalized: dict):
    from .translations import upsert_translation, normalize_translation_language  # local import to avoid import cycles

    def pick_title_en(norm: dict) -> str | None:
        try:
            prov = (norm.get('provider') or '').strip().lower()
        except Exception:
            prov = ''
        try:
            if prov == 'jikan':
                te = norm.get('title_english') or None
                if isinstance(te, str) and te.strip():
                    return te.strip()
        except Exception:
            pass
        try:
            t = norm.get('title')
            if isinstance(t, str) and t.strip():
                return t.strip()
        except Exception:
            pass
        return None

    def should_localize() -> bool:
        try:
            if not bool(get('tmdb_localization')):
                return False
        except Exception:
            return False
        try:
            lang = str(get('metadata_language') or '').strip().lower()
        except Exception:
            lang = ''
        # If metadata_language is English (or unset), no need to store a localized track.
        return bool(lang and not lang.startswith('en'))

    def get_localization_language() -> str | None:
        try:
            lang = str(get('metadata_language') or '').strip()
            return lang or None
        except Exception:
            return None

    def get_series_and_season_for_item(mi_obj: MediaItem):
        try:
            si = session.query(SeasonItem).filter(SeasonItem.media_item_id == mi_obj.id).first()
        except Exception:
            si = None
        if not si:
            return None, None
        try:
            season = session.query(Season).filter(Season.id == si.season_id).first()
        except Exception:
            season = None
        if not season:
            return None, None
        try:
            series = session.query(Series).filter(Series.id == season.series_id).first()
        except Exception:
            series = None
        return series, season

    def sync_series_season_fields(mi_obj: MediaItem):
        series, season = get_series_and_season_for_item(mi_obj)
        if not series:
            return
        # Keep `title` as best-display for back-compat; store anchor/localized in dedicated columns.
        #
        # Important: avoid overwriting series titles with season-specific titles ("Show - Season 2").
        # We only promote titles from Season 1 (or when series fields are empty).
        season_num = None
        try:
            season_num = getattr(season, "season_number", None) if season else None
        except Exception:
            season_num = None

        allow_promote_series_title = bool(season_num in (None, 1))

        try:
            if getattr(mi_obj, 'title_en', None) and (allow_promote_series_title or not getattr(series, "title_en", None)):
                series.title_en = mi_obj.title_en
        except Exception:
            pass
        try:
            if getattr(mi_obj, 'title_localized', None) and (allow_promote_series_title or not getattr(series, "title_localized", None)):
                series.title_localized = mi_obj.title_localized
        except Exception:
            pass
        try:
            # `title` should always be non-empty. Only overwrite it for Season 1 (or if it's empty).
            if allow_promote_series_title or not getattr(series, "title", None):
                series.title = (mi_obj.title_localized or mi_obj.title_en or mi_obj.title or series.title).strip() if isinstance((mi_obj.title_localized or mi_obj.title_en or mi_obj.title or series.title), str) else series.title
        except Exception:
            pass
        try:
            if getattr(mi_obj, 'mal_id', None):
                series.mal_id = str(mi_obj.mal_id)
        except Exception:
            pass
        try:
            if getattr(mi_obj, 'tmdb_id', None):
                series.tmdb_id = str(mi_obj.tmdb_id)
        except Exception:
            pass
        try:
            if getattr(mi_obj, 'release_year', None):
                series.year = int(mi_obj.release_year)  # type: ignore[arg-type]
        except Exception:
            pass
        try:
            if getattr(mi_obj, 'poster_path', None):
                series.main_poster = str(mi_obj.poster_path)
        except Exception:
            pass

        if season:
            try:
                if getattr(mi_obj, 'title_en', None):
                    season.title_en = mi_obj.title_en
            except Exception:
                pass
            try:
                if getattr(mi_obj, 'title_localized', None):
                    season.title_localized = mi_obj.title_localized
            except Exception:
                pass

        try:
            session.add(series)
            if season:
                session.add(season)
        except Exception:
            pass

    def sync_episode_localized_titles(mi_obj: MediaItem, tmdb_id: str, media_type: str | None, language: str, season_number: int | None):
        if not tmdb or not hasattr(tmdb, 'tmdb_fetch_season'):
            return
        if not season_number:
            return
        if (media_type or '').strip().lower() not in ('series', 'tv', 'show'):
            return
        series, season = get_series_and_season_for_item(mi_obj)
        if not season:
            return
        try:
            season_data = tmdb.tmdb_fetch_season(tmdb_id, season_number=season_number, language=language)  # type: ignore[arg-type]
        except Exception:
            season_data = None
        if not season_data or not isinstance(season_data, dict):
            return
        eps = season_data.get('episodes') or []
        if not isinstance(eps, list) or not eps:
            return
        try:
            existing_eps = session.query(Episode).filter(Episode.season_id == season.id).all()
        except Exception:
            existing_eps = []
        by_num: dict[int, Episode] = {}
        for ep in existing_eps:
            try:
                n = getattr(ep, 'episode_number', None)
                if isinstance(n, int):
                    by_num[n] = ep
            except Exception:
                continue

        def _is_generic_episode_title(v: str | None) -> bool:
            if v is None:
                return True
            s = str(v).strip()
            if not s:
                return True
            if re.fullmatch(r"\d{1,4}", s):
                return True
            if s.lower().startswith("episode ") or s.lower().startswith("episodio "):
                return True
            return False

        changed = False
        for e in eps:
            if not isinstance(e, dict):
                continue
            n = e.get('episode_number')
            if not isinstance(n, int):
                try:
                    n = int(n)
                except Exception:
                    continue
            row = by_num.get(n)
            if not row:
                continue
            try:
                tloc = e.get('title') or e.get('name')
                if isinstance(tloc, str) and tloc.strip():
                    row.title_localized = tloc.strip()
                    changed = True
            except Exception:
                pass
            try:
                ov = e.get('overview') or ''
                if isinstance(ov, str) and ov.strip():
                    row.synopsis_localized = ov.strip()
                    changed = True
            except Exception:
                pass
            try:
                session.add(row)
            except Exception:
                pass

        # Also backfill stable English anchor titles for episodes when current title_en is generic.
        # This improves UX for libraries where filenames are purely numeric (01.mkv) and prevents
        # regressions when localized fetch is temporarily unavailable.
        try:
            need_en = any(_is_generic_episode_title(getattr(r, "title_en", None)) for r in by_num.values())
        except Exception:
            need_en = False
        if need_en:
            try:
                season_data_en = tmdb.tmdb_fetch_season(tmdb_id, season_number=season_number, language="en-US")  # type: ignore[arg-type]
            except Exception:
                season_data_en = None
            if isinstance(season_data_en, dict):
                en_eps = season_data_en.get("episodes") or []
                if isinstance(en_eps, list):
                    for e in en_eps:
                        if not isinstance(e, dict):
                            continue
                        n = e.get("episode_number")
                        if not isinstance(n, int):
                            try:
                                n = int(n)
                            except Exception:
                                continue
                        row = by_num.get(n)
                        if not row:
                            continue
                        try:
                            ten = e.get("title") or e.get("name")
                            if isinstance(ten, str) and ten.strip() and _is_generic_episode_title(getattr(row, "title_en", None)):
                                row.title_en = ten.strip()
                                changed = True
                                session.add(row)
                        except Exception:
                            continue
        if changed:
            try:
                session.flush()
            except Exception:
                pass

    def maybe_apply_tmdb_localization(mi_obj: MediaItem, norm: dict, media_type: str | None):
        if not should_localize():
            return
        if not tmdb:
            return
        try:
            if not _tmdb_configured():
                return
        except Exception:
            return
        lang = get_localization_language()
        if not lang:
            return
        title_en = getattr(mi_obj, 'title_en', None) or pick_title_en(norm) or getattr(mi_obj, 'title', None) or ''
        year = getattr(mi_obj, 'release_year', None) or norm.get('year') or None
        try:
            year = int(year) if year is not None else None
        except Exception:
            year = None

        tmdb_id = getattr(mi_obj, 'tmdb_id', None)
        tmdb_media_type = 'tv' if (media_type or '').strip().lower() in ('series', 'tv', 'show') else 'movie'

        # If we don't have tmdb_id yet (common for anime identity via Jikan), try to resolve it from TMDB.
        if not tmdb_id:
            try:
                q = f"{title_en} {year}" if year else str(title_en)
                res = _tmdb_search_with_optional_season(q, tmdb_media_type)
                if res and isinstance(res, dict):
                    tmdb_id = res.get('tmdb_id') or res.get('provider_id')
                    if tmdb_id:
                        mi_obj.tmdb_id = str(tmdb_id)
            except Exception:
                tmdb_id = None

        if not tmdb_id:
            return

        # Fetch localized details for title/overview.
        #
        # For TV libraries with normalized seasons, prefer season-level localized titles/overview for
        # season folders (Season 2, etc.). Otherwise we end up storing the show overview everywhere,
        # and season titles can be wrong (or override series title).
        season_number = None
        try:
            _series0, _season0 = get_series_and_season_for_item(mi_obj)
            season_number = getattr(_season0, "season_number", None) if _season0 else None
        except Exception:
            season_number = None

        if tmdb_media_type == "tv" and isinstance(season_number, int) and season_number > 1 and hasattr(tmdb, "tmdb_fetch_season"):
            # Season-specific localized fields
            try:
                sdata = tmdb.tmdb_fetch_season(str(tmdb_id), season_number=season_number, language=lang)  # type: ignore[arg-type]
            except Exception:
                sdata = None
            if isinstance(sdata, dict):
                try:
                    tloc = sdata.get("title")
                    if isinstance(tloc, str) and tloc.strip():
                        mi_obj.title_localized = tloc.strip()
                except Exception:
                    pass
                try:
                    ov = sdata.get("synopsis") or ""
                    if isinstance(ov, str) and ov.strip():
                        mi_obj.synopsis_localized = ov.strip()
                except Exception:
                    pass
            # Persist TMDB translation rows (requested language + English fallback) for series/movie-level UI.
            try:
                tloc0 = getattr(mi_obj, "title_localized", None) or None
                ov0 = getattr(mi_obj, "synopsis_localized", None) or None
                upsert_translation(session, path_id=int(mi_obj.id), language=lang, source="tmdb", title=tloc0, overview=ov0)
            except Exception:
                pass
            try:
                # English fallback row (do not overwrite localized fields; UI will fallback by chain).
                sdata_en = tmdb.tmdb_fetch_season(str(tmdb_id), season_number=season_number, language="en-US")  # type: ignore[arg-type]
                if isinstance(sdata_en, dict):
                    ten = sdata_en.get("title") or None
                    ov_en = sdata_en.get("synopsis") or ""
                    upsert_translation(
                        session,
                        path_id=int(mi_obj.id),
                        language="en",
                        source="tmdb",
                        title=ten if isinstance(ten, str) and ten.strip() else None,
                        overview=ov_en if isinstance(ov_en, str) and ov_en.strip() else None,
                    )
            except Exception:
                pass
            # Season anchor English title (helps when UI falls back)
            try:
                if not getattr(mi_obj, "title_en", None):
                    sdata_en = tmdb.tmdb_fetch_season(str(tmdb_id), season_number=season_number, language="en-US")  # type: ignore[arg-type]
                    if isinstance(sdata_en, dict):
                        ten = sdata_en.get("title")
                        if isinstance(ten, str) and ten.strip():
                            mi_obj.title_en = ten.strip()
            except Exception:
                pass
        else:
            # Series/movie-level localized fields
            try:
                details = tmdb.tmdb_fetch_by_id(tmdb_id, media_type=tmdb_media_type, language=lang)  # type: ignore[arg-type]
            except Exception:
                details = None
            if isinstance(details, dict):
                try:
                    tloc = details.get('title') or details.get('series_title')
                    if isinstance(tloc, str) and tloc.strip():
                        mi_obj.title_localized = tloc.strip()
                except Exception:
                    pass
                try:
                    ov = details.get('synopsis') or ''
                    if isinstance(ov, str) and ov.strip():
                        mi_obj.synopsis_localized = ov.strip()
                except Exception:
                    pass
                # Persist TMDB translations (requested language + English fallback).
                try:
                    tloc0 = getattr(mi_obj, "title_localized", None) or None
                    ov0 = getattr(mi_obj, "synopsis_localized", None) or None
                    upsert_translation(session, path_id=int(mi_obj.id), language=lang, source="tmdb", title=tloc0, overview=ov0)
                except Exception:
                    pass
                try:
                    details_en = tmdb.tmdb_fetch_by_id(tmdb_id, media_type=tmdb_media_type, language="en-US")  # type: ignore[arg-type]
                except Exception:
                    details_en = None
                if isinstance(details_en, dict):
                    try:
                        ten = details_en.get("title") or details_en.get("series_title") or None
                        ov_en = details_en.get("synopsis") or ""
                        upsert_translation(
                            session,
                            path_id=int(mi_obj.id),
                            language="en",
                            source="tmdb",
                            title=ten if isinstance(ten, str) and ten.strip() else None,
                            overview=ov_en if isinstance(ov_en, str) and ov_en.strip() else None,
                        )
                    except Exception:
                        pass
                try:
                    import difflib
                    tloc0 = (details.get('title') or details.get('series_title') or '').strip()
                    if title_en and tloc0:
                        conf = difflib.SequenceMatcher(None, str(title_en).lower(), str(tloc0).lower()).ratio()
                        logger.info(
                            "tmdb_localization: media_item=%s tmdb_id=%s lang=%s confidence=%.2f",
                            getattr(mi_obj, "id", None),
                            str(tmdb_id),
                            lang,
                            float(conf),
                        )
                except Exception:
                    pass

        # Episode titles localization (only for series + known season number).
        try:
            series, season = get_series_and_season_for_item(mi_obj)
            season_number = getattr(season, 'season_number', None) if season else None
        except Exception:
            season_number = None
        try:
            sync_episode_localized_titles(mi_obj, str(tmdb_id), media_type, lang, season_number if isinstance(season_number, int) else None)
        except Exception:
            pass

        try:
            # Back-compat: keep `title` as best display.
            mi_obj.title = mi_obj.title_localized or mi_obj.title_en or mi_obj.title
        except Exception:
            pass

        try:
            session.add(mi_obj)
        except Exception:
            pass

    data_json = json.dumps(normalized or {}, ensure_ascii=False, sort_keys=True)
    checksum = hashlib.sha256(data_json.encode('utf-8')).hexdigest()
    mi_obj = _cast(_Any, media_item)
    existing = session.query(MediaMetadata).filter(MediaMetadata.media_item_id == mi_obj.id, MediaMetadata.checksum == checksum).first()
    if existing:
        title_en = pick_title_en(normalized)
        if title_en:
            try:
                mi_obj.title_en = title_en  # pyright: ignore[reportAttributeAccessIssue]
            except Exception:
                pass
        if normalized.get('title'):
            # Back-compat display title: prefer localized, else English anchor, else provider title.
            try:
                mi_obj.title = getattr(mi_obj, 'title_localized', None) or getattr(mi_obj, 'title_en', None) or normalized.get('title')
            except Exception:
                mi_obj.title = normalized.get('title')
        if normalized.get('media_type'):
            mi_obj.media_type = normalized.get('media_type')
        if normalized.get('provider'):
            mi_obj.provider = normalized.get('provider')
        if normalized.get('provider_id'):
            mi_obj.provider_id = str(normalized.get('provider_id'))
        mi_obj.status = 'ENRICHED'
        try:
            mi_obj.is_identified = True
        except Exception:
            pass
        session.add(mi_obj)
        session.commit()
        # Persist translations for UI fallback (prefer TMDB over Jikan at query time).
        try:
            prov0 = str(normalized.get("provider") or "").strip().lower() or "manual"
            lang0 = "en" if prov0 == "jikan" else normalize_translation_language(get('metadata_language') or "en")
            title0 = getattr(mi_obj, "title_en", None) or getattr(mi_obj, "title_localized", None) or getattr(mi_obj, "title", None)
            overview0 = normalized.get("synopsis") or normalized.get("overview") or None
            upsert_translation(session, path_id=int(mi_obj.id), language=lang0, source=prov0, title=title0, overview=overview0)
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
        try:
            maybe_apply_tmdb_localization(mi_obj, normalized, mi_obj.media_type)
            sync_series_season_fields(mi_obj)
            session.commit()
        except Exception:
            session.rollback()
        logger.info('Skipping identical metadata for media_item=%s provider=%s', media_item.id, normalized.get('provider'))
        return
    maxv = session.query(func.max(MediaMetadata.version)).filter(MediaMetadata.media_item_id == media_item.id).scalar() or 0
    next_version = int(maxv or 0) + 1
    mm = MediaMetadata(
        media_item_id=media_item.id,
        provider=normalized.get('provider'),
        provider_id=str(normalized.get('provider_id') or ''),
        data=data_json,
        version=next_version,
        checksum=checksum,
    )
    session.add(mm)
    title_en = pick_title_en(normalized)
    if title_en:
        try:
            mi_obj.title_en = title_en  # pyright: ignore[reportAttributeAccessIssue]
        except Exception:
            pass
    if normalized.get('title'):
        try:
            mi_obj.title = getattr(mi_obj, 'title_localized', None) or getattr(mi_obj, 'title_en', None) or normalized.get('title')
        except Exception:
            mi_obj.title = normalized.get('title')
    if normalized.get('media_type'):
        mi_obj.media_type = normalized.get('media_type')
    if normalized.get('provider'):
        mi_obj.provider = normalized.get('provider')
    if normalized.get('provider_id'):
        mi_obj.provider_id = str(normalized.get('provider_id'))
    raw = normalized.get('raw') or {}
    try:
        rd = None
        if isinstance(raw, dict):
            rd = raw.get('release_date') or raw.get('first_air_date') or raw.get('aired')
        if rd and isinstance(rd, str) and len(rd) >= 4:
            try:
                mi_obj.release_year = int(str(rd)[:4])
            except Exception:
                pass
    except Exception:
        pass
    try:
        rt = None
        if isinstance(raw, dict):
            rt = raw.get('runtime') or (raw.get('episode_run_time')[0] if isinstance(raw.get('episode_run_time'), list) and raw.get('episode_run_time') else None) # pyright: ignore[reportOptionalSubscript] # pyright: ignore[reportOptionalSubscript]
        if rt:
            try:
                mi_obj.runtime = int(rt)
            except Exception:
                pass
    except Exception:
        pass
    try:
        cast_arr = None
        if isinstance(raw, dict):
            cast_arr = (raw.get('credits') or {}).get('cast') if isinstance(raw.get('credits'), dict) else raw.get('cast')
        if cast_arr and isinstance(cast_arr, list):
            names = []
            for c in cast_arr[:10]:
                if isinstance(c, dict):
                    if c.get('name'):
                        names.append(c.get('name'))
                    elif c.get('person') and isinstance(c.get('person'), dict) and c.get('person').get('name'): # pyright: ignore[reportOptionalMemberAccess]
                        names.append(c.get('person').get('name')) # pyright: ignore[reportOptionalMemberAccess]
                elif isinstance(c, str):
                    names.append(c)
            if names:
                mi_obj.cast = ', '.join(names)
    except Exception:
        pass
    try:
        prov = (normalized.get('provider') or '').lower()
        genres = normalized.get('genres') or []
        is_anim = False
        origin = None
        if prov == 'jikan':
            is_anim = True
            raw_source = (raw.get('source') or '').lower() if isinstance(raw.get('source'), str) else ''
            if raw_source:
                origin = raw_source
        if any('animation' in (g or '').lower() for g in genres):
            is_anim = True
        mi_obj.is_animated = True if is_anim else (mi_obj.is_animated or False)
        if origin:
            mi_obj.origin = origin
    except Exception:
        pass
    try:
        prov = (normalized.get('provider') or '').lower()
        if prov == 'jikan':
            try:
                mal = normalized.get('provider_id')
                if mal:
                    mi_obj.mal_id = str(mal)
            except Exception:
                pass
        if prov == 'tmdb':
            try:
                tmdb_id = normalized.get('tmdb_id') or normalized.get('provider_id')
                if tmdb_id:
                    mi_obj.tmdb_id = str(tmdb_id)
            except Exception:
                pass
            try:
                raw_local = normalized.get('raw') or {}
                backdrop = raw_local.get('backdrop_path') or raw_local.get('backdrop') or ''
                if backdrop:
                    mi_obj.backdrop_path = backdrop
            except Exception:
                pass
            try:
                vote = None
                raw_local = normalized.get('raw') or {}
                vote = raw_local.get('vote_average') or raw_local.get('rating') or raw_local.get('vote')
                if vote is not None:
                    try:
                        mi_obj.rating = float(vote)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                g = normalized.get('genres') or []
                if isinstance(g, list) and g:
                    mi_obj.genres = ', '.join([str(x) for x in g if x])
            except Exception:
                pass
    except Exception:
        pass
    mi_obj.status = 'ENRICHED'
    try:
        mi_obj.is_identified = True
    except Exception:
        pass
    session.add(mi_obj)
    try:
        if get('download_images'):
            img_url = None
            try:
                img_url = (normalized.get('images') or {}).get('jpg', {}).get('large_image_url')
            except Exception:
                img_url = None
            if img_url:
                _download_image_and_record(session, mi_obj, img_url, source='provider')
    except Exception:
        pass
    try:
        maybe_apply_tmdb_localization(mi_obj, normalized, mi_obj.media_type)
        sync_series_season_fields(mi_obj)
    except Exception:
        logger.debug("Failed applying TMDB localization/sync for media_item=%s", mi_obj.id, exc_info=True)
        try:
            session.rollback()
        except Exception:
            pass
    session.commit()
    # Persist base translation row for the provider that produced these normalized fields.
    try:
        prov0 = str(normalized.get("provider") or "").strip().lower() or "manual"
        lang0 = "en" if prov0 == "jikan" else normalize_translation_language(get('metadata_language') or "en")
        title0 = getattr(mi_obj, "title_en", None) or getattr(mi_obj, "title_localized", None) or getattr(mi_obj, "title", None)
        overview0 = normalized.get("synopsis") or normalized.get("overview") or None
        upsert_translation(session, path_id=int(mi_obj.id), language=lang0, source=prov0, title=title0, overview=overview0)
        session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
    logger.info('Persisted metadata for media_item=%s provider=%s id=%s version=%s', mi_obj.id, normalized.get('provider'), normalized.get('provider_id'), next_version)

    try:
        _persist_relations(session, media_item, normalized)
    except Exception:
        logger.debug("Failed persisting relations for media_item=%s", media_item.id, exc_info=True)


def _persist_relations(session, media_item: MediaItem, normalized: dict) -> None:
    try:
        prov = (normalized.get('provider') or '').lower()
    except Exception:
        prov = ''
    if prov != 'jikan':
        return
    rels = normalized.get('relations') or []
    if not isinstance(rels, list):
        return

    mi_obj = _cast(_Any, media_item)
    cur_mal = None
    try:
        cur_mal = str(getattr(mi_obj, 'mal_id', None) or normalized.get('provider_id') or '').strip()
    except Exception:
        cur_mal = None

    for rel in rels:
        if not isinstance(rel, dict):
            continue
        rel_name = str(rel.get('relation') or '').strip().lower()
        if rel_name not in ('prequel', 'sequel'):
            continue
        entries = rel.get('entry') or []
        if not isinstance(entries, list):
            continue
        for e in entries:
            if not isinstance(e, dict):
                continue
            mid = e.get('mal_id') or e.get('id')
            if not mid:
                continue
            try:
                target_mal = str(int(mid))
            except Exception:
                target_mal = str(mid).strip()
            if not target_mal:
                continue

            target_item = None
            try:
                target_item = (
                    session.query(MediaItem)
                    .filter(MediaItem.mal_id == target_mal)
                    .order_by(MediaItem.id.desc())
                    .first()
                )
            except Exception:
                target_item = None

            existing = None
            try:
                existing = (
                    session.query(MediaRelation)
                    .filter(
                        MediaRelation.from_item_id == mi_obj.id,
                        MediaRelation.relation_type == rel_name,
                        MediaRelation.external_id == target_mal,
                    )
                    .first()
                )
            except Exception:
                existing = None

            if existing:
                if target_item and existing.to_item_id is None:
                    existing.to_item_id = target_item.id
                    session.add(existing)
                    session.commit()
                continue

            mr = MediaRelation(
                from_item_id=mi_obj.id,
                to_item_id=target_item.id if target_item else None,
                relation_type=rel_name,
                provider='jikan',
                external_id=target_mal,
            )
            session.add(mr)
            session.commit()

    if cur_mal:
        try:
            pending = (
                session.query(MediaRelation)
                .filter(MediaRelation.external_id == cur_mal, MediaRelation.to_item_id == None)
                .all()
            )
            for rel in pending:
                rel.to_item_id = mi_obj.id
                session.add(rel)
            if pending:
                session.commit()
        except Exception:
            session.rollback()


def enrich_pending(limit: Optional[int] = None) -> int:
    processed = 0
    session = SessionLocal()
    try:
        subq = session.query(MediaMetadata.media_item_id).filter(MediaMetadata.provider != None).filter(MediaMetadata.provider != '')
        retry_no_match = str(os.environ.get("ARCANEA_ENRICH_RETRY_NO_MATCH", "")).strip().lower() in ("1", "true", "yes", "on")
        if retry_no_match:
            q = session.query(MediaItem).filter(~MediaItem.id.in_(subq)).filter(
                (MediaItem.status == None)
                | (
                    (MediaItem.status != 'ERROR')
                    & (MediaItem.status != 'MANUAL')
                    & (MediaItem.status != 'OMITTED')
                )
            )
        else:
            q = session.query(MediaItem).filter(~MediaItem.id.in_(subq)).filter(
                (MediaItem.status == None)
                | (
                    (MediaItem.status != 'ERROR')
                    & (MediaItem.status != 'NO_MATCH')
                    & (MediaItem.status != 'MANUAL')
                    & (MediaItem.status != 'OMITTED')
                )
            )
        if limit:
            q = q.limit(limit)
        candidates = q.all()
        cfg_movies = get('metadata_movies_provider') or 'tmdb'
        cfg_anime = get('metadata_anime_provider') or 'jikan'

        for mi in candidates:
            mi = _cast(_Any, mi)
            try:
                try:
                    set_state({'running': True, 'current_id': mi.id, 'current_title': mi.title, 'current_step': 'enrich_pending'})
                except Exception:
                    pass
                inferred = _decide_media_type(mi, session) or 'movie'
                cur_mt = (mi.media_type or '').strip().lower()
                # If a folder has multiple files, treat it as a series even if an older scan marked it as movie.
                # (Avoids TMDB movie-first mis-matches like "Halo Temporada 2" -> "Halo Wars".)
                if inferred == 'series' and cur_mt not in ('series', 'tv', 'show'):
                    mi.media_type = 'series'
                else:
                    mi.media_type = mi.media_type or inferred
                media_type = mi.media_type or inferred
                title = mi.title or ''
                normalized = {}
                candidate_titles = _candidate_titles_for_item(mi)
                providers_order = _provider_order_for_item(media_type, getattr(mi, 'mal_id', None), cfg_movies, cfg_anime, getattr(mi, 'media_root', None))
                provider_error = False
                try:
                    tmdb_ok = _tmdb_configured()
                except Exception:
                    tmdb_ok = False
                logger.info("enrichment: media_item=%s media_type=%s providers_order=%s tmdb_configured=%s", getattr(mi, 'id', None), media_type, providers_order, tmdb_ok)

                # Strict hierarchy: if this looks like a season subfolder and parent already has TMDB identity,
                # skip TMDB search and fetch the season directly.
                try:
                    inherited = _inherit_tmdb_identity_from_parent_if_season(mi, session)
                except Exception:
                    inherited = None
                if inherited and tmdb is not None and hasattr(tmdb, "tmdb_fetch_season"):
                    parent_tmdb_id, season_number = inherited
                    try:
                        lang = str(get("metadata_language") or "").strip() or "en-US"
                    except Exception:
                        lang = "en-US"
                    try:
                        mi.tmdb_id = str(parent_tmdb_id)
                        mi.media_type = 'series'
                        session.add(mi)
                        session.commit()
                    except Exception:
                        session.rollback()
                    try:
                        normalized = tmdb.tmdb_fetch_season(str(parent_tmdb_id), season_number=season_number, language=lang)  # type: ignore[arg-type]
                    except Exception:
                        normalized = None
                    if isinstance(normalized, dict) and normalized:
                        try:
                            # Ensure season_number is present so i18n/episode attachment behave.
                            normalized.setdefault("season_number", int(season_number))
                        except Exception:
                            pass
                        try:
                            set_state({'current_step': 'persist_metadata'})
                        except Exception:
                            pass
                        try:
                            _attach_tmdb_i18n(normalized, 'series')
                            _attach_episodes('tmdb', normalized, 'series', mi)
                        except Exception:
                            pass
                        _persist_metadata(session, mi, normalized)
                        processed += 1
                        continue

                if providers_order == ['tmdb'] and not tmdb_ok:
                    logger.warning(
                        "enrichment: TMDB not configured; skipping enrichment for media_item=%s",
                        getattr(mi, "id", None),
                    )
                    try:
                        try:
                            mi.title = _fallback_title_for_item(mi)
                        except Exception:
                            pass
                        mi.status = "NO_MATCH"
                        try:
                            mi.is_identified = False
                        except Exception:
                            pass
                        session.add(mi)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue
                try:
                    if 'tmdb' in providers_order and not tmdb_ok:
                        logger.info('enrichment: removing TMDB from providers_order for media_item=%s', getattr(mi, 'id', None))
                        providers_order = [p for p in providers_order if p != 'tmdb']
                except Exception:
                    pass
                if not providers_order:
                    logger.warning(
                        "enrichment: empty providers_order; skipping enrichment for media_item=%s",
                        getattr(mi, "id", None),
                    )
                    try:
                        try:
                            mi.title = _fallback_title_for_item(mi)
                        except Exception:
                            pass
                        mi.status = "NO_MATCH"
                        try:
                            mi.is_identified = False
                        except Exception:
                            pass
                        session.add(mi)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                for p in providers_order:
                    try:
                        res = None
                        if p == 'jikan' and jikan is not None:
                            if getattr(mi, 'mal_id', None) and hasattr(jikan, 'fetch_by_id'):
                                try:
                                    logger.info('enrichment: calling jikan.fetch_by_id mal_id=%s media_item=%s', getattr(mi, 'mal_id', None), getattr(mi, 'id', None))
                                    res = jikan.fetch_by_id(str(mi.mal_id))
                                    logger.info('enrichment: jikan.fetch_by_id returned for media_item=%s result=%s', getattr(mi, 'id', None), 'ok' if res else 'none')
                                except Exception:
                                    logger.exception('enrichment: jikan.fetch_by_id failed for media_item=%s', getattr(mi, 'id', None))
                                    provider_error = True
                                    res = None
                            if not res:
                                # provider_jikan already expands name variants; do a single request per item.
                                for cand in (candidate_titles[:1] if isinstance(candidate_titles, list) else []):
                                    try:
                                        logger.info('enrichment: jikan.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                        r = jikan.search(cand) if hasattr(jikan, 'search') else None
                                        logger.info('enrichment: jikan.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                                    except Exception:
                                        logger.warning('enrichment: jikan.search failed for query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                        provider_error = True
                                        r = None
                                        break
                                    if isinstance(r, list) and r:
                                        res = r[0]
                                        break
                                    if r and not isinstance(r, list):
                                        res = r
                        elif p == 'tmdb' and tmdb is not None:
                            try:
                                for cand in candidate_titles:
                                    try:
                                        logger.info('enrichment: tmdb.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                        r = _tmdb_search_with_optional_season(cand, media_type)
                                        logger.info('enrichment: tmdb.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                                    except Exception:
                                        logger.exception('enrichment: tmdb.search failed for query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                        provider_error = True
                                        r = None
                                    if isinstance(r, list) and r:
                                        res = r[0]
                                        break
                                    if r and not isinstance(r, list):
                                        res = r
                                        break
                            except Exception:
                                # protect outer try from unexpected provider errors
                                logger.exception('enrichment: unexpected tmdb provider error for media_item=%s', getattr(mi, 'id', None))
                                provider_error = True
                                pass

                        if not res:
                            continue
                        normalized = _normalize_result(p, res)
                        if normalized:
                            break
                    except Exception:
                        logger.debug('Provider %s failed for title=%s', p, title, exc_info=True)

                if not normalized:
                    # Bridge: if Jikan couldn't match and TMDB is configured, try a single TMDB lookup as TV.
                    # This is useful for folders with noisy suffixes (Part/Cour/Especiales) or misclassified libraries.
                    try:
                        if tmdb_ok and ('tmdb' not in providers_order) and ('jikan' in providers_order or providers_order == ['jikan']):
                            is_series = (media_type or '').strip().lower() in ('series', 'tv', 'anime')
                            if not is_series:
                                try:
                                    is_series = session.query(FileRecord).filter(FileRecord.media_item_id == mi.id).count() > 1
                                except Exception:
                                    is_series = False
                            if is_series:
                                cand0 = candidate_titles[0] if candidate_titles else (mi.title or '')
                                cand0 = str(cand0 or '').strip()
                                if cand0:
                                    logger.info('enrichment: bridge jikan->tmdb (tv) query="%s" media_item=%s', cand0, getattr(mi, 'id', None))
                                    r = _tmdb_search_with_optional_season(cand0, 'tv')
                                    if isinstance(r, list) and r:
                                        r = r[0]
                                    if r and isinstance(r, dict):
                                        normalized = _normalize_result('tmdb', r)
                    except Exception:
                        pass

                if not normalized:
                    try:
                        logger.info('enrichment: no-match for media_item=%s candidates=%s', getattr(mi, 'id', None), candidate_titles)
                    except Exception:
                        pass
                    try:
                        set_state({'current_step': 'provider_error' if provider_error else 'no_match'})
                    except Exception:
                        pass
                    # If provider calls failed (rate limit / network), keep it pending to retry later.
                    # If provider calls succeeded but no match was found, mark as NO_MATCH to avoid endless retries.
                    if not provider_error:
                        try:
                            try:
                                mi.title = _fallback_title_for_item(mi)
                            except Exception:
                                pass
                            mi.status = 'NO_MATCH'
                            try:
                                mi.is_identified = False
                            except Exception:
                                pass
                            session.add(mi)
                            session.commit()
                        except Exception:
                            session.rollback()
                    processed += 1 if not provider_error else 0
                    continue

                try:
                    suggested = classify_metadata(normalized)
                    if suggested:
                        mi.media_type = suggested
                except Exception:
                    pass
                try:
                    set_state({'current_step': 'persist_metadata'})
                except Exception:
                    pass
                try:
                    _attach_tmdb_i18n(normalized, media_type)
                    _attach_episodes(normalized.get('provider') or '', normalized, media_type, mi)
                except Exception:
                    pass
                _persist_metadata(session, mi, normalized)
                processed += 1
            except Exception:
                logger.exception('Failed enriching media_item %s', mi.id)
            finally:
                try:
                    clear_state()
                except Exception:
                    pass
        return processed
    finally:
        session.close()


def backfill_tmdb_episode_titles(
    limit_seasons: int | None = None,
    manage_state: bool = True,
) -> dict[str, int | str | bool]:
    """Backfill normalized `episode` rows with TMDB titles/synopsis.

    Why: scans rebuild `episode` rows from filesystem state; older DBs or earlier scan errors can
    leave `episode.title_localized` empty even when season-level localization exists.

    This is safe to run multiple times; it only fills missing/generic fields and commits per season.
    """
    if tmdb is None or not hasattr(tmdb, "tmdb_fetch_season"):
        return {"ok": False, "detail": "tmdb_not_available", "seasons_updated": 0, "episodes_updated": 0}
    try:
        if not _tmdb_configured():
            return {"ok": False, "detail": "tmdb_not_configured", "seasons_updated": 0, "episodes_updated": 0}
    except Exception:
        return {"ok": False, "detail": "tmdb_not_configured", "seasons_updated": 0, "episodes_updated": 0}

    try:
        lang = str(get("metadata_language") or "").strip() or "en-US"
    except Exception:
        lang = "en-US"

    try:
        localize = bool(get("tmdb_localization")) and bool(lang) and not str(lang).lower().startswith("en")
    except Exception:
        localize = False

    def is_generic_episode_title(v: str | None) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        if not s:
            return True
        if re.fullmatch(r"\d{1,4}", s):
            return True
        if s.lower().startswith("episode ") or s.lower().startswith("episodio "):
            return True
        return False

    seasons_updated = 0
    episodes_updated = 0

    session = SessionLocal()
    try:
        if manage_state:
            try:
                set_state(
                    {
                        "running": True,
                        "current_id": None,
                        "current_title": "TMDB: episodios",
                        "current_step": "backfill_episodes",
                        "backfill_lang": lang,
                        "backfill_localized": bool(localize),
                        "backfill_total": 0,
                        "backfill_done": 0,
                        "backfill_seasons_updated": 0,
                        "backfill_episodes_updated": 0,
                    }
                )
            except Exception:
                pass

        q = (
            session.query(SeasonItem, Season, MediaItem, Series)
            .join(Season, Season.id == SeasonItem.season_id)
            .join(MediaItem, MediaItem.id == SeasonItem.media_item_id)
            .join(Series, Series.id == Season.series_id, isouter=True)
            .filter(MediaItem.tmdb_id != None)  # noqa: E711
        )
        if limit_seasons and int(limit_seasons) > 0:
            q = q.limit(int(limit_seasons))
        rows = q.all()

        total = len(rows)
        try:
            set_state({"backfill_total": int(total), "backfill_done": 0})
        except Exception:
            pass

        done = 0

        for si, season, mi, series in rows:
            done += 1
            try:
                season_number = getattr(season, "season_number", None)
                if not isinstance(season_number, int) or season_number <= 0:
                    continue

                tmdb_id = getattr(mi, "tmdb_id", None) or (getattr(series, "tmdb_id", None) if series else None)
                if not tmdb_id:
                    continue
                tmdb_id_s = str(tmdb_id)

                eps = session.query(Episode).filter(Episode.season_id == season.id).all()
                if not eps:
                    continue

                need_loc = False
                if localize:
                    try:
                        need_loc = any(not (getattr(ep, "title_localized", None) or "").strip() for ep in eps)
                    except Exception:
                        need_loc = True

                need_en = False
                try:
                    need_en = any(is_generic_episode_title(getattr(ep, "title_en", None)) for ep in eps)
                except Exception:
                    need_en = False

                need_syn = False
                try:
                    need_syn = any(not (getattr(ep, "synopsis_localized", None) or "").strip() for ep in eps)
                except Exception:
                    need_syn = False

                if not need_loc and not need_en and not need_syn:
                    continue

                try:
                    # keep the UI responsive with a human-readable step
                    set_state(
                        {
                            "running": True,
                            "current_id": getattr(mi, "id", None),
                            "current_title": (getattr(mi, "title_localized", None) or getattr(mi, "title_en", None) or getattr(mi, "title", None) or "TMDB"),
                            "current_step": f"backfill_episodes {done}/{total}",
                            "backfill_done": int(done),
                            "backfill_seasons_updated": int(seasons_updated),
                            "backfill_episodes_updated": int(episodes_updated),
                        }
                    )
                except Exception:
                    pass

                season_data_loc = None
                if need_loc:
                    try:
                        season_data_loc = tmdb.tmdb_fetch_season(tmdb_id_s, season_number=season_number, language=lang)  # type: ignore[arg-type]
                    except Exception:
                        season_data_loc = None

                season_data_en = None
                if need_en:
                    try:
                        season_data_en = tmdb.tmdb_fetch_season(tmdb_id_s, season_number=season_number, language="en-US")  # type: ignore[arg-type]
                    except Exception:
                        season_data_en = None

                # Global synopsis fallback (series-level) when episode overviews are missing.
                global_synopsis = None
                if need_syn:
                    try:
                        details_loc = tmdb.tmdb_fetch_by_id(tmdb_id_s, media_type="tv", language=lang)  # type: ignore[arg-type]
                    except Exception:
                        details_loc = None
                    if isinstance(details_loc, dict):
                        try:
                            gs = details_loc.get("synopsis") or details_loc.get("overview") or ""
                            if isinstance(gs, str) and gs.strip():
                                global_synopsis = gs.strip()
                        except Exception:
                            global_synopsis = None
                    # Fallback to TMDB English (not Jikan) if localized synopsis is missing.
                    if not global_synopsis:
                        try:
                            details_en = tmdb.tmdb_fetch_by_id(tmdb_id_s, media_type="tv", language="en-US")  # type: ignore[arg-type]
                        except Exception:
                            details_en = None
                        if isinstance(details_en, dict):
                            try:
                                gs = details_en.get("synopsis") or details_en.get("overview") or ""
                                if isinstance(gs, str) and gs.strip():
                                    global_synopsis = gs.strip()
                            except Exception:
                                global_synopsis = None
                    if not global_synopsis and isinstance(season_data_loc, dict):
                        try:
                            gs = season_data_loc.get("synopsis") or season_data_loc.get("overview") or ""
                            if isinstance(gs, str) and gs.strip():
                                global_synopsis = gs.strip()
                        except Exception:
                            global_synopsis = None

                by_num = {}
                for ep in eps:
                    try:
                        n = getattr(ep, "episode_number", None)
                        if isinstance(n, int):
                            by_num[n] = ep
                    except Exception:
                        continue

                changed_this_season = False

                if isinstance(season_data_loc, dict):
                    loc_eps = season_data_loc.get("episodes") or []
                    if isinstance(loc_eps, list):
                        for e in loc_eps:
                            if not isinstance(e, dict):
                                continue
                            n = e.get("episode_number")
                            if not isinstance(n, int):
                                try:
                                    n = int(n)
                                except Exception:
                                    continue
                            row = by_num.get(n)
                            if not row:
                                continue
                            try:
                                tloc = e.get("title") or e.get("name")
                                if isinstance(tloc, str) and tloc.strip() and not (getattr(row, "title_localized", None) or "").strip():
                                    row.title_localized = tloc.strip()
                                    changed_this_season = True
                                    episodes_updated += 1
                            except Exception:
                                pass
                            try:
                                ov = e.get("overview") or ""
                                if not (getattr(row, "synopsis_localized", None) or "").strip():
                                    if isinstance(ov, str) and ov.strip():
                                        row.synopsis_localized = ov.strip()
                                        changed_this_season = True
                                    elif isinstance(global_synopsis, str) and global_synopsis.strip():
                                        # Requirement: when episode synopsis is missing, fall back to global series synopsis.
                                        row.synopsis_localized = global_synopsis.strip()
                                        changed_this_season = True
                            except Exception:
                                pass
                            session.add(row)

                if isinstance(season_data_en, dict):
                    en_eps = season_data_en.get("episodes") or []
                    if isinstance(en_eps, list):
                        for e in en_eps:
                            if not isinstance(e, dict):
                                continue
                            n = e.get("episode_number")
                            if not isinstance(n, int):
                                try:
                                    n = int(n)
                                except Exception:
                                    continue
                            row = by_num.get(n)
                            if not row:
                                continue
                            try:
                                ten = e.get("title") or e.get("name")
                                if isinstance(ten, str) and ten.strip() and is_generic_episode_title(getattr(row, "title_en", None)):
                                    row.title_en = ten.strip()
                                    # keep `title` usable if it was also generic
                                    if is_generic_episode_title(getattr(row, "title", None)):
                                        row.title = ten.strip()
                                    changed_this_season = True
                                    episodes_updated += 1
                                    session.add(row)
                            except Exception:
                                continue

                if changed_this_season:
                    try:
                        session.commit()
                    except Exception:
                        session.rollback()
                        continue
                    seasons_updated += 1
                    try:
                        set_state(
                            {
                                "backfill_seasons_updated": int(seasons_updated),
                                "backfill_episodes_updated": int(episodes_updated),
                            }
                        )
                    except Exception:
                        pass
            except Exception:
                session.rollback()
                continue

        res = {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": seasons_updated, "episodes_updated": episodes_updated}
        if manage_state:
            try:
                set_state(
                    {
                        "running": False,
                        "current_id": None,
                        "current_title": None,
                        "current_step": "idle",
                        "backfill_seasons_updated": int(seasons_updated),
                        "backfill_episodes_updated": int(episodes_updated),
                    }
                )
            except Exception:
                pass
        return res
    finally:
        session.close()


def tmdb_sync_from_jikan_title_en(
    limit_series: int | None = None,
    manage_state: bool = True,
    min_confidence: float = 0.55,
) -> dict[str, int | str | bool]:
    """Resolve and persist TMDB ids for Jikan-identified anime series using the English anchor title.

    This enables the existing TMDB episode backfill to populate:
      - episode.title_localized
      - episode.synopsis_localized

    Why: older libraries may have been enriched with Jikan only, leaving tmdb_id empty and making
    localized episode sync impossible without re-enrichment.
    """
    if tmdb is None or not hasattr(tmdb, "tmdb_search"):
        return {"ok": False, "detail": "tmdb_not_available", "series_updated": 0, "items_updated": 0}
    try:
        if not _tmdb_configured():
            return {"ok": False, "detail": "tmdb_not_configured", "series_updated": 0, "items_updated": 0}
    except Exception:
        return {"ok": False, "detail": "tmdb_not_configured", "series_updated": 0, "items_updated": 0}

    try:
        lang = str(get("metadata_language") or "").strip() or "en-US"
    except Exception:
        lang = "en-US"

    try:
        localize = bool(get("tmdb_localization")) and bool(lang) and not str(lang).lower().startswith("en")
    except Exception:
        localize = False

    if not localize:
        return {"ok": True, "detail": "localization_disabled", "lang": lang, "series_updated": 0, "items_updated": 0}

    session = SessionLocal()
    series_updated = 0
    items_updated = 0
    matched = 0
    skipped_low_conf = 0
    no_match = 0

    try:
        from sqlalchemy import or_
        # Only target anime series (identified via MAL/Jikan) that still don't have a TMDB identity.
        q = (
            session.query(Series)
            .filter(Series.mal_id != None)  # noqa: E711
            .filter(or_(Series.tmdb_id == None, Series.tmdb_id == ""))  # noqa: E711
            .filter(or_(Series.tmdb_no_match == None, Series.tmdb_no_match == False))  # noqa: E711
        )
        if limit_series and int(limit_series) > 0:
            q = q.limit(int(limit_series))
        series_rows = q.all()

        total = len(series_rows)

        if manage_state:
            try:
                set_state(
                    {
                        "running": True,
                        "current_id": None,
                        "current_title": "TMDB: sync",
                        "current_step": "tmdb_sync",
                        "sync_lang": lang,
                        "sync_total": int(total),
                        "sync_done": 0,
                        "sync_series_updated": 0,
                        "sync_items_updated": 0,
                        "sync_matched": 0,
                        "sync_no_match": 0,
                        "sync_skipped_low_conf": 0,
                    }
                )
            except Exception:
                pass

        import difflib

        for idx, series in enumerate(series_rows, 1):
            try:
                # Fetch all season items for this series (one media_item per season).
                season_items = (
                    session.query(SeasonItem, Season, MediaItem)
                    .join(Season, Season.id == SeasonItem.season_id)
                    .join(MediaItem, MediaItem.id == SeasonItem.media_item_id)
                    .filter(Season.series_id == series.id)
                    .all()
                )

                # Determine if we already have a stable tmdb_id from either series or any season item.
                desired_tmdb_id = (getattr(series, "tmdb_id", None) or "").strip() or None
                if not desired_tmdb_id:
                    ids = []
                    for _si, _season, mi in season_items:
                        tid = (getattr(mi, "tmdb_id", None) or "").strip()
                        if tid:
                            ids.append(tid)
                    ids = sorted(set(ids))
                    if len(ids) == 1:
                        desired_tmdb_id = ids[0]

                # If a tmdb_id is already known, just propagate it to season items.
                if desired_tmdb_id:
                    changed_any = False
                    for _si, _season, mi in season_items:
                        cur = (getattr(mi, "tmdb_id", None) or "").strip()
                        if cur != desired_tmdb_id:
                            mi.tmdb_id = str(desired_tmdb_id)
                            session.add(mi)
                            items_updated += 1
                            changed_any = True
                    if (getattr(series, "tmdb_id", None) or "").strip() != desired_tmdb_id:
                        series.tmdb_id = str(desired_tmdb_id)
                        session.add(series)
                        series_updated += 1
                        changed_any = True
                    if changed_any:
                        try:
                            session.commit()
                        except Exception:
                            session.rollback()
                    if manage_state:
                        try:
                            set_state(
                                {
                                    "sync_done": int(idx),
                                    "sync_series_updated": int(series_updated),
                                    "sync_items_updated": int(items_updated),
                                }
                            )
                        except Exception:
                            pass
                    continue

                # Resolve anchor title/year. Prefer the Jikan English anchor (`title_en`) from series or media_item.
                anchor_title = (getattr(series, "title_en", None) or "").strip()
                if not anchor_title:
                    try:
                        for _si, _season, mi in season_items:
                            t0 = (getattr(mi, "title_en", None) or "").strip()
                            if t0:
                                anchor_title = t0
                                break
                    except Exception:
                        pass
                if not anchor_title:
                    anchor_title = (getattr(series, "title", None) or "").strip()
                year = getattr(series, "year", None)
                if year is None:
                    try:
                        for _si, _season, mi in season_items:
                            y0 = getattr(mi, "release_year", None)
                            if isinstance(y0, int) and y0 > 0:
                                year = y0
                                break
                    except Exception:
                        year = None

                if not anchor_title:
                    no_match += 1
                    try:
                        series.tmdb_no_match = True
                        series.tmdb_no_match_reason = "missing_anchor"
                        session.add(series)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                if manage_state:
                    try:
                        set_state(
                            {
                                "running": True,
                                "current_id": getattr(series, "id", None),
                                "current_title": anchor_title,
                                "current_step": f"tmdb_sync {idx}/{total}",
                                "sync_done": int(idx),
                                "sync_series_updated": int(series_updated),
                                "sync_items_updated": int(items_updated),
                                "sync_matched": int(matched),
                                "sync_no_match": int(no_match),
                                "sync_skipped_low_conf": int(skipped_low_conf),
                            }
                        )
                    except Exception:
                        pass

                queries: list[str] = []
                if year:
                    queries.append(f"{anchor_title} {year}")
                queries.append(anchor_title)

                res = None
                try:
                    res = tmdb.tmdb_search(queries, media_preference="tv")  # type: ignore[attr-defined]
                except Exception:
                    res = None

                if not isinstance(res, dict):
                    no_match += 1
                    try:
                        series.tmdb_no_match = True
                        series.tmdb_no_match_reason = "no_match"
                        session.add(series)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                mt = (res.get("media_type") or "").strip().lower()
                if mt != "tv":
                    no_match += 1
                    try:
                        series.tmdb_no_match = True
                        series.tmdb_no_match_reason = f"not_tv:{mt or 'unknown'}"
                        session.add(series)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                tmdb_id = res.get("tmdb_id") or res.get("provider_id")
                if not tmdb_id:
                    no_match += 1
                    try:
                        series.tmdb_no_match = True
                        series.tmdb_no_match_reason = "missing_tmdb_id"
                        session.add(series)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                tmdb_title = (res.get("title") or res.get("series_title") or "").strip()
                conf = 0.0
                if anchor_title and tmdb_title:
                    try:
                        conf = difflib.SequenceMatcher(None, anchor_title.lower(), tmdb_title.lower()).ratio()
                    except Exception:
                        conf = 0.0

                if conf < float(min_confidence):
                    skipped_low_conf += 1
                    logger.warning(
                        "tmdb_sync: low confidence match series_id=%s title_en='%s' -> tmdb_id=%s tmdb_title='%s' conf=%.2f (skipping)",
                        getattr(series, "id", None),
                        anchor_title,
                        str(tmdb_id),
                        tmdb_title,
                        float(conf),
                    )
                    try:
                        series.tmdb_no_match = True
                        series.tmdb_no_match_reason = f"low_conf:{conf:.2f}"
                        session.add(series)
                        session.commit()
                    except Exception:
                        session.rollback()
                    continue

                logger.info(
                    "tmdb_sync: matched series_id=%s title_en='%s' -> tmdb_id=%s tmdb_title='%s' conf=%.2f",
                    getattr(series, "id", None),
                    anchor_title,
                    str(tmdb_id),
                    tmdb_title,
                    float(conf),
                )

                series.tmdb_id = str(tmdb_id)
                session.add(series)

                for _si, _season, mi in season_items:
                    mi.tmdb_id = str(tmdb_id)
                    session.add(mi)
                    items_updated += 1

                try:
                    session.commit()
                except Exception:
                    session.rollback()
                    continue

                series_updated += 1
                matched += 1

                if manage_state:
                    try:
                        set_state(
                            {
                                "sync_series_updated": int(series_updated),
                                "sync_items_updated": int(items_updated),
                                "sync_matched": int(matched),
                                "sync_no_match": int(no_match),
                                "sync_skipped_low_conf": int(skipped_low_conf),
                            }
                        )
                    except Exception:
                        pass
            except Exception:
                session.rollback()
                continue

        res_out: dict[str, int | str | bool] = {
            "ok": True,
            "lang": lang,
            "series_total": int(total),
            "series_updated": int(series_updated),
            "items_updated": int(items_updated),
            "matched": int(matched),
            "no_match": int(no_match),
            "skipped_low_conf": int(skipped_low_conf),
        }
        if manage_state:
            try:
                set_state(
                    {
                        "running": False,
                        "current_id": None,
                        "current_title": None,
                        "current_step": "idle",
                        "sync_series_updated": int(series_updated),
                        "sync_items_updated": int(items_updated),
                        "sync_matched": int(matched),
                        "sync_no_match": int(no_match),
                        "sync_skipped_low_conf": int(skipped_low_conf),
                    }
                )
            except Exception:
                pass
        return res_out
    finally:
        session.close()


def backfill_tmdb_episode_titles_for_media_item(
    media_item_id: int,
    manage_state: bool = True,
) -> dict[str, int | str | bool]:
    """Backfill episode titles/synopsis for a single media_item's season.

    Used for "lazy" fixes when the UI opens a modal/player and detects generic episode titles.
    """
    if tmdb is None or not hasattr(tmdb, "tmdb_fetch_season"):
        return {"ok": False, "detail": "tmdb_not_available", "seasons_updated": 0, "episodes_updated": 0}
    try:
        if not _tmdb_configured():
            return {"ok": False, "detail": "tmdb_not_configured", "seasons_updated": 0, "episodes_updated": 0}
    except Exception:
        return {"ok": False, "detail": "tmdb_not_configured", "seasons_updated": 0, "episodes_updated": 0}

    try:
        lang = str(get("metadata_language") or "").strip() or "en-US"
    except Exception:
        lang = "en-US"

    try:
        localize = bool(get("tmdb_localization")) and bool(lang) and not str(lang).lower().startswith("en")
    except Exception:
        localize = False

    def is_generic_episode_title(v: str | None) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        if not s:
            return True
        if re.fullmatch(r"\d{1,4}", s):
            return True
        if s.lower().startswith("episode ") or s.lower().startswith("episodio "):
            return True
        return False

    seasons_updated = 0
    episodes_updated = 0

    session = SessionLocal()
    try:
        si = session.query(SeasonItem).filter(SeasonItem.media_item_id == int(media_item_id)).first()
        if not si:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}
        season = session.query(Season).filter(Season.id == si.season_id).first()
        if not season:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}

        season_number = getattr(season, "season_number", None)
        if not isinstance(season_number, int) or season_number <= 0:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}

        mi = session.query(MediaItem).filter(MediaItem.id == int(media_item_id)).first()
        if not mi:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}

        series = None
        try:
            if getattr(season, "series_id", None) is not None:
                series = session.query(Series).filter(Series.id == season.series_id).first()
        except Exception:
            series = None

        tmdb_id = getattr(mi, "tmdb_id", None) or (getattr(series, "tmdb_id", None) if series else None)
        if not tmdb_id:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}
        tmdb_id_s = str(tmdb_id)

        eps = session.query(Episode).filter(Episode.season_id == season.id).all()
        if not eps:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}

        need_loc = False
        if localize:
            try:
                need_loc = any(not (getattr(ep, "title_localized", None) or "").strip() for ep in eps)
            except Exception:
                need_loc = True

        need_en = False
        try:
            need_en = any(is_generic_episode_title(getattr(ep, "title_en", None)) for ep in eps)
        except Exception:
            need_en = False

        need_syn = False
        try:
            need_syn = any(not (getattr(ep, "synopsis_localized", None) or "").strip() for ep in eps)
        except Exception:
            need_syn = False

        if not need_loc and not need_en and not need_syn:
            return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": 0, "episodes_updated": 0}

        if manage_state:
            try:
                set_state(
                    {
                        "running": True,
                        "current_id": getattr(mi, "id", None),
                        "current_title": (getattr(mi, "title_localized", None) or getattr(mi, "title_en", None) or getattr(mi, "title", None) or "TMDB"),
                        "current_step": "backfill_episodes_one",
                        "backfill_lang": lang,
                        "backfill_localized": bool(localize),
                        "backfill_total": 1,
                        "backfill_done": 0,
                        "backfill_seasons_updated": 0,
                        "backfill_episodes_updated": 0,
                    }
                )
            except Exception:
                pass

        season_data_loc = None
        if need_loc:
            try:
                season_data_loc = tmdb.tmdb_fetch_season(tmdb_id_s, season_number=season_number, language=lang)  # type: ignore[arg-type]
            except Exception:
                season_data_loc = None

        season_data_en = None
        if need_en:
            try:
                season_data_en = tmdb.tmdb_fetch_season(tmdb_id_s, season_number=season_number, language="en-US")  # type: ignore[arg-type]
            except Exception:
                season_data_en = None

        # Global synopsis fallback (series-level) when episode overviews are missing.
        global_synopsis = None
        if need_syn:
            try:
                details_loc = tmdb.tmdb_fetch_by_id(tmdb_id_s, media_type="tv", language=lang)  # type: ignore[arg-type]
            except Exception:
                details_loc = None
            if isinstance(details_loc, dict):
                try:
                    gs = details_loc.get("synopsis") or details_loc.get("overview") or ""
                    if isinstance(gs, str) and gs.strip():
                        global_synopsis = gs.strip()
                except Exception:
                    global_synopsis = None
            # Fallback to TMDB English (not Jikan) if localized synopsis is missing.
            if not global_synopsis:
                try:
                    details_en = tmdb.tmdb_fetch_by_id(tmdb_id_s, media_type="tv", language="en-US")  # type: ignore[arg-type]
                except Exception:
                    details_en = None
                if isinstance(details_en, dict):
                    try:
                        gs = details_en.get("synopsis") or details_en.get("overview") or ""
                        if isinstance(gs, str) and gs.strip():
                            global_synopsis = gs.strip()
                    except Exception:
                        global_synopsis = None
            if not global_synopsis and isinstance(season_data_loc, dict):
                try:
                    gs = season_data_loc.get("synopsis") or season_data_loc.get("overview") or ""
                    if isinstance(gs, str) and gs.strip():
                        global_synopsis = gs.strip()
                except Exception:
                    global_synopsis = None

        by_num: dict[int, Episode] = {}
        for ep in eps:
            try:
                n = getattr(ep, "episode_number", None)
                if isinstance(n, int):
                    by_num[n] = ep
            except Exception:
                continue

        changed_this_season = False

        if isinstance(season_data_loc, dict):
            loc_eps = season_data_loc.get("episodes") or []
            if isinstance(loc_eps, list):
                for e in loc_eps:
                    if not isinstance(e, dict):
                        continue
                    n = e.get("episode_number")
                    if not isinstance(n, int):
                        try:
                            n = int(n)
                        except Exception:
                            continue
                    row = by_num.get(n)
                    if not row:
                        continue
                    try:
                        tloc = e.get("title") or e.get("name")
                        if isinstance(tloc, str) and tloc.strip() and not (getattr(row, "title_localized", None) or "").strip():
                            row.title_localized = tloc.strip()
                            changed_this_season = True
                            episodes_updated += 1
                    except Exception:
                        pass
                    try:
                        ov = e.get("overview") or ""
                        if not (getattr(row, "synopsis_localized", None) or "").strip():
                            if isinstance(ov, str) and ov.strip():
                                row.synopsis_localized = ov.strip()
                                changed_this_season = True
                            elif isinstance(global_synopsis, str) and global_synopsis.strip():
                                # Requirement: when episode synopsis is missing, fall back to global series synopsis.
                                row.synopsis_localized = global_synopsis.strip()
                                changed_this_season = True
                    except Exception:
                        pass
                    session.add(row)

        if isinstance(season_data_en, dict):
            en_eps = season_data_en.get("episodes") or []
            if isinstance(en_eps, list):
                for e in en_eps:
                    if not isinstance(e, dict):
                        continue
                    n = e.get("episode_number")
                    if not isinstance(n, int):
                        try:
                            n = int(n)
                        except Exception:
                            continue
                    row = by_num.get(n)
                    if not row:
                        continue
                    try:
                        ten = e.get("title") or e.get("name")
                        if isinstance(ten, str) and ten.strip() and is_generic_episode_title(getattr(row, "title_en", None)):
                            row.title_en = ten.strip()
                            if is_generic_episode_title(getattr(row, "title", None)):
                                row.title = ten.strip()
                            changed_this_season = True
                            episodes_updated += 1
                            session.add(row)
                    except Exception:
                        continue

        if changed_this_season:
            try:
                session.commit()
                seasons_updated = 1
            except Exception:
                session.rollback()

        if manage_state:
            try:
                set_state(
                    {
                        "backfill_done": 1,
                        "backfill_seasons_updated": int(seasons_updated),
                        "backfill_episodes_updated": int(episodes_updated),
                        "running": False,
                        "current_step": "idle",
                    }
                )
            except Exception:
                pass

        return {"ok": True, "localized": bool(localize), "lang": lang, "seasons_updated": seasons_updated, "episodes_updated": episodes_updated}
    finally:
        session.close()

def repair_series_grouping_by_tmdb(dry_run: bool = False) -> dict[str, int | bool]:
    """Repair normalized Series/Season grouping when a single TMDB show was split into multiple Series rows.

    Root cause: earlier scans could create a Series keyed by a generic folder title like "Temporada 2".
    If enrichment later sets `series.tmdb_id`, we can safely merge those Series by tmdb_id.
    """
    session = SessionLocal()
    try:
        dup_tmdb_ids = [
            r[0]
            for r in session.query(Series.tmdb_id)
            .filter(Series.tmdb_id != None)  # noqa: E711
            .filter(Series.tmdb_id != "")
            .group_by(Series.tmdb_id)
            .having(func.count(Series.id) > 1)
            .all()
        ]

        def _is_generic_series_title(v: str | None) -> bool:
            if v is None:
                return True
            s = str(v).strip().lower()
            if not s:
                return True
            return bool(re.fullmatch(r"(?:temporada|season|part|cour|s)\s*0*\d{1,2}", s))

        merges = 0
        seasons_moved = 0
        series_deleted = 0

        for tid in dup_tmdb_ids:
            rows = session.query(Series).filter(Series.tmdb_id == tid).order_by(Series.id.asc()).all()
            if len(rows) < 2:
                continue

            # Pick canonical series: prefer non-generic title/provider_id, then lowest id.
            def score(sr: Series) -> tuple[int, int, int]:
                title = getattr(sr, "title", None)
                provider_id = getattr(sr, "provider_id", None)
                generic = 1 if (_is_generic_series_title(title) or _is_generic_series_title(provider_id)) else 0
                # Longer titles tend to be more descriptive; invert for sorting.
                ln = len(str(title or "")) if title else 0
                return (generic, -ln, int(getattr(sr, "id", 0) or 0))

            rows_sorted = sorted(rows, key=score)
            keep = rows_sorted[0]
            drop = [r for r in rows_sorted[1:] if getattr(r, "id", None) != getattr(keep, "id", None)]

            drop_ids = [int(getattr(r, "id")) for r in drop if getattr(r, "id", None) is not None]
            if not drop_ids:
                continue

            # Move seasons.
            srows = session.query(Season).filter(Season.series_id.in_(drop_ids)).all()
            if not srows:
                # nothing to move; safe to delete duplicates
                pass
            else:
                for s in srows:
                    if getattr(s, "series_id", None) != keep.id:
                        s.series_id = keep.id
                        seasons_moved += 1
                        session.add(s)

            # Improve kept series fields using best available values across rows.
            try:
                if not getattr(keep, "title_en", None):
                    for r in rows_sorted:
                        te = getattr(r, "title_en", None)
                        if te and isinstance(te, str) and te.strip() and not _is_generic_series_title(te):
                            keep.title_en = te.strip()
                            break
            except Exception:
                pass
            try:
                if not getattr(keep, "title_localized", None):
                    for r in rows_sorted:
                        tl = getattr(r, "title_localized", None)
                        if tl and isinstance(tl, str) and tl.strip() and not _is_generic_series_title(tl):
                            keep.title_localized = tl.strip()
                            break
            except Exception:
                pass
            try:
                # Ensure `title` is not generic.
                if _is_generic_series_title(getattr(keep, "title", None)):
                    best = getattr(keep, "title_localized", None) or getattr(keep, "title_en", None)
                    if best and isinstance(best, str) and best.strip():
                        keep.title = best.strip()
            except Exception:
                pass
            session.add(keep)

            if dry_run:
                session.rollback()
                merges += 1
                continue

            try:
                session.commit()
            except Exception:
                session.rollback()
                continue

            merges += 1

            # Delete orphan duplicate Series rows (now with no seasons).
            for r in drop:
                try:
                    sid = int(getattr(r, "id"))
                except Exception:
                    continue
                try:
                    left = session.query(Season).filter(Season.series_id == sid).count()
                except Exception:
                    left = 1
                if left:
                    continue
                try:
                    session.query(Series).filter(Series.id == sid).delete(synchronize_session=False)
                    session.commit()
                    series_deleted += 1
                except Exception:
                    session.rollback()

        return {
            "ok": True,
            "dry_run": bool(dry_run),
            "merges": merges,
            "seasons_moved": seasons_moved,
            "series_deleted": series_deleted,
        }
    finally:
        session.close()


def enrich_one(media_item_id: int, providers_override: Optional[Iterable[str]] = None, force_media_type: Optional[str] = None) -> bool:
    session = SessionLocal()
    try:
        mi = session.query(MediaItem).filter(MediaItem.id == media_item_id).first()
        if not mi:
            return False
        mi = _cast(_Any, mi)
        try:
            set_state({'running': True, 'current_id': mi.id, 'current_title': mi.title or '', 'current_step': 'starting'})
        except Exception:
            pass
        cfg_movies = get('metadata_movies_provider') or 'tmdb'
        cfg_anime = get('metadata_anime_provider') or 'jikan'
        if force_media_type:
            media_type = force_media_type
        else:
            inferred = _decide_media_type(mi, session) or 'movie'
            cur_mt = (mi.media_type or '').strip().lower()
            if inferred == 'series' and cur_mt not in ('series', 'tv', 'show'):
                media_type = 'series'
            else:
                media_type = mi.media_type or inferred
        mi.media_type = media_type
        title = mi.title or ''
        normalized = {}
        candidate_titles = _candidate_titles_for_item(mi)

        # Strict hierarchy: if this looks like a season subfolder and parent already has TMDB identity,
        # skip TMDB search and fetch the season directly.
        try:
            inherited = _inherit_tmdb_identity_from_parent_if_season(mi, session)
        except Exception:
            inherited = None
        if inherited and tmdb is not None and hasattr(tmdb, "tmdb_fetch_season"):
            parent_tmdb_id, season_number = inherited
            try:
                lang = str(get("metadata_language") or "").strip() or "en-US"
            except Exception:
                lang = "en-US"
            try:
                mi.tmdb_id = str(parent_tmdb_id)
                mi.media_type = 'series'
                session.add(mi)
                session.commit()
            except Exception:
                session.rollback()
            try:
                normalized2 = tmdb.tmdb_fetch_season(str(parent_tmdb_id), season_number=season_number, language=lang)  # type: ignore[arg-type]
            except Exception:
                normalized2 = None
            if isinstance(normalized2, dict) and normalized2:
                try:
                    normalized2.setdefault("season_number", int(season_number))
                except Exception:
                    pass
                try:
                    set_state({'current_step': 'persist_metadata'})
                except Exception:
                    pass
                try:
                    _attach_tmdb_i18n(normalized2, 'series')
                    _attach_episodes('tmdb', normalized2, 'series', mi)
                except Exception:
                    pass
                _persist_metadata(session, mi, normalized2)
                try:
                    clear_state()
                except Exception:
                    pass
                return True
        try:
            if providers_override:
                try:
                    providers_order = [p.strip().lower() for p in providers_override if isinstance(p, str) and p]
                except Exception:
                    providers_order = []
            else:
                providers_order = _provider_order_for_item(media_type, getattr(mi, 'mal_id', None), cfg_movies, cfg_anime, getattr(mi, 'media_root', None))
            logger.info("enrich_one: media_item=%s media_type=%s providers_order=%s", getattr(mi, 'id', None), media_type, providers_order)
            try:
                tmdb_ok = _tmdb_configured()
            except Exception:
                tmdb_ok = False
            logger.debug('enrich_one: media_item=%s tmdb_configured=%s', getattr(mi, 'id', None), tmdb_ok)
            if providers_order == ['tmdb'] and not tmdb_ok:
                logger.warning(
                    "enrich_one: TMDB not configured; skipping enrichment for media_item=%s",
                    getattr(mi, "id", None),
                )
                try:
                    try:
                        mi.title = _fallback_title_for_item(mi)
                    except Exception:
                        pass
                    mi.status = "NO_MATCH"
                    try:
                        mi.is_identified = False
                    except Exception:
                        pass
                    session.add(mi)
                    session.commit()
                except Exception:
                    session.rollback()
                try:
                    clear_state()
                except Exception:
                    pass
                return False
            try:
                if 'tmdb' in providers_order and not tmdb_ok:
                    providers_order = [p for p in providers_order if p != 'tmdb']
            except Exception:
                pass
            if not providers_order:
                logger.warning(
                    "enrich_one: empty providers_order; skipping enrichment for media_item=%s",
                    getattr(mi, "id", None),
                )
                try:
                    mi.status = "NO_MATCH"
                    session.add(mi)
                    session.commit()
                except Exception:
                    session.rollback()
                try:
                    clear_state()
                except Exception:
                    pass
                return False
        except Exception:
            providers_order = _provider_order_for_item(media_type, getattr(mi, 'mal_id', None), cfg_movies, cfg_anime, getattr(mi, 'media_root', None))

        provider_error_any = False
        for p in providers_order:
            try:
                res = None
                try:
                    set_state({'current_step': f'search:{p}'})
                except Exception:
                    pass
                if p == 'jikan' and jikan is not None:
                    if getattr(mi, 'mal_id', None) and hasattr(jikan, 'fetch_by_id'):
                        try:
                            logger.info('enrich_one: calling jikan.fetch_by_id mal_id=%s media_item=%s', getattr(mi, 'mal_id', None), getattr(mi, 'id', None))
                            res = jikan.fetch_by_id(str(mi.mal_id))
                            logger.info('enrich_one: jikan.fetch_by_id result for media_item=%s -> %s', getattr(mi, 'id', None), 'ok' if res else 'none')
                        except Exception:
                            logger.exception('enrich_one: jikan.fetch_by_id failed for media_item=%s', getattr(mi, 'id', None))
                            provider_error_any = True
                            res = None
                    if not res:
                        # provider_jikan already expands name variants; do a single request per item.
                        for cand in (candidate_titles[:1] if isinstance(candidate_titles, list) else []):
                            try:
                                logger.info('enrich_one: jikan.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                r = jikan.search(cand) if hasattr(jikan, 'search') else None
                                logger.info('enrich_one: jikan.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                            except Exception:
                                logger.warning('enrich_one: jikan.search failed for query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                provider_error_any = True
                                r = None
                                break
                            if isinstance(r, list) and r:
                                res = r[0]
                                break
                            if r and not isinstance(r, list):
                                res = r
                                break
                elif p == 'tmdb' and tmdb is not None:
                    for cand in candidate_titles:
                        try:
                            logger.info('enrich_one: tmdb.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                            r = _tmdb_search_with_optional_season(cand, media_type)
                            logger.info('enrich_one: tmdb.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                        except Exception:
                            logger.exception('enrich_one: tmdb.search failed for query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                            provider_error_any = True
                            r = None
                        if isinstance(r, list) and r:
                            res = r[0]
                            break
                        if r and not isinstance(r, list):
                            res = r
                            break
                if not res:
                    continue
                normalized = _normalize_result(p, res)
                if normalized:
                    break
            except Exception:
                logger.debug('Provider %s failed for title=%s', p, title, exc_info=True)

        if not normalized:
            # Bridge: if Jikan couldn't match and TMDB is configured, try a single TMDB lookup as TV.
            try:
                try:
                    tmdb_ok = _tmdb_configured()
                except Exception:
                    tmdb_ok = False
                if tmdb_ok and ('tmdb' not in providers_order) and ('jikan' in providers_order or providers_order == ['jikan']):
                    is_series = (media_type or '').strip().lower() in ('series', 'tv', 'anime')
                    if not is_series:
                        try:
                            is_series = session.query(FileRecord).filter(FileRecord.media_item_id == mi.id).count() > 1
                        except Exception:
                            is_series = False
                    if is_series:
                        cand0 = candidate_titles[0] if candidate_titles else (mi.title or '')
                        cand0 = str(cand0 or '').strip()
                        if cand0:
                            logger.info('enrich_one: bridge jikan->tmdb (tv) query="%s" media_item=%s', cand0, getattr(mi, 'id', None))
                            r = _tmdb_search_with_optional_season(cand0, 'tv')
                            if isinstance(r, list) and r:
                                r = r[0]
                            if r and isinstance(r, dict):
                                normalized = _normalize_result('tmdb', r)
            except Exception:
                pass

        if not normalized:
            try:
                set_state({'current_step': 'provider_error' if provider_error_any else 'no_match'})
            except Exception:
                pass
            if not provider_error_any:
                try:
                    try:
                        mi.title = _fallback_title_for_item(mi)
                    except Exception:
                        pass
                    mi.status = 'NO_MATCH'
                    try:
                        mi.is_identified = False
                    except Exception:
                        pass
                    session.add(mi)
                    session.commit()
                except Exception:
                    session.rollback()
            try:
                clear_state()
            except Exception:
                pass
            return False
        try:
            set_state({'current_step': 'persist_metadata'})
        except Exception:
            pass
        try:
            _attach_tmdb_i18n(normalized, media_type)
            _attach_episodes(normalized.get('provider') or '', normalized, media_type, mi)
        except Exception:
            pass
        _persist_metadata(session, mi, normalized)
        try:
            clear_state()
        except Exception:
            pass
        return True
    finally:
        session.close()


def enrich_pending_detailed(limit: Optional[int] = None, dry_run: bool = False) -> dict:
    session = SessionLocal()
    report = {'total': 0, 'pending': 0, 'enriched': [], 'failed': [], 'items': []}
    try:
        report['total'] = session.query(MediaItem).count()
        subq = session.query(MediaMetadata.media_item_id).filter(MediaMetadata.provider != None).filter(MediaMetadata.provider != '')
        q = session.query(MediaItem).filter(~MediaItem.id.in_(subq)).filter(
            (MediaItem.status == None)
            | (
                (MediaItem.status != 'ERROR')
                & (MediaItem.status != 'MANUAL')
                & (MediaItem.status != 'OMITTED')
            )
        )
        if limit:
            q = q.limit(limit)
        candidates = q.all()
        report['pending'] = len(candidates)
        cfg_movies = get('metadata_movies_provider') or 'tmdb'
        cfg_anime = get('metadata_anime_provider') or 'jikan'

        for mi in candidates:
            mi = _cast(_Any, mi)
            try:
                media_type = mi.media_type or _decide_media_type(mi, session) or 'movie'
                title = mi.title or ''
                normalized = {}
                candidate_titles = _candidate_titles_for_item(mi)
                providers_order = _provider_order_for_item(media_type, getattr(mi, 'mal_id', None), cfg_movies, cfg_anime, getattr(mi, 'media_root', None))
                try:
                    if 'tmdb' in providers_order and not _tmdb_configured():
                        providers_order = [p for p in providers_order if p != 'tmdb']
                except Exception:
                    pass
                if not providers_order:
                    providers_order = ['jikan']
                if providers_order == ['tmdb'] and not _tmdb_configured():
                    report['failed'].append((mi.id, 'tmdb_not_configured'))
                    continue

                found = False
                last_exc = None
                provider_used = None
                provider_result_raw = None
                ambiguous = False
                start = time.perf_counter()
                for p in providers_order:
                    try:
                        res = None
                        if p == 'jikan' and jikan is not None:
                            if getattr(mi, 'mal_id', None):
                                try:
                                    logger.info('enrich_pending_detailed: calling jikan.fetch_by_id mal_id=%s media_item=%s', getattr(mi, 'mal_id', None), getattr(mi, 'id', None))
                                    res = jikan.fetch_by_id(str(mi.mal_id))
                                    logger.info('enrich_pending_detailed: jikan.fetch_by_id result for media_item=%s -> %s', getattr(mi, 'id', None), 'ok' if res else 'none')
                                except Exception:
                                    res = None
                            if not res:
                                # provider_jikan already expands name variants; limit attempts.
                                for cand in (candidate_titles[:1] if isinstance(candidate_titles, list) else []):
                                    try:
                                        logger.info('enrich_pending_detailed: jikan.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                        r = jikan.search(cand) if hasattr(jikan, 'search') else None
                                        logger.info('enrich_pending_detailed: jikan.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                                    except Exception:
                                        r = None
                                        break
                                    if isinstance(r, list) and r:
                                        res = r
                                        break
                                    if r and not isinstance(r, list):
                                        res = r
                                        break
                        elif p == 'tmdb' and tmdb is not None:
                            for cand in (candidate_titles[:2] if isinstance(candidate_titles, list) else []):
                                try:
                                    logger.info('enrich_pending_detailed: tmdb.search query="%s" media_item=%s', cand, getattr(mi, 'id', None))
                                    r = _tmdb_search_with_optional_season(cand, media_type)
                                    logger.info('enrich_pending_detailed: tmdb.search returned %s candidates for media_item=%s', (len(r) if isinstance(r, list) else (1 if r else 0)), getattr(mi, 'id', None))
                                except Exception:
                                    r = None
                                if isinstance(r, list) and r:
                                    res = r
                                    break
                                if r and not isinstance(r, list):
                                    res = r
                                    break

                        if isinstance(res, list):
                            provider_result_raw = res
                            if len(res) == 0:
                                continue
                            if len(res) > 1:
                                ambiguous = True
                            res_for_norm = res[0]
                        else:
                            provider_result_raw = res
                            res_for_norm = res

                        if not res_for_norm:
                            continue

                        normalized = _normalize_result(p, res_for_norm)
                        if normalized:
                            found = True
                            provider_used = p
                            break
                    except Exception as exc:
                        last_exc = exc
                        logger.debug('Provider %s failed for title=%s', p, title, exc_info=True)
                elapsed_ms = int((time.perf_counter() - start) * 1000)

                item_result = {
                    'id': mi.id,
                    'title': title,
                    'provider': provider_used or None,
                    'result': None,
                    'duration_ms': elapsed_ms,
                    'info': None,
                }

                if not found:
                    item_result['result'] = 'no-match'
                    if last_exc:
                        item_result['info'] = str(last_exc)
                    report['failed'].append((mi.id, 'no_match'))
                    report['items'].append(item_result)
                    continue

                if ambiguous:
                    item_result['result'] = 'ambiguous'
                    try:
                        item_result['info'] = f"{len(provider_result_raw)} candidates" # pyright: ignore[reportArgumentType]
                    except Exception:
                        item_result['info'] = 'multiple candidates'
                else:
                    item_result['result'] = 'success'

                if dry_run:
                    report['enriched'].append(mi.id)
                    report['items'].append(item_result)
                    continue

                try:
                    try:
                        _attach_tmdb_i18n(normalized, media_type)
                        _attach_episodes(normalized.get('provider') or '', normalized, media_type, mi)
                    except Exception:
                        pass
                    _persist_metadata(session, mi, normalized)
                    report['enriched'].append(mi.id)
                    item_result['result'] = 'success'
                except Exception as exc:
                    report['failed'].append((mi.id, str(exc)))
                    item_result['result'] = 'error'
                    item_result['info'] = str(exc)

                report['items'].append(item_result)
            except Exception as exc:
                logger.exception('Failed enriching media_item %s', mi.id)
                report['failed'].append((mi.id, str(exc)))
        return report
    finally:
        session.close()
