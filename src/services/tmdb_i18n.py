from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _poster_url(path: str | None) -> str | None:
    if not path:
        return None
    p = str(path).strip()
    if not p:
        return None
    return f"https://image.tmdb.org/t/p/original{p}"


def _safe_str(v: Any) -> str | None:
    if v is None:
        return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    return s or None


def _coerce_int(v: Any) -> int | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        return int(v)
    except Exception:
        return None


def _drop_empty(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, list):
        out = []
        for x in v:
            xx = _drop_empty(x)
            if xx is None:
                continue
            out.append(xx)
        return out or None
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            xx = _drop_empty(x)
            if xx is None:
                continue
            out[k] = xx
        return out or None
    return v


def _pick_lang(i18n: dict, lang: str) -> dict:
    v = i18n.get(lang)
    return v if isinstance(v, dict) else {}


def _fallback(primary: dict, secondary: dict) -> dict:
    out = dict(primary or {})
    for k, v in (secondary or {}).items():
        if k not in out or out.get(k) in (None, "", [], {}):
            out[k] = v

    ps = out.get("seasons") if isinstance(out.get("seasons"), dict) else {}
    ss = secondary.get("seasons") if isinstance(secondary.get("seasons"), dict) else {}
    merged_seasons = dict(ps)
    for sn, sdata in ss.items():
        if sn not in merged_seasons:
            merged_seasons[sn] = sdata
            continue
        if isinstance(merged_seasons.get(sn), dict) and isinstance(sdata, dict):
            cur = dict(merged_seasons[sn])
            for kk, vv in sdata.items():
                if kk not in cur or cur.get(kk) in (None, "", [], {}):
                    cur[kk] = vv
            merged_seasons[sn] = cur
    if merged_seasons:
        out["seasons"] = merged_seasons
    return out


def _tmdb_auth() -> tuple[dict[str, str], dict[str, str]]:
    try:
        from src.core.config import config  # type: ignore
    except Exception:
        config = {}

    key = (os.environ.get("TMDB_API_KEY") or config.get("tmdb_api_key") or "").strip()  # type: ignore[union-attr]
    use_v4 = bool(config.get("tmdb_use_v4", False))  # type: ignore[union-attr]
    access = os.environ.get("TMDB_ACCESS_TOKEN") or (config.get("tmdb_access_token") if use_v4 else None)  # type: ignore[union-attr]
    access = str(access or "").strip()

    headers: dict[str, str] = {}
    params: dict[str, str] = {}
    if access:
        headers["Authorization"] = f"Bearer {access}"
    elif key:
        params["api_key"] = key
    return params, headers


@dataclass(frozen=True)
class _Lang:
    key: str
    tmdb_language: str


_LANGS = [
    _Lang("es", "es-ES"),
    _Lang("en", "en-US"),
]


def fetch_tmdb_tv_i18n_clean(
    tmdb_id: str,
    *,
    primary: str = "en",
    season_numbers: list[int] | None = None,
    include_all_seasons: bool = False,
    max_workers: int = 6,
) -> dict[str, Any] | None:
    """
    Fetch TMDB TV base + seasons in ES/EN, then return a cleaned structure:
    - i18n: {es, en}
    - modal: resolved primary with fallback
    Cached in meta_cache.json via src.tools.cache.
    """
    tid = str(tmdb_id).strip()
    if not tid:
        return None
    primary = (primary or "en").strip().lower()
    if primary not in ("en", "es"):
        primary = "en"

    sn_key = "all" if include_all_seasons else ",".join(str(x) for x in (season_numbers or [])) or "base"
    cache_key = f"tmdb_i18n_clean:tv:{tid}:primary={primary}:seasons={sn_key}"

    try:
        from src.tools.cache import cache_get, cache_set  # type: ignore
    except Exception:
        cache_get = None
        cache_set = None

    try:
        if cache_get is not None:
            cached = cache_get(cache_key)
            if isinstance(cached, dict) and cached.get("i18n"):
                return cached
    except Exception:
        pass

    try:
        from src.providers import provider_tmdb as tmdb  # type: ignore
    except Exception as e:
        logger.debug("tmdb_i18n: cannot import provider_tmdb: %s", e)
        return None

    params_base, headers_base = _tmdb_auth()
    if not params_base and not headers_base:
        return None

    # Base tv/{id} in parallel
    def fetch_base(lang: _Lang) -> tuple[str, dict | None]:
        params = dict(params_base)
        params["language"] = lang.tmdb_language
        params["append_to_response"] = "credits"
        data = tmdb._tmdb_get(f"tv/{tid}", params=params or None, headers=headers_base or None)  # type: ignore[attr-defined]
        return lang.key, data if isinstance(data, dict) else None

    base_by_lang: dict[str, dict | None] = {}
    with ThreadPoolExecutor(max_workers=max(2, int(max_workers))) as ex:
        futs = [ex.submit(fetch_base, lang) for lang in _LANGS]
        for fut in as_completed(futs):
            k, data = fut.result()
            base_by_lang[k] = data

    base_ref = base_by_lang.get("en") or base_by_lang.get("es") or {}
    if not isinstance(base_ref, dict) or not base_ref:
        return None

    sns: list[int] = []
    if include_all_seasons:
        seasons = base_ref.get("seasons")
        if isinstance(seasons, list):
            for s in seasons:
                if not isinstance(s, dict):
                    continue
                sn = _coerce_int(s.get("season_number"))
                if sn is None or sn == 0:
                    continue
                sns.append(sn)
        sns = sorted(set(sns))
    elif season_numbers:
        sns = sorted(set(int(x) for x in season_numbers if isinstance(x, int) and x > 0))

    seasons_by_lang: dict[str, dict[int, dict | None]] = {l.key: {} for l in _LANGS}
    if sns:
        def fetch_season(lang: _Lang, sn: int) -> tuple[str, int, dict | None]:
            params = dict(params_base)
            params["language"] = lang.tmdb_language
            data = tmdb._tmdb_get(f"tv/{tid}/season/{sn}", params=params or None, headers=headers_base or None)  # type: ignore[attr-defined]
            return lang.key, sn, data if isinstance(data, dict) else None

        with ThreadPoolExecutor(max_workers=max(2, int(max_workers))) as ex:
            futs = [ex.submit(fetch_season, lang, sn) for lang in _LANGS for sn in sns]
            for fut in as_completed(futs):
                lk, sn, data = fut.result()
                seasons_by_lang.setdefault(lk, {})[sn] = data

    def normalize_lang(lang_key: str) -> dict[str, Any]:
        base = base_by_lang.get(lang_key) or {}
        title = _safe_str(base.get("name") or base.get("title") or "")
        synopsis = _safe_str(base.get("overview") or "")

        genres: list[str] = []
        for g in base.get("genres") or []:
            if isinstance(g, dict):
                n = _safe_str(g.get("name"))
                if n:
                    genres.append(n)

        credits = base.get("credits") if isinstance(base, dict) else None
        cast_names: list[str] = []
        director_name: str | None = None
        if isinstance(credits, dict):
            cast = credits.get("cast") or []
            if isinstance(cast, list):
                for c in cast[:12]:
                    if isinstance(c, dict):
                        n = _safe_str(c.get("name"))
                        if n:
                            cast_names.append(n)
            crew = credits.get("crew") or []
            if isinstance(crew, list):
                for c in crew:
                    if not isinstance(c, dict):
                        continue
                    if _safe_str(c.get("job")) == "Director":
                        director_name = _safe_str(c.get("name"))
                        break

        seasons_out: dict[str, dict[str, Any]] = {}
        for sn, sraw in (seasons_by_lang.get(lang_key) or {}).items():
            if not isinstance(sraw, dict):
                seasons_out[str(sn)] = {"season_number": sn, "title": None, "synopsis": None, "episodes": []}
                continue
            stitle = _safe_str(sraw.get("name") or f"Season {sn}")
            ssyn = _safe_str(sraw.get("overview") or "")
            episodes: list[dict[str, Any]] = []
            for ep in sraw.get("episodes") or []:
                if not isinstance(ep, dict):
                    continue
                episodes.append(
                    {
                        "episode_number": _coerce_int(ep.get("episode_number")),
                        "title": _safe_str(ep.get("name") or ""),
                        "overview": _safe_str(ep.get("overview") or ""),
                        "air_date": _safe_str(ep.get("air_date")),
                    }
                )
            seasons_out[str(sn)] = {
                "season_number": sn,
                "title": stitle,
                "synopsis": ssyn,
                "episodes": episodes,
                "episode_titles": [e["title"] for e in episodes if e.get("title")],
                "poster_url": _poster_url(sraw.get("poster_path")),
            }

        return {
            "title": title,
            "synopsis": synopsis,
            "genres": genres,
            "images": {
                "jpg": {
                    "large_image_url": _poster_url(base.get("poster_path") or base.get("backdrop_path")),
                }
            },
            "dates": {
                "first_air_date": _safe_str(base.get("first_air_date")),
                "last_air_date": _safe_str(base.get("last_air_date")),
            },
            "status": _safe_str(base.get("status")),
            "counts": {
                "number_of_seasons": _coerce_int(base.get("number_of_seasons")),
                "number_of_episodes": _coerce_int(base.get("number_of_episodes")),
            },
            "credits": {
                "director": director_name,
                "cast": cast_names,
            },
            "seasons": seasons_out,
        }

    i18n_raw = {
        "es": normalize_lang("es"),
        "en": normalize_lang("en"),
    }

    es_final = _fallback(_pick_lang(i18n_raw, "es"), _pick_lang(i18n_raw, "en"))
    en_final = _fallback(_pick_lang(i18n_raw, "en"), _pick_lang(i18n_raw, "es"))
    modal = _fallback(en_final, es_final) if primary == "en" else _fallback(es_final, en_final)

    cleaned = {
        "generated_at": _now_iso(),
        "provider": "tmdb",
        "ids": {"tmdb_id": tid, "media_type": "tv"},
        "languages": {"primary": primary, "available": ["es", "en"]},
        "i18n": {"es": es_final, "en": en_final},
        "modal": {
            "title": modal.get("title"),
            "overview": modal.get("synopsis"),
            "genres": modal.get("genres") or [],
            "images": modal.get("images") or {},
            "dates": modal.get("dates") or {},
            "status": modal.get("status"),
            "counts": modal.get("counts") or {},
            "credits": modal.get("credits") or {},
            "seasons": modal.get("seasons") or {},
        },
    }

    cleaned = _drop_empty(cleaned) or {}
    try:
        if cache_set is not None and isinstance(cleaned, dict) and cleaned.get("i18n"):
            cache_set(cache_key, cleaned)
    except Exception:
        pass
    return cleaned if isinstance(cleaned, dict) else None

