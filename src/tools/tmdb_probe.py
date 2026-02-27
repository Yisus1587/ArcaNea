"""Small TMDB probe utility (dev-only).

Run this locally to validate TMDB configuration and season/episode extraction without involving scan/enrich.

Examples:
  python -m src.tools.tmdb_probe --tmdb-id 52814 --season 2 --lang es-419
  python -m src.tools.tmdb_probe --query "Halo Season 2" --pref tv --lang es-419
"""

from __future__ import annotations

import argparse
import json
import os
import sys

try:
    from ..providers import provider_tmdb as tmdb  # type: ignore
except Exception:
    try:
        from src.providers import provider_tmdb as tmdb  # type: ignore
    except Exception:  # pragma: no cover
        tmdb = None


def _die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", help="TMDB search query (movie/tv).")
    ap.add_argument("--pref", default="auto", help="Search preference: auto|tv|movie.")
    ap.add_argument("--tmdb-id", help="TMDB numeric id (tv/movie).")
    ap.add_argument("--type", default="tv", help="Type for --tmdb-id fetch: tv|movie.")
    ap.add_argument("--season", type=int, help="Season number for tmdb_fetch_season.")
    ap.add_argument("--lang", default=os.environ.get("ARCANEA_TMBD_LANG") or "", help="Language tag (e.g. es-419, es-MX, en-US).")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    args = ap.parse_args(argv)

    if tmdb is None:
        _die("TMDB provider not importable (src.providers.provider_tmdb).")

    lang = args.lang.strip() or None

    out: dict = {"ok": True}

    if args.query:
        res = tmdb.search_by_type(args.query, media_preference=args.pref, allow_when_config_is_jikan=True)
        out["search"] = res
        if isinstance(res, dict):
            out["tmdb_id"] = res.get("tmdb_id") or res.get("provider_id")
            out["media_type"] = res.get("media_type")

    if args.tmdb_id:
        out["by_id"] = tmdb.tmdb_fetch_by_id(args.tmdb_id, media_type=args.type, language=lang)

    if args.tmdb_id and args.season:
        out["season"] = tmdb.tmdb_fetch_season(args.tmdb_id, season_number=int(args.season), language=lang)
        try:
            eps = (out.get("season") or {}).get("episodes") or []
            out["season_episode_count"] = len(eps) if isinstance(eps, list) else 0
            if isinstance(eps, list) and eps:
                out["season_episode_sample"] = eps[:3]
        except Exception:
            pass

    if not (args.query or args.tmdb_id):
        _die("Provide --query or --tmdb-id.", 2)

    if args.pretty:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

