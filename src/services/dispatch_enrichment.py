"""Dispatch enrichment after a filesystem scan.

This module processes pending `MediaItem` rows grouped by configured `media_roots` in
the order they are declared. For each root, it finds pending items (no MediaMetadata)
under that root and calls `enrich_one` for each item so provider preference follows
the media_root mapping.
"""
import logging
from typing import Optional
from pathlib import Path

from ..db import SessionLocal
from ..db.models import MediaItem, MediaMetadata, MediaRoot
from ..core.config import get
from .enrichment_runner import enrich_one_serialized

logger = logging.getLogger(__name__)


def dispatch_enrichment_by_roots(limit_per_root: Optional[int] = None) -> dict:
    """Dispatch enrichment for pending items grouped by configured media_roots.

    Returns a report dict with processed counts by root.
    """
    session = SessionLocal()
    report = {'stages': [], 'total_processed': 0}
    try:
        # Prefer explicit MediaRoot rows (persisted by scanner/onboarding) since those
        # represent the source of truth for scanning. Fall back to core config roots
        # only when DB has none (legacy).
        try:
            persisted = session.query(MediaRoot).all()
        except Exception:
            persisted = []
        roots_cfg = get('media_roots') or []

        def _classify_root(bname: str) -> str:
            b = bname.lower()
            if any(k in b for k in ('anime', 'animes')):
                return 'anime'
            if any(k in b for k in ('pelicula', 'peliculas', 'movie', 'movies', 'film')):
                return 'movie'
            if any(k in b for k in ('serie', 'series', 'tv', 'shows')):
                return 'tv'
            return 'other'

        root_entries = []
        if persisted:
            for pr in persisted:
                rp0 = getattr(pr, "path", None)
                if not rp0:
                    continue
                try:
                    root_path = str(Path(str(rp0)).resolve())
                except Exception:
                    root_path = str(rp0)
                rt = (getattr(pr, "type", None) or "").strip().lower() or None
                if rt not in ("anime", "movie", "tv"):
                    rt = _classify_root(Path(root_path).name)
                root_entries.append({'root': root_path, 'class': rt})
        else:
            for r in roots_cfg:
                try:
                    root_path = str(Path(r).resolve())
                except Exception:
                    root_path = str(r)
                rt = _classify_root(Path(root_path).name)
                root_entries.append({'root': root_path, 'class': rt})

        # Stage 1: run Jikan first only for roots classified as 'anime'
        stage_report = {'stage': 'jikan_first', 'per_root': []}
        for entry in root_entries:
            if entry['class'] != 'anime':
                continue
            root_path = entry['root']
            # find pending items under this root (no MediaMetadata with provider)
            subq = session.query(MediaMetadata.media_item_id).filter(MediaMetadata.provider != None).filter(MediaMetadata.provider != '')
            q = session.query(MediaItem).filter(~MediaItem.id.in_(subq)).filter((MediaItem.status == None) | ((MediaItem.status != 'ERROR') & (MediaItem.status != 'NO_MATCH')))
            like_pattern = f"{root_path}%"
            q = q.filter(MediaItem.canonical_path.like(like_pattern))
            if limit_per_root:
                q = q.limit(limit_per_root)
            pending = q.all()
            processed = 0
            for mi in pending:
                try:
                    mid_val = getattr(mi, 'id', None)
                    try:
                        mid_int = int(mid_val) if mid_val is not None else None
                    except Exception:
                        mid_int = None
                    if mid_int is None:
                        logger.warning('Skipping media_item with non-int id=%s', mid_val)
                        continue
                    # Don't force 'series' for anime roots; let enrichment decide (movie vs series)
                    # based on files count and metadata.
                    ok = enrich_one_serialized(mid_int, providers_override=['jikan'], force_media_type=None)
                    if ok:
                        processed += 1
                except Exception:
                    logger.exception('Failed enriching media_item %s during jikan stage', getattr(mi, 'id', None))
            stage_report['per_root'].append({'root': root_path, 'class': entry['class'], 'pending': len(pending), 'processed': processed})
            report['total_processed'] += processed
        report['stages'].append(stage_report)

        # Stage 2: run TMDB for movie/tv roots (and "other" roots as best-effort)
        stage_report = {'stage': 'tmdb_last', 'per_root': []}
        for entry in root_entries:
            if entry['class'] not in ('movie', 'tv', 'other'):
                continue
            root_path = entry['root']
            subq = session.query(MediaMetadata.media_item_id).filter(MediaMetadata.provider != None).filter(MediaMetadata.provider != '')
            # TMDB stage should also repair items that were previously enriched with the wrong provider
            # (e.g. Jikan match in a TV root) by ensuring a TMDB identity is present.
            q = (
                session.query(MediaItem)
                .filter((MediaItem.status == None) | ((MediaItem.status != 'ERROR') & (MediaItem.status != 'NO_MATCH')))
                .filter(
                    (~MediaItem.id.in_(subq))
                    | (MediaItem.tmdb_id == None)  # noqa: E711
                    | (MediaItem.tmdb_id == "")
                )
            )
            like_pattern = f"{root_path}%"
            q = q.filter(MediaItem.canonical_path.like(like_pattern))
            if limit_per_root:
                q = q.limit(limit_per_root)
            pending = q.all()
            processed = 0
            for mi in pending:
                try:
                    mid_val = getattr(mi, 'id', None)
                    try:
                        mid_int = int(mid_val) if mid_val is not None else None
                    except Exception:
                        mid_int = None
                    if mid_int is None:
                        logger.warning('Skipping media_item with non-int id=%s', mid_val)
                        continue
                    # choose media_type based on root class: 'movie' -> movie, 'tv' -> series, 'other' -> autodetect
                    fm = 'movie' if entry['class'] == 'movie' else ('series' if entry['class'] == 'tv' else None)
                    ok = enrich_one_serialized(mid_int, providers_override=['tmdb'], force_media_type=fm)
                    if ok:
                        processed += 1
                except Exception:
                    logger.exception('Failed enriching media_item %s during tmdb stage', getattr(mi, 'id', None))
            stage_report['per_root'].append({'root': root_path, 'class': entry['class'], 'pending': len(pending), 'processed': processed})
            report['total_processed'] += processed
        report['stages'].append(stage_report)

        return report
    finally:
        session.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print('Dispatching enrichment by configured media_roots...')
    r = dispatch_enrichment_by_roots()
    print('Dispatch report:', r)
