from __future__ import annotations

import time

from sqlalchemy.orm import Session

from ...db import models
from ...services.enrich_state import get_state as get_enrich_state
from ...services.scan_state import get_state as get_scan_state
from ...services.enrichment_runner import is_paused as is_enrich_paused

from .. import runtime


def compute_enrich_counts(db: Session) -> dict:
    total = db.query(models.MediaItem).count()

    # only consider a MediaItem enriched if there's a MediaMetadata row with a non-empty provider
    enriched_subq = (
        db.query(models.MediaMetadata.media_item_id)
        .filter(models.MediaMetadata.provider != None)  # noqa: E711
        .filter(models.MediaMetadata.provider != "")
    )
    not_enriched_q = db.query(models.MediaItem).filter(~models.MediaItem.id.in_(enriched_subq))
    pending_total = not_enriched_q.filter(
        (models.MediaItem.status == None)  # noqa: E711
        | ((models.MediaItem.status != "OMITTED") & (models.MediaItem.status != "MANUAL"))
    ).count()
    enriched = total - pending_total

    # "Runnable" items are those that are not enriched and not terminally excluded by status.
    pending_runnable = (
        not_enriched_q.filter(
            (models.MediaItem.status == None)  # noqa: E711
            | (
                (models.MediaItem.status != "ERROR")
                & (models.MediaItem.status != "NO_MATCH")
                & (models.MediaItem.status != "MANUAL")
                & (models.MediaItem.status != "OMITTED")
            )
        ).count()
    )
    no_match = not_enriched_q.filter(models.MediaItem.status == "NO_MATCH").count()
    error = not_enriched_q.filter(models.MediaItem.status == "ERROR").count()
    omitted = not_enriched_q.filter(models.MediaItem.status == "OMITTED").count()
    manual = not_enriched_q.filter(models.MediaItem.status == "MANUAL").count()
    return {
        "total": total,
        "pending": pending_runnable,
        "pending_total": pending_total,
        "enriched": enriched,
        "no_match": no_match,
        "error": error,
        "omitted": omitted,
        "manual": manual,
    }


def status_snapshot(db: Session | None = None) -> dict:
    try:
        scan = get_scan_state()
    except Exception:
        scan = {"status": "idle"}

    try:
        enrich = get_enrich_state()
    except Exception:
        enrich = {"running": False}

    counts: dict = {}
    if db is not None:
        try:
            counts = compute_enrich_counts(db)
        except Exception:
            counts = {}

    return {
        "scan": scan,
        "enrich": enrich,
        "enrich_counts": counts,
        "system": {"watcher_paused": runtime.watcher_is_paused(), "enrich_paused": bool(is_enrich_paused())},
        "ts": int(time.time()),
    }
