from __future__ import annotations

import datetime
from typing import Iterable

from ..db import models

# Tunable weights
WEIGHT_GENRE = 0.55
WEIGHT_RATING = 0.30
WEIGHT_RECENCY = 0.15
WATCHED_PENALTY = 0.1
SEED_LIMIT = 10
OUT_LIMIT = 20


def _parse_genres(raw: str | Iterable[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        return [g.strip().lower() for g in raw.split(",") if g.strip()]
    return []


def _recency_score(created_at: datetime.datetime | None) -> float:
    if not created_at:
        return 0.0
    now = datetime.datetime.utcnow()
    try:
        age_days = max(0.0, (now - created_at).total_seconds() / 86400.0)
    except Exception:
        return 0.0
    # 0..1, with a 30-day soft half-life
    return 1.0 / (1.0 + (age_days / 30.0))


def _rating_score(rating: float | None) -> float:
    if rating is None:
        return 0.0
    try:
        r = float(rating)
    except Exception:
        return 0.0
    # normalize 0..10 (tmdb/jikan). if 0..100, scale down.
    if r > 10:
        r = r / 10.0
    return max(0.0, min(1.0, r / 10.0))


def _genre_score(item_genres: list[str], seed_weights: dict[str, int]) -> float:
    if not item_genres or not seed_weights:
        return 0.0
    total = sum(seed_weights.values()) or 1
    score = 0.0
    for g in item_genres:
        score += seed_weights.get(g, 0)
    return score / total


def get_recommendations(db, *, profile_id: str | None = None, limit: int = OUT_LIMIT) -> list[models.MediaItem]:
    # Pull last played items
    ph_q = db.query(models.PlayHistory).filter(models.PlayHistory.media_item_id != None)
    if profile_id:
        ph_q = ph_q.filter(models.PlayHistory.profile_id == profile_id)
    ph = (
        ph_q.order_by(models.PlayHistory.last_played.desc(), models.PlayHistory.updated_at.desc())
        .limit(SEED_LIMIT)
        .all()
    )
    seed_ids = [int(x.media_item_id) for x in ph if getattr(x, "media_item_id", None) is not None]

    seed_weights: dict[str, int] = {}
    if seed_ids:
        seeds = db.query(models.MediaItem).filter(models.MediaItem.id.in_(seed_ids)).all()
        for s in seeds:
            for g in _parse_genres(getattr(s, "genres", None)):
                seed_weights[g] = seed_weights.get(g, 0) + 1

    items = db.query(models.MediaItem).all()
    scored: list[tuple[float, models.MediaItem]] = []
    for it in items:
        genres = _parse_genres(getattr(it, "genres", None))
        g_score = _genre_score(genres, seed_weights)
        r_score = _rating_score(getattr(it, "rating", None))
        c_score = _recency_score(getattr(it, "created_at", None))
        score = (WEIGHT_GENRE * g_score) + (WEIGHT_RATING * r_score) + (WEIGHT_RECENCY * c_score)

        # watched penalty
        watched = False
        try:
            ph_row_q = db.query(models.PlayHistory).filter(models.PlayHistory.media_item_id == it.id)
            if profile_id:
                ph_row_q = ph_row_q.filter(models.PlayHistory.profile_id == profile_id)
            ph_row = ph_row_q.first()
            watched = bool(getattr(ph_row, "watched", False)) if ph_row else False
        except Exception:
            watched = False
        if watched:
            score *= WATCHED_PENALTY

        scored.append((score, it))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [it for _, it in scored[: max(1, int(limit or OUT_LIMIT))]]


def record_play(db, *, media_item_id: int, profile_id: str | None = None) -> dict:
    now = datetime.datetime.utcnow()
    row_q = db.query(models.PlayHistory).filter(models.PlayHistory.media_item_id == media_item_id)
    if profile_id:
        row_q = row_q.filter(models.PlayHistory.profile_id == profile_id)
    row = row_q.first()
    if not row:
        row = models.PlayHistory(media_item_id=media_item_id, profile_id=profile_id)
        row.play_count = 0
    row.play_count = int(row.play_count or 0) + 1
    row.last_played = now
    # Mark watched on first play (simple heuristic)
    if row.play_count and row.play_count >= 1:
        row.watched = True
    row.updated_at = now
    db.add(row)
    try:
        db.commit()
    except Exception:
        db.rollback()
        return {"ok": False, "detail": "db_commit_failed"}
    return {"ok": True, "play_count": row.play_count, "watched": bool(row.watched)}
