from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from typing import Iterable

from sqlalchemy.orm import Session
from sqlalchemy import or_

from ..db import models


def _normalize(text: str) -> str:
    try:
        s = unicodedata.normalize("NFD", str(text or "").lower())
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        s = re.sub(r"[^a-z0-9]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(text or "").lower().strip()


def _score(query: str, target: str) -> float:
    if not query or not target:
        return 0.0
    if query in target:
        return 1.2 + min(0.4, len(query) / max(1, len(target)))
    return SequenceMatcher(None, query, target).ratio()


def suggest_media(
    *,
    db: Session,
    query: str,
    limit: int = 20,
    types: Iterable[str] | None = None,
    max_candidates: int = 500,
) -> list[int]:
    qn = _normalize(query)
    if len(qn) < 2:
        return []
    tokens = [t for t in qn.split(" ") if len(t) >= 2]
    if not tokens:
        return []

    q = db.query(models.MediaItem)
    if types:
        cond = None
        for t in [str(x).strip().lower() for x in types if x]:
            c = (models.MediaItem.media_type.ilike(t)) | (models.MediaItem.library_type.ilike(t))
            cond = c if cond is None else (cond | c)
        if cond is not None:
            q = q.filter(cond)

    like_conds = []
    for tok in tokens[:4]:
        s = f"%{tok}%"
        like_conds.append(models.MediaItem.title.ilike(s))
        like_conds.append(models.MediaItem.base_title.ilike(s))
        like_conds.append(models.MediaItem.canonical_path.ilike(s))
    if like_conds:
        q = q.filter(or_(*like_conds))

    try:
        candidates = q.order_by(models.MediaItem.id.desc()).limit(max_candidates).all()
    except Exception:
        candidates = []

    scored = []
    for it in candidates:
        try:
            title = getattr(it, "title", None) or getattr(it, "base_title", None) or ""
            tn = _normalize(title)
            s = _score(qn, tn)
            if s <= 0:
                continue
            scored.append((s, int(it.id)))
        except Exception:
            continue
    scored.sort(key=lambda x: x[0], reverse=True)
    return [mid for _, mid in scored[: max(1, int(limit))]]
