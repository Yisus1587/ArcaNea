from __future__ import annotations

from typing import Iterable

from ..core.config import get as get_config

COMMON_VIDEO_EXTS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".m4v",
    ".wmv",
    ".webm",
    ".ts",
    ".m2ts",
}


def _normalize_ext(raw: str) -> str | None:
    try:
        s = str(raw or "").strip().lower()
    except Exception:
        return None
    if not s:
        return None
    if not s.startswith("."):
        s = "." + s
    return s


def resolve_media_extensions(raw: Iterable[str] | None) -> list[str]:
    merged = set(COMMON_VIDEO_EXTS)
    for e in raw or []:
        ne = _normalize_ext(e)
        if ne:
            merged.add(ne)
    return sorted(merged)


def get_media_extensions() -> list[str]:
    try:
        raw = get_config("media_extensions") or []
    except Exception:
        raw = []
    return resolve_media_extensions(raw)
