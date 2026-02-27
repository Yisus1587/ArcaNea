"""Normalize existing DB into series/season/episode (+ bridges) tables.

Goal:
- Do NOT expose `media_metadata.data` blobs as the primary source for episodes.
- Populate normalized tables from existing `media_item` + `file_record` rows.
- Keep it safe to re-run (idempotent-ish): we rebuild derived tables.

Rules:
- Each `media_item` becomes exactly one `season` by default (lightweight scan).
- Episode ordering uses `file_record.file_index` when present.
- We link episodes to filesystem via `episode_file(file_record_id)`.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DB_PATH = Path("data") / "arcanea.db"


_SEASON_MARKER_RE = re.compile(r"(?i)\b(?:part|season|temporada|cour)\s*0*(\d{1,2})\b")


def _series_key_from_title(title: str) -> str:
    """Return a stable grouping key for 'series' rows from a folder-ish title."""
    t = (title or "").strip()
    # Remove bracket/paren noise and common season markers.
    t = re.sub(r"\[.*?\]", " ", t)
    t = re.sub(r"\(.*?\)", " ", t)
    t = _SEASON_MARKER_RE.sub(" ", t)
    t = re.sub(r"[_\-]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def _infer_season_number_from_media_item_title(title: str | None) -> int | None:
    if not title:
        return None
    try:
        m = _SEASON_MARKER_RE.search(str(title))
        if m:
            n = int(m.group(1))
            return n if n > 0 else None
    except Exception:
        return None
    return None


def _episode_number_from_file_index(file_index: int | None) -> tuple[int | None, int | None]:
    """Return (season_number, episode_number) from file_index conventions."""
    if file_index is None:
        return None, None
    try:
        i = int(file_index)
    except Exception:
        return None, None
    if i >= 1000:
        s = i // 1000
        e = i % 1000
        if s > 0 and e > 0:
            return s, e
    if i > 0:
        return None, i
    return None, None


def _clean_episode_title_from_filename(filename: str) -> str:
    base = Path(filename).stem
    base = re.sub(r"\[.*?\]", " ", base)
    base = re.sub(r"\(.*?\)", " ", base)
    base = re.sub(r"(?i)\b(?:cap(?:itulo)?|ep(?:isode)?)\b", " ", base)
    base = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*\d{1,2}\b", " ", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    if not base:
        return base
    # Avoid noisy titles like "01" / "12" when filenames are purely numeric.
    if re.fullmatch(r"\d{1,4}", base):
        return ""
    return base


def normalize(rebuild: bool = True) -> dict[str, Any]:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found at {DB_PATH}")

    # Ensure schema exists (episode_file/season_item tables, indexes, etc.)
    try:
        from .apply_db_migrations import main as apply_db_migrations_main

        apply_db_migrations_main()
    except Exception:
        # Best-effort: normalization will fail later if schema is missing.
        pass

    # Use a higher timeout to tolerate concurrent readers (e.g., API polling status endpoints).
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        try:
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        # Best-effort performance/safety pragmas for bulk rebuilds.
        # If these fail (e.g., due to permissions/locks), we still proceed.
        try:
            conn.execute("PRAGMA busy_timeout=5000;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass

        if rebuild:
            # Derived tables: rebuild from scratch.
            try:
                conn.execute("DELETE FROM episode_file;")
                conn.execute("DELETE FROM episode;")
                conn.execute("DELETE FROM season_item;")
                conn.execute("DELETE FROM season;")
                conn.execute("DELETE FROM series;")
                conn.commit()
            except sqlite3.OperationalError as e:
                # Common in Windows when the DB is open by the running server (.exe/uvicorn).
                msg = str(e).lower()
                if "disk i/o" in msg or "locked" in msg or "readonly" in msg or "permission" in msg:
                    raise RuntimeError(
                        "No se pudo escribir en la base de datos durante la normalización. "
                        "Cierra el servidor (.exe/uvicorn) y vuelve a intentar."
                    ) from e
                raise

        # Map series_key -> series_id
        series_map: dict[str, int] = {}
        created_series = 0

        def get_or_create_series(title: str) -> int:
            key = _series_key_from_title(title)
            if not key:
                key = (title or "").strip().lower() or "unknown"
            if key in series_map:
                return series_map[key]
            cur = conn.execute(
                "INSERT INTO series(title, provider_id, title_en) VALUES (?,?,?)",
                (title or "Unknown", key, (title or "Unknown")),
            )
            sid = int(cur.lastrowid)
            series_map[key] = sid
            nonlocal created_series
            created_series += 1
            return sid

        items = conn.execute("SELECT id, title, base_title, canonical_path FROM media_item ORDER BY id").fetchall()

        created_seasons = 0
        created_episodes = 0
        created_links = 0

        for it in items:
            media_item_id = int(it["id"])
            title = (it["title"] or it["base_title"] or Path(str(it["canonical_path"] or "")).name or f"Item {media_item_id}").strip()
            series_id = get_or_create_series(title)

            # Gather file records for this media item.
            files = conn.execute(
                "SELECT id, path, file_index FROM file_record WHERE media_item_id=? ORDER BY id",
                (media_item_id,),
            ).fetchall()
            if not files:
                continue

            # Ensure episode_file mappings for these file_record ids are clean (file_record_id is UNIQUE).
            try:
                file_ids = [int(f["id"]) for f in files if f["id"] is not None]
            except Exception:
                file_ids = []
            if file_ids:
                try:
                    qmarks = ",".join(["?"] * len(file_ids))
                    conn.execute(f"DELETE FROM episode_file WHERE file_record_id IN ({qmarks})", file_ids)
                except Exception:
                    pass

            # Infer season number:
            season_number = _infer_season_number_from_media_item_title(title)
            if season_number is None:
                # If file_index encodes season (s*1000+ep), use the most common season.
                seasons = []
                for f in files:
                    s, _e = _episode_number_from_file_index(f["file_index"])
                    if s:
                        seasons.append(s)
                if seasons:
                    season_number = max(set(seasons), key=seasons.count)
            if season_number is None:
                season_number = 1

            cur = conn.execute(
                "INSERT INTO season(series_id, season_number, title_en) VALUES (?,?,?)",
                (series_id, int(season_number), title),
            )
            season_id = int(cur.lastrowid)
            created_seasons += 1

            conn.execute(
                "INSERT INTO season_item(season_id, media_item_id, created_at) VALUES (?,?,CURRENT_TIMESTAMP)",
                (season_id, media_item_id),
            )
            created_links += 1

            # Build episodes from file_index; fallback to filename order.
            tmp = []
            for f in files:
                file_record_id = int(f["id"])
                path = str(f["path"] or "")
                filename = Path(path).name if path else str(file_record_id)
                _s2, epn = _episode_number_from_file_index(f["file_index"])
                tmp.append((epn, filename, file_record_id))

            # sort: epn if present, else filename
            tmp.sort(key=lambda x: (x[0] is None, x[0] if x[0] is not None else 0, x[1]))

            next_auto = 1
            used = set()
            for epn, filename, file_record_id in tmp:
                if epn is None or epn <= 0 or epn in used:
                    while next_auto in used:
                        next_auto += 1
                    epn = next_auto
                    next_auto += 1
                used.add(int(epn))

                title_ep = _clean_episode_title_from_filename(filename)
                conn.execute(
                    "INSERT INTO episode(season_id, episode_number, title, title_en) VALUES (?,?,?,?)",
                    (season_id, int(epn), title_ep or None, title_ep or None),
                )
                episode_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                created_episodes += 1

                conn.execute(
                    "INSERT INTO episode_file(episode_id, file_record_id, created_at) VALUES (?,?,CURRENT_TIMESTAMP)",
                    (episode_id, file_record_id),
                )
                created_links += 1

        conn.commit()
        return {
            "ok": True,
            "series": conn.execute("SELECT COUNT(1) FROM series").fetchone()[0],
            "seasons": conn.execute("SELECT COUNT(1) FROM season").fetchone()[0],
            "episodes": conn.execute("SELECT COUNT(1) FROM episode").fetchone()[0],
            "episode_files": conn.execute("SELECT COUNT(1) FROM episode_file").fetchone()[0],
        }
    finally:
        conn.close()


def main():
    r = normalize(rebuild=True)
    print(json.dumps(r, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
