import os
from pathlib import Path
import logging
from typing import Iterable
from ..core.config import get
from ..db import SessionLocal, init_db
from ..db.models import FileRecord, MediaItem, Base, MediaImage, Series, Season, Episode, SeasonItem, EpisodeFile
from ..db.models import MediaRoot
import requests
import json
from pathlib import PurePath
from ..utils.utils import normalize_folder_name_for_search
import json
import re
import time
from ..services.scan_state import set_state as set_scan_state, get_state as get_scan_state, clear_state as clear_scan_state
from ..utils.media_exts import COMMON_VIDEO_EXTS, get_media_extensions, resolve_media_extensions
from ..utils.path_hash import hash_path

logger = logging.getLogger(__name__)

# Avoid creating/migrating DB schema at import time.
# The API startup runs explicit migrations + init_db; CLI tools can call init_db when needed.
try:
    eager = str(os.environ.get("ARCANEA_EAGER_INIT_DB", "0")).strip().lower() in ("1", "true", "yes", "on")
    if eager:
        init_db(Base)
except Exception:
    pass


class Scanner:
    """Scanner walks folders, detects media files and inserts/updates DB records.

    No providers, no parsing beyond normalization of path.
    """

    def __init__(self, db_session_factory=None, media_extensions: Iterable[str] | None = None):
        self.db_session_factory = db_session_factory or SessionLocal
        self._common_video_exts = set(COMMON_VIDEO_EXTS)
        if media_extensions is not None:
            self.media_exts = set(resolve_media_extensions(media_extensions))
        else:
            self.media_exts = set(get_media_extensions())
        # configured roots where media lives
        # Keep simple list of root Paths for compatibility with watcher and other
        # consumers, but also attempt to read richer media_roots entries from
        # DATA_DIR/app_config.json so a root can optionally provide a library type.
        self.roots = [Path(r).resolve() for r in (get('media_roots') or [])]
        # Read optional typed roots from app_config.json; map resolved_path -> type
        self._root_types = {}
        try:
            from ..core.config import DATA_DIR
            import json as _json
            app_cfg_path = DATA_DIR / 'app_config.json'
            if app_cfg_path.exists():
                with open(app_cfg_path, 'r', encoding='utf-8') as fh:
                    _app_cfg = _json.load(fh) or {}
                for entry in (_app_cfg.get('media_roots') or []):
                    try:
                        if isinstance(entry, dict):
                            p = Path(entry.get('path') or '').resolve()
                            t = entry.get('type') or entry.get('library_type') or None
                            if p and t:
                                self._root_types[str(p)] = str(t).lower()
                        elif isinstance(entry, str):
                            # allow string entries like "E:\\Peliculas|movie"
                            s = str(entry)
                            if '|' in s:
                                parts = s.split('|', 1)
                                try:
                                    p = Path(parts[0]).resolve()
                                    t = parts[1]
                                    if p and t:
                                        self._root_types[str(p)] = str(t).lower()
                                except Exception:
                                    pass
                    except Exception:
                        continue
        except Exception:
            pass
        # ensure configured media_roots are persisted in DB (path + optional type)
        try:
            self._ensure_media_roots_persisted()
        except Exception:
            pass
        # Merge DB-persisted roots so scans/watchers honor newly added paths
        try:
            self._merge_roots_from_db()
        except Exception:
            pass

    def _ensure_media_roots_persisted(self):
        session = self.db_session_factory()
        try:
            # Upsert each configured root path and optionally its type
            for r in self.roots:
                try:
                    rp = str(Path(r).resolve())
                    existing = session.query(MediaRoot).filter(MediaRoot.path == rp).first()
                    if existing:
                        # update type if we have richer info
                        t = self._root_types.get(rp)
                        if t and getattr(existing, 'type', None) != t:
                            existing.type = t
                            existing.source = 'auto'
                            session.add(existing)
                    else:
                        t = self._root_types.get(rp)
                        mr = MediaRoot(path=rp, type=t, source='auto')
                        session.add(mr)
                except Exception:
                    continue
            session.commit()
        finally:
            session.close()

    def _merge_roots_from_db(self):
        session = self.db_session_factory()
        try:
            try:
                rows = session.query(MediaRoot).all()
            except Exception:
                rows = []
            existing = set()
            try:
                existing = {str(r) for r in self.roots}
            except Exception:
                existing = set()
            for row in rows:
                try:
                    rp_raw = getattr(row, "path", None)
                    if not rp_raw:
                        continue
                    try:
                        rp = str(Path(rp_raw).resolve())
                    except Exception:
                        rp = os.path.abspath(str(rp_raw))
                    if rp and rp not in existing:
                        self.roots.append(Path(rp))
                        existing.add(rp)
                    t = getattr(row, "type", None)
                    if t:
                        self._root_types[rp] = str(t).lower()
                except Exception:
                    continue
        finally:
            session.close()

    def is_media_file(self, path: str) -> bool:
        p = Path(path)
        return p.is_file() and p.suffix.lower() in self.media_exts

    def normalize_path(self, path: str) -> str:
        return str(Path(path).resolve())

    def _normcase_path(self, path: str) -> str:
        # Use Windows-style case-insensitive comparisons when available.
        try:
            return os.path.normcase(os.path.abspath(str(path)))
        except Exception:
            return os.path.normcase(str(path))

    def _is_path_under_root(self, path: str, root: str) -> bool:
        try:
            p = self._normcase_path(path)
            r = self._normcase_path(root).rstrip("\\/")
            if not r:
                return False
            return p == r or p.startswith(r + os.sep)
        except Exception:
            return False

    def _best_matching_root_from_candidates(self, path: str, roots: Iterable[str]) -> str | None:
        best = None
        best_len = -1
        for r in roots:
            try:
                if not r:
                    continue
                if not self._is_path_under_root(path, r):
                    continue
                rl = len(self._normcase_path(r).rstrip("\\/"))
                if rl > best_len:
                    best_len = rl
                    best = r
            except Exception:
                continue
        return best

    def _best_matching_root(self, path: str) -> str | None:
        return self._best_matching_root_from_candidates(path, [str(r) for r in self.roots])

    def scan_path(self, path: str):
        """Scan a single path (file or directory). Insert or update `file_record` and a placeholder `media_item` if missing."""
        p = Path(path)
        did_set_state = False
        try:
            # Avoid overriding a full scan in progress
            try:
                st = get_scan_state()
            except Exception:
                st = {}
            if st.get('status') != 'scanning':
                set_scan_state({'status': 'scanning', 'current': str(p), 'started_at': int(__import__('time').time())})
                did_set_state = True
        except Exception:
            pass
        try:
            # Only process paths under configured media_roots
            try:
                if not self._is_under_roots(p):
                    return
            except Exception:
                return

            # If directory: walk directories and handle each directory that contains media files
            if p.is_dir():
                for root, dirs, files in os.walk(p):
                    try:
                        media_files = []
                        for f in files:
                            try:
                                full = os.path.join(root, f)
                                if self.is_media_file(full):
                                    media_files.append(full)
                            except Exception:
                                logger.exception("Error checking file %s in %s", f, root)
                        if not media_files and files:
                            try:
                                if scan_debug:
                                    ext_hits = []
                                    for f in files:
                                        try:
                                            ext = Path(f).suffix.lower()
                                        except Exception:
                                            ext = ""
                                        if ext in self._common_video_exts and ext not in self.media_exts:
                                            ext_hits.append(ext)
                                    if ext_hits:
                                        sample = ", ".join(sorted(set(ext_hits))[:5])
                                        logger.debug("[scan-debug] folder skipped (no matching media_exts) folder=%s missing_exts=%s", root, sample)
                            except Exception:
                                pass
                        if media_files:
                            # update current folder being processed for frontend
                            try:
                                if did_set_state:
                                    set_scan_state({'current': str(root)})
                                    # increment processed counter if available
                                    st = get_scan_state()
                                    processed = int(st.get('processed') or 0) + 1
                                    set_scan_state({'processed': processed})
                            except Exception:
                                pass
                            self._upsert_folder(root, media_files)
                    except Exception:
                        logger.exception("Error scanning folder %s", root)
                return
            # If single file: treat its parent folder as the unit
            if p.is_file():
                fp = str(p)
                if self.is_media_file(fp):
                    parent = str(Path(fp).parent)
                    # IMPORTANT: when triggered by the watcher, we may get only a single changed file.
                    # Always scan the full parent folder so media_type inference (series vs movie) remains stable
                    # and file_index ordering is correct.
                    try:
                        media_files = []
                        for name in os.listdir(parent):
                            try:
                                full = os.path.join(parent, name)
                                if self.is_media_file(full):
                                    media_files.append(full)
                            except Exception:
                                logger.exception("Error checking file %s in %s", name, parent)
                    except Exception:
                        media_files = [fp]
                    try:
                        if did_set_state:
                            set_scan_state({'current': parent})
                            st = get_scan_state()
                            processed = int(st.get('processed') or 0) + 1
                            set_scan_state({'processed': processed})
                    except Exception:
                        pass
                    self._upsert_folder(parent, media_files)
        finally:
            if did_set_state:
                try:
                    # mark idle when done
                    set_scan_state({'status': 'idle', 'current': None, 'finished_at': int(__import__('time').time())})
                except Exception:
                    pass

    def _upsert_file(self, fullpath: str):
        # keep for single-file convenience; delegate to folder upsert
        folder = str(Path(fullpath).parent)
        self._upsert_folder(folder, [fullpath])

    def _upsert_folder(
        self,
        folder: str,
        media_files: list[str],
        *,
        skip_grouping: bool = False,
        title_seed_override: str | None = None,
        media_type_override: str | None = None,
        canonical_path_override: str | None = None,
        force_season_number: int | None = None,
        series_base_title_override: str | None = None,
        force_new_item: bool = False,
    ):
        """Ensure a single MediaItem exists for `folder` and upsert FileRecord rows for `media_files`.

        Rules:
        - 1 folder => 1 MediaItem
        - stable base_title derived from folder name via `normalize_folder_name_for_search`
        - avoid duplicates on rescan by matching existing FileRecord entries in the same folder
        - do not call providers
        """
        norm_folder = str(Path(folder).resolve())
        canonical_folder = norm_folder
        if canonical_path_override:
            try:
                canonical_folder = str(Path(canonical_path_override).resolve())
            except Exception:
                canonical_folder = str(canonical_path_override)
        session = self.db_session_factory()
        try:
            t0 = time.perf_counter()
            scan_debug = str(os.environ.get("ARCANEA_SCAN_DEBUG", "")).strip().lower() in ("1", "true", "yes", "on", "debug")
            try:
                scan_debug_samples = int(os.environ.get("ARCANEA_SCAN_DEBUG_SAMPLES", "0") or "0")
            except Exception:
                scan_debug_samples = 0
            if scan_debug and scan_debug_samples <= 0:
                scan_debug_samples = 3

            def parse_index(name: str):
                """Parse a sortable file_index from a filename stem.

                Goal: be robust against noisy release tags like `[BD][UF+]` and titles that contain
                numbers (e.g. "Part 2", "86", "S2") without mistaking those for the episode number.
                """
                try:
                    s = str(name or "")
                except Exception:
                    s = ""

                # Strip bracketed/parenthesized noise first (sites, group tags, hashes, etc.)
                try:
                    s_clean = re.sub(r"\[.*?\]", " ", s)
                    s_clean = re.sub(r"\(.*?\)", " ", s_clean)
                    s_clean = re.sub(r"\s+", " ", s_clean).strip()
                except Exception:
                    s_clean = s

                # common patterns: S01E02, s01e02
                m = re.search(r"(?i)\bS(\d{1,2})\s*E(\d{1,3})\b", s_clean)
                if m:
                    season = int(m.group(1))
                    ep = int(m.group(2))
                    return season * 1000 + ep

                # If we have an explicit season/part/cour marker, capture it and remove it
                season_num = None
                try:
                    m_season = re.search(r"(?i)\b(?:part|season|temporada|cour)\s*0*(\d{1,2})\b", s_clean)
                    if m_season:
                        season_num = int(m_season.group(1))
                        # Remove only the first match so the remaining string still contains episode tokens.
                        s_clean = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*0*\d{1,2}\b", " ", s_clean, count=1)
                        s_clean = re.sub(r"\s+", " ", s_clean).strip()
                except Exception:
                    season_num = None

                # patterns like Cap 05, Cap.05, CAP05
                m = re.search(r"(?i)\bcap(?:itulo)?\W*0*(\d{1,3})\b", s_clean)
                if m:
                    ep = int(m.group(1))
                    return (season_num * 1000 + ep) if season_num else ep

                # episode or ep 05
                m = re.search(r"(?i)\bep(?:isode)?\W*0*(\d{1,3})\b", s_clean)
                if m:
                    ep = int(m.group(1))
                    return (season_num * 1000 + ep) if season_num else ep

                # leading number patterns: 01 - Title or 01.Title or 01_Title
                m = re.search(r"^\s*0*(\d{1,3})\s*[-._]", s_clean)
                if m:
                    # Only treat a leading number as the episode when it's the *only* plausible number.
                    # This avoids mis-parsing titles like "86 - 01" (where 86 is part of the title).
                    try:
                        nums_all = re.findall(r"\b0*(\d{1,3})\b", s_clean)
                    except Exception:
                        nums_all = []
                    if len(nums_all) <= 1:
                        ep = int(m.group(1))
                        return (season_num * 1000 + ep) if season_num else ep

                # generic fallback: pick the *last* 1-3 digit number (avoids titles like "... Part 2 - 01")
                nums = re.findall(r"\b0*(\d{1,3})\b", s_clean)
                if nums:
                    try:
                        ep = int(nums[-1])
                    except Exception:
                        ep = None
                    if ep is not None:
                        return (season_num * 1000 + ep) if season_num else ep

                return None

            def _series_key_from_title(title: str) -> str:
                t = (title or "").strip()
                t = re.sub(r"\[.*?\]", " ", t)
                t = re.sub(r"\(.*?\)", " ", t)
                t = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*0*\d{1,2}\b", " ", t)
                t = re.sub(r"[_\\-]+", " ", t)
                t = re.sub(r"\s+", " ", t).strip().lower()
                return t

            def _infer_season_number(title: str | None, file_indexes: list[int | None]) -> int:
                if title:
                    try:
                        m = re.search(r"(?i)\b(?:part|season|temporada|cour)\s*0*(\d{1,2})\b", str(title))
                        if m:
                            n = int(m.group(1))
                            if n > 0:
                                return n
                    except Exception:
                        pass
                # if file_index encodes season (s*1000+ep), pick the most common season
                seasons = []
                for idx in file_indexes:
                    try:
                        if idx is None:
                            continue
                        i = int(idx)
                        if i >= 1000:
                            s = i // 1000
                            if s > 0:
                                seasons.append(s)
                    except Exception:
                        continue
                if seasons:
                    try:
                        return max(set(seasons), key=seasons.count)
                    except Exception:
                        return seasons[0]
                return 1

            def _episode_number_from_file_index(file_index: int | None) -> int | None:
                if file_index is None:
                    return None
                try:
                    i = int(file_index)
                except Exception:
                    return None
                if i >= 1000:
                    ep = i % 1000
                    return ep if ep > 0 else None
                return i if i > 0 else None

            def _clean_episode_title_from_filename(filename: str) -> str | None:
                base = Path(filename).stem
                base = re.sub(r"\[.*?\]", " ", base)
                base = re.sub(r"\(.*?\)", " ", base)
                base = re.sub(r"(?i)\b(?:cap(?:itulo)?|ep(?:isode)?)\b", " ", base)
                base = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*\d{1,2}\b", " ", base)
                base = re.sub(r"[_\\-]+", " ", base)
                base = re.sub(r"\s+", " ", base).strip()
                if not base:
                    return None
                # Avoid noisy titles like "01" / "12" when filenames are purely numeric.
                if re.fullmatch(r"\d{1,4}", base):
                    return None
                return base

            # Find any existing FileRecord in this folder to link to its MediaItem
            match = None
            if not force_new_item:
                try:
                    # Use SQL LIKE to match paths starting with folder
                    # Canonical folder for this MediaItem is the actual folder being processed.
                    # We want 1 folder => 1 media_item (lightweight, deterministic), even when series have subfolders
                    # like "Temporada 2". Higher-level grouping is handled via normalized series/season tables.
                    matched_root = self._best_matching_root(norm_folder)

                    like_pattern = f"{canonical_folder}%"
                    match = session.query(FileRecord).filter(FileRecord.path.like(like_pattern)).first()
                except Exception:
                    match = None

            mi = None
            # Stable type inference: decide early from actual files present in the folder.
            # This avoids temporary misclassification while scan/enrichment overlap.
            inferred_media_type = media_type_override or ('series' if len(media_files) > 1 else 'movie')
            try:
                inferred_media_type = str(inferred_media_type).lower()
            except Exception:
                pass

            def _infer_library_type(folder_path: str, media_files: list[str], inferred_type: str) -> str | None:
                try:
                    if inferred_type == 'movie':
                        return 'movie'
                    path_lower = str(folder_path or '').lower()
                    anime_keywords = ['anime', 'animes', 'animé', 'アニメ', '动漫', '動畫']
                    if any(k in path_lower for k in anime_keywords):
                        return 'anime'
                    anime_hits = 0
                    total = 0
                    for fp in media_files:
                        try:
                            name = Path(fp).name
                        except Exception:
                            name = str(fp)
                        total += 1
                        if re.match(r'^\[.+?\]', name):
                            anime_hits += 1
                            continue
                        if re.search(r'[\u3040-\u30ff\u4e00-\u9fff]', name):
                            anime_hits += 1
                            continue
                        if re.search(r'(?i)\b(ova|ona|bd|bdrip|bluray|webrip|web-dl|sub|subs|dual|lat)\b', name):
                            anime_hits += 1
                            continue
                    if total > 0 and (anime_hits / total) >= 0.4:
                        return 'anime'
                    return 'series'
                except Exception:
                    return None

            def _extract_series_title_from_filename(filename: str) -> str | None:
                try:
                    base = Path(filename).stem
                except Exception:
                    base = str(filename or "")
                if not base:
                    return None
                try:
                    s = re.sub(r"\[.*?\]", " ", base)
                    s = re.sub(r"\(.*?\)", " ", s)
                except Exception:
                    s = base
                try:
                    s = re.sub(r"(?i)\b(480p|720p|1080p|2160p|4k|hdr|x264|x265|h264|h265|hevc|av1|aac|flac|dts|ac3|webrip|web[-_. ]dl|bluray|bdrip|dvdrip|hdrip|sub|subs|dual|latino|castellano|jap|jpn|eng|spa|es|en|multi)\b", " ", s)
                except Exception:
                    pass
                try:
                    s = re.sub(r"(?i)\bS\d{1,2}E\d{1,3}\b", " ", s)
                    s = re.sub(r"(?i)\b(?:cap(?:itulo)?|ep(?:isode)?|episode|episodio)\s*0*\d{1,3}\b", " ", s)
                    s = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*0*\d{1,2}\b", " ", s)
                    s = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", s)
                    s = re.sub(r"^\s*\d{1,3}\s*[-._]+\s*", " ", s)
                except Exception:
                    pass
                s = re.sub(r"[_\\-]+", " ", s)
                s = re.sub(r"\s+", " ", s).strip()
                if not s:
                    return None
                if re.fullmatch(r"\d{1,4}", s):
                    return None
                return s

            if not skip_grouping and media_files:
                folder_name = Path(norm_folder).name
                try:
                    folder_title = normalize_folder_name_for_search(folder_name)[0]
                except Exception:
                    folder_title = folder_name

                def _clean_title_for_match(name: str) -> str:
                    try:
                        s = str(name or "")
                    except Exception:
                        s = ""
                    if not s:
                        return ""
                    try:
                        s = re.sub(r"\[.*?\]", " ", s)
                        s = re.sub(r"\(.*?\)", " ", s)
                    except Exception:
                        pass
                    try:
                        s = re.sub(
                            r"(?i)\b(480p|720p|1080p|2160p|4k|hdr|x264|x265|h264|h265|hevc|av1|aac|flac|dts|ac3|webrip|web[-_. ]dl|bluray|bdrip|dvdrip|hdrip|sub|subs|dual|latino|castellano|jap|jpn|eng|spa|es|en|multi|español|espanol)\b",
                            " ",
                            s,
                        )
                    except Exception:
                        pass
                    try:
                        s = re.sub(r"(?i)\bS\d{1,2}E\d{1,3}\b", " ", s)
                        s = re.sub(r"(?i)\b(?:cap(?:itulo)?|ep(?:isode)?|episode|episodio)\s*0*\d{1,3}\b", " ", s)
                        s = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", s)
                        s = re.sub(r"^\s*\d{2,4}\s*[-._]+\s*", " ", s)
                        s = re.sub(r"^\s*\d{1,3}\s*[-._]+\s*", " ", s)
                        s = re.sub(r"(?i)\b(?!19\d{2}\b)(?!20\d{2}\b)\d{1,3}\b\s*$", " ", s)
                    except Exception:
                        pass
                    s = re.sub(r"[_\\-]+", " ", s)
                    s = re.sub(r"\s+", " ", s).strip()
                    return s

                def _is_generic_title(clean: str | None, raw: str | None) -> bool:
                    s = (clean or raw or "").strip().lower()
                    if not s:
                        return True
                    if re.fullmatch(r"\d{1,4}", s):
                        return True
                    if re.fullmatch(r"(?:e|ep|episode|cap|capitulo|episodio)?\s*\d{1,3}", s):
                        return True
                    if s in {"movie", "pelicula", "película", "video", "sample", "trailer"}:
                        return True
                    if re.fullmatch(r"(?:movie|pelicula|película|video)(?:\s*\d+)?", s):
                        return True
                    return False

                def _is_special_title(raw: str | None) -> bool:
                    s = str(raw or "")
                    return bool(re.search(r"(?i)\b(?:ova|ona|especial|special|movie)\b", s))

                file_infos = []
                for fp in media_files:
                    try:
                        stem = Path(fp).stem
                    except Exception:
                        stem = str(fp)
                    try:
                        stem_hint = stem
                        try:
                            stem_hint = re.sub(r"\[.*?\]", " ", stem_hint)
                            stem_hint = re.sub(r"\(.*?\)", " ", stem_hint)
                            stem_hint = re.sub(r"\s+", " ", stem_hint).strip()
                        except Exception:
                            stem_hint = stem
                        episode_hint = bool(
                            re.search(r"(?i)\bS\d{1,2}E\d{1,3}\b", stem_hint)
                            or re.search(r"(?i)\b(?:ep(?:isode)?|cap(?:itulo)?|episodio)\s*\d{1,3}\b", stem_hint)
                            or re.search(r"^\s*\d{1,3}\s*[-._]", stem_hint)
                            or re.search(r"(?i)\b(?!19\d{2}\b)(?!20\d{2}\b)\d{1,3}\b\s*$", stem_hint)
                        )
                    except Exception:
                        episode_hint = False
                    clean = _clean_title_for_match(stem)
                    clean = clean or stem
                    file_infos.append(
                        {
                            "path": fp,
                            "stem": stem,
                            "clean_title": clean,
                            "is_generic": _is_generic_title(clean, stem),
                            "file_index": parse_index(stem),
                            "episode_hint": episode_hint,
                            "is_special": _is_special_title(stem),
                        }
                    )

                total_files = len(file_infos)
                numeric_count = len([f for f in file_infos if f.get("episode_hint")])
                numeric_ratio = (numeric_count / total_files) if total_files else 0.0
                descriptive_titles = {
                    str(f.get("clean_title") or "").strip().lower()
                    for f in file_infos
                    if f.get("clean_title") and not f.get("is_generic")
                }
                descriptive_titles.discard("")

                lib_type_hint = None
                lib_type_source = None
                try:
                    media_root = self._best_matching_root(norm_folder)
                except Exception:
                    media_root = None
                if not media_root:
                    try:
                        db_sess = self.db_session_factory()
                        try:
                            rows = db_sess.query(MediaRoot).all()
                            candidates = []
                            for mr in rows:
                                mp = str(getattr(mr, 'path', '') or '')
                                if mp:
                                    candidates.append(mp)
                            media_root = self._best_matching_root_from_candidates(norm_folder, candidates)
                        finally:
                            db_sess.close()
                    except Exception:
                        pass
                try:
                    if media_root:
                        lib_type_hint = self._root_types.get(str(Path(media_root).resolve()))
                        if lib_type_hint:
                            lib_type_source = "root_cache"
                except Exception:
                    lib_type_hint = None
                    lib_type_source = None
                if not lib_type_hint and media_root:
                    try:
                        db_sess = self.db_session_factory()
                        try:
                            mr = db_sess.query(MediaRoot).filter(MediaRoot.path == str(Path(media_root).resolve())).first()
                            if mr and getattr(mr, 'type', None):
                                lib_type_hint = mr.type
                                lib_type_source = "db"
                        finally:
                            db_sess.close()
                    except Exception:
                        pass
                if not lib_type_hint:
                    try:
                        lib_type_hint = _infer_library_type(norm_folder, media_files, inferred_media_type)
                        if lib_type_hint:
                            lib_type_source = "inferred"
                    except Exception:
                        lib_type_hint = None

                series_mode = total_files > 0 and numeric_count >= 3 and numeric_ratio >= 0.6
                mixed_mode = len(descriptive_titles) >= 2 and not series_mode
                try:
                    lib_type_l = str(lib_type_hint or "").strip().lower()
                except Exception:
                    lib_type_l = ""
                if (
                    not series_mode
                    and lib_type_l in ("anime", "series", "tv", "show")
                    and total_files >= 2
                    and lib_type_source in ("root_cache", "db")
                ):
                    series_mode = True
                    mixed_mode = False

                if series_mode:
                    main_files = [f["path"] for f in file_infos if not f.get("is_special")]
                    special_files = [f["path"] for f in file_infos if f.get("is_special")]
                    if main_files:
                        self._upsert_folder(
                            norm_folder,
                            main_files,
                            skip_grouping=True,
                            media_type_override="series",
                        )
                    if special_files:
                        specials_title = f"{folder_title} Specials".strip()
                        specials_path = str(Path(norm_folder) / "__specials__")
                        self._upsert_folder(
                            norm_folder,
                            special_files,
                            skip_grouping=True,
                            media_type_override="series",
                            title_seed_override=specials_title,
                            canonical_path_override=specials_path,
                            force_season_number=0,
                            series_base_title_override=folder_title,
                        )
                    return

                if mixed_mode:
                    groups: dict[str, dict[str, list[str] | str]] = {}
                    for f in file_infos:
                        if f.get("is_generic") or not f.get("clean_title"):
                            key = "__folder__"
                            title = folder_title
                        else:
                            key = str(f.get("clean_title") or "").strip().lower()
                            title = str(f.get("clean_title") or "").strip()
                        group = groups.setdefault(key, {"title": title, "files": []})
                        group["files"].append(f["path"])  # type: ignore[union-attr]
                    group_list = list(groups.values())
                    try:
                        existing_title = None
                        if match is not None and getattr(match, "media_item_id", None):
                            mi_existing = session.query(MediaItem).filter(MediaItem.id == getattr(match, "media_item_id")).first()
                            if mi_existing is not None:
                                existing_title = str(getattr(mi_existing, "title", "") or getattr(mi_existing, "base_title", "") or "").strip().lower()
                    except Exception:
                        existing_title = None
                    if existing_title:
                        def _match_score(g):
                            try:
                                t = str(g.get("title") or "").strip().lower()
                            except Exception:
                                t = ""
                            if not t:
                                return 0
                            if t == existing_title:
                                return 3
                            if t in existing_title or existing_title in t:
                                return 2
                            return 1
                        try:
                            group_list.sort(key=lambda g: (-_match_score(g), -len(g.get("files") or []), str(g.get("title") or "")))
                        except Exception:
                            pass
                    else:
                        try:
                            group_list.sort(key=lambda g: (-len(g.get("files") or []), str(g.get("title") or "")))
                        except Exception:
                            pass

                    for idx, g in enumerate(group_list):
                        files = g.get("files") or []
                        if not files:
                            continue
                        canonical_override = str(Path(files[0]).resolve())
                        self._upsert_folder(
                            norm_folder,
                            list(files),
                            skip_grouping=True,
                            media_type_override="movie",
                            title_seed_override=str(g.get("title") or folder_title),
                            canonical_path_override=canonical_override,
                            force_new_item=(idx > 0),
                        )
                    return

                # Single-movie mode (possibly multiple qualities)
                title_choice = folder_title
                if descriptive_titles:
                    counts: dict[str, int] = {}
                    for f in file_infos:
                        if f.get("clean_title") and not f.get("is_generic"):
                            k = str(f.get("clean_title") or "").strip()
                            if not k:
                                continue
                            counts[k] = counts.get(k, 0) + 1
                    if counts:
                        title_choice = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]
                self._upsert_folder(
                    norm_folder,
                    [f["path"] for f in file_infos],
                    skip_grouping=True,
                    media_type_override="movie",
                    title_seed_override=title_choice,
                )
                return

            if canonical_folder:
                try:
                    media_root = self._best_matching_root(canonical_folder)
                    if not media_root:
                        try:
                            db_sess = self.db_session_factory()
                            try:
                                rows = db_sess.query(MediaRoot).all()
                                candidates = []
                                for mr in rows:
                                    mp = str(getattr(mr, 'path', '') or '')
                                    if mp:
                                        candidates.append(mp)
                                media_root = self._best_matching_root_from_candidates(canonical_folder, candidates)
                            finally:
                                db_sess.close()
                        except Exception:
                            pass
                    lib_type = None
                    try:
                        if media_root:
                            lib_type = self._root_types.get(str(Path(media_root).resolve()))
                    except Exception:
                        lib_type = None
                    if not lib_type and media_root:
                        try:
                            db_sess = self.db_session_factory()
                            try:
                                mr = db_sess.query(MediaRoot).filter(MediaRoot.path == str(Path(media_root).resolve())).first()
                                if mr and getattr(mr, 'type', None):
                                    lib_type = mr.type
                            finally:
                                db_sess.close()
                        except Exception:
                            pass
                    if not lib_type:
                        lib_type = _infer_library_type(canonical_folder, media_files, inferred_media_type)

                    if (
                        not skip_grouping
                        and str(os.environ.get("ARCANEA_LEGACY_MOVIE_GROUPING", "")).strip().lower() in ("1", "true", "yes", "on")
                        and str(lib_type or "").lower() == "movie"
                        and len(media_files) > 1
                    ):
                        try:
                            try:
                                from difflib import SequenceMatcher
                            except Exception:
                                SequenceMatcher = None
                            group_display: dict[str, str] = {}
                            grouped: dict[str, list[str]] = {}
                            def _best_group_key(candidate_key: str, existing_keys: list[str]) -> str | None:
                                if not candidate_key or not existing_keys:
                                    return None
                                if SequenceMatcher is None:
                                    return None
                                best_key = None
                                best_score = 0.0
                                try:
                                    tokens_c = set(candidate_key.split())
                                except Exception:
                                    tokens_c = set()
                                for ek in existing_keys:
                                    try:
                                        score = SequenceMatcher(None, candidate_key, ek).ratio()
                                    except Exception:
                                        score = 0.0
                                    try:
                                        tokens_e = set(ek.split())
                                        inter = len(tokens_c & tokens_e)
                                        union = len(tokens_c | tokens_e)
                                        jaccard = (inter / union) if union else 0.0
                                    except Exception:
                                        jaccard = 0.0
                                    if score > best_score:
                                        best_score = score
                                        best_key = ek
                                try:
                                    threshold = float(os.environ.get("ARCANEA_MOVIE_GROUP_SIM", "0.84") or 0.84)
                                except Exception:
                                    threshold = 0.84
                                try:
                                    jacc_min = float(os.environ.get("ARCANEA_MOVIE_GROUP_JACCARD", "0.8") or 0.8)
                                except Exception:
                                    jacc_min = 0.8
                                if not best_key or best_score < threshold:
                                    return None
                                try:
                                    tokens_e = set(best_key.split())
                                    inter = len(tokens_c & tokens_e)
                                    union = len(tokens_c | tokens_e)
                                    jaccard = (inter / union) if union else 0.0
                                except Exception:
                                    jaccard = 0.0
                                return best_key if jaccard >= jacc_min else None

                            for fp in media_files:
                                cand = _extract_series_title_from_filename(Path(fp).name) or Path(fp).stem
                                key = _series_key_from_title(cand) or cand.strip().lower() or Path(fp).stem.lower()
                                use_key = _best_group_key(key, list(grouped.keys())) or key
                                grouped.setdefault(use_key, []).append(fp)
                                if use_key not in group_display:
                                    group_display[use_key] = cand

                            for key, files in grouped.items():
                                group_title = group_display.get(key) or Path(files[0]).stem
                                mi_group = None
                                try:
                                    existing = session.query(FileRecord).filter(FileRecord.path.in_(files)).first()
                                except Exception:
                                    existing = None
                                if existing and getattr(existing, 'media_item_id', None):
                                    mi_group = session.query(MediaItem).filter(MediaItem.id == getattr(existing, 'media_item_id')).first()

                                if not mi_group:
                                    try:
                                        variants = normalize_folder_name_for_search(group_title)
                                        base_title = variants[0] if variants else group_title
                                    except Exception:
                                        base_title = group_title
                                    canonical_path = str(Path(files[0]).resolve())
                                    mi_group = MediaItem(
                                        title=base_title,
                                        media_type='movie',
                                        canonical_path=canonical_path,
                                        base_title=base_title,
                                        media_root=media_root,
                                        library_type='movie',
                                        is_identified=False,
                                    )
                                    h = hash_path(canonical_path)
                                    if h:
                                        setattr(mi_group, 'canonical_path_hash', h)
                                    session.add(mi_group)
                                    session.flush()
                                else:
                                    try:
                                        if (getattr(mi_group, 'media_type', None) or '').strip().lower() != 'movie':
                                            mi_group.media_type = 'movie'
                                        if not (getattr(mi_group, 'library_type', None) or '').strip():
                                            mi_group.library_type = 'movie'
                                        if not (getattr(mi_group, 'media_root', None) or '').strip() and media_root:
                                            mi_group.media_root = media_root
                                        session.add(mi_group)
                                    except Exception:
                                        pass

                                try:
                                    variants = []
                                    try:
                                        variants = normalize_folder_name_for_search(group_title)
                                    except Exception:
                                        variants = [group_title]
                                    extras = [group_title, Path(canonical_folder).name]
                                    seen = set()
                                    out = []
                                    for s in (variants + extras):
                                        if not s:
                                            continue
                                        ss = str(s).strip()
                                        if not ss:
                                            continue
                                        k = ss.lower()
                                        if k in seen:
                                            continue
                                        seen.add(k)
                                        out.append(ss)
                                    if out:
                                        try:
                                            existing_st = []
                                            if getattr(mi_group, "search_titles", None):
                                                existing_st = json.loads(getattr(mi_group, "search_titles") or "[]")
                                            for s in existing_st:
                                                if not s:
                                                    continue
                                                k = str(s).strip().lower()
                                                if k in seen:
                                                    continue
                                                seen.add(k)
                                                out.append(str(s).strip())
                                        except Exception:
                                            pass
                                        out = out[:12]
                                        mi_group.search_titles = json.dumps(out, ensure_ascii=False)
                                        session.add(mi_group)
                                except Exception:
                                    pass

                                batch_count = 0
                                batch_size = 50
                                try:
                                    batch_size = int(os.environ.get("ARCANEA_SCAN_BATCH_SIZE", "50") or 50)
                                except Exception:
                                    batch_size = 50
                                batch_size = max(1, batch_size)
                                for fp in files:
                                    try:
                                        norm = str(Path(fp).resolve())
                                        try:
                                            st = Path(norm).stat()
                                            size = st.st_size
                                            mtime = int(st.st_mtime)
                                        except Exception:
                                            size = None
                                            mtime = None
                                        existing = session.query(FileRecord).filter(FileRecord.path == norm).first()
                                        if existing:
                                            changed = False
                                            if getattr(existing, 'media_item_id', None) != getattr(mi_group, 'id', None):
                                                setattr(existing, 'media_item_id', getattr(mi_group, 'id', None))
                                                changed = True
                                            if size is not None and getattr(existing, 'size', None) != size:
                                                setattr(existing, 'size', size)
                                                changed = True
                                            if mtime is not None and getattr(existing, 'mtime', None) != mtime:
                                                setattr(existing, 'mtime', mtime)
                                                changed = True
                                            if changed:
                                                session.add(existing)
                                        else:
                                            fr = FileRecord(media_item_id=getattr(mi_group, 'id', None), path=norm, size=size, mtime=mtime, file_index=None)
                                            session.add(fr)
                                        batch_count += 1
                                        if batch_count % batch_size == 0:
                                            session.commit()
                                    except Exception:
                                        logger.exception("Failed upserting file %s in movie group %s", fp, group_title)
                                        session.rollback()
                                try:
                                    session.commit()
                                except Exception:
                                    session.rollback()
                        except Exception:
                            logger.exception("Failed splitting multi-movie folder %s", norm_folder)
                        return
                except Exception:
                    pass
            if match is not None and getattr(match, 'media_item_id', None):
                mi = session.query(MediaItem).filter(MediaItem.id == getattr(match, 'media_item_id')).first()

            # Parent identity lookup (for season subfolders): inherit TMDB identity and dedupe grouping.
            parent_mi = None
            parent_series = None
            try:
                cp = str(Path(canonical_folder).resolve())
                parent_path = str(Path(cp).parent.resolve())
                if parent_path and parent_path.lower() != cp.lower():
                    # Prefer exact canonical_path match; fall back to hash match.
                    parent_mi = session.query(MediaItem).filter(MediaItem.canonical_path == parent_path).first()
                    if not parent_mi:
                        ph = hash_path(parent_path)
                        if ph:
                            parent_mi = session.query(MediaItem).filter(MediaItem.canonical_path_hash == ph).first()
                if parent_mi is not None:
                    try:
                        psi = session.query(SeasonItem).filter(SeasonItem.media_item_id == parent_mi.id).first()
                    except Exception:
                        psi = None
                    if psi:
                        try:
                            pseason = session.query(Season).filter(Season.id == psi.season_id).first()
                        except Exception:
                            pseason = None
                        if pseason and getattr(pseason, "series_id", None) is not None:
                            try:
                                parent_series = session.query(Series).filter(Series.id == pseason.series_id).first()
                            except Exception:
                                parent_series = None
            except Exception:
                parent_mi = None
                parent_series = None

            if not mi:
                # create placeholder media item using normalized canonical folder name
                folder_path = Path(canonical_folder)
                folder_name = folder_path.name
                parent_name = folder_path.parent.name if folder_path.parent else ""
                # If the folder is a pure season marker (e.g. "Temporada 2", "Season 2", "Part 2"),
                # include the parent series name to avoid meaningless titles like just "Temporada 2".
                try:
                    season_only = bool(re.search(r"(?i)^(?:temporada|season|part|cour|temp(?:\\.|orada)?|s)\\s*0*\\d{1,2}$", folder_name.strip()))
                    specials_only = bool(re.search(r"(?i)^(?:especial(?:es)?|specials?|ovas?|onas?)$", folder_name.strip()))
                    if force_season_number == 0:
                        specials_only = True
                    if not specials_only and title_seed_override:
                        specials_only = bool(re.search(r"(?i)\b(?:especial(?:es)?|specials?|ova(?:s)?|ona(?:s)?)\b", title_seed_override))
                except Exception:
                    season_only = False
                    specials_only = False
                title_seed = title_seed_override or folder_name
                if not title_seed_override and (season_only or specials_only) and parent_name:
                    title_seed = f"{parent_name} {folder_name}".strip()
                try:
                    variants = normalize_folder_name_for_search(title_seed)
                    base_title = variants[0] if variants else title_seed
                except Exception:
                    base_title = title_seed
                # set canonical path and media_root
                media_root = self._best_matching_root(canonical_folder)
                # If not found in configured runtime roots, try persisted MediaRoot rows in DB
                if not media_root:
                    try:
                        db_sess = self.db_session_factory()
                        try:
                            rows = db_sess.query(MediaRoot).all()
                            candidates = []
                            for mr in rows:
                                mp = str(getattr(mr, 'path', '') or '')
                                if mp:
                                    candidates.append(mp)
                            media_root = self._best_matching_root_from_candidates(canonical_folder, candidates)
                        finally:
                            db_sess.close()
                    except Exception:
                        pass
                # Determine library_type from configured typed roots if available.
                lib_type = None
                try:
                    if media_root:
                        lib_type = self._root_types.get(str(Path(media_root).resolve()))
                except Exception:
                    lib_type = None
                # If lib_type not provided from app_config parsing, try persisted DB value
                if not lib_type and media_root:
                    try:
                        db_sess = self.db_session_factory()
                        try:
                            mr = db_sess.query(MediaRoot).filter(MediaRoot.path == str(Path(media_root).resolve())).first()
                            if mr and getattr(mr, 'type', None):
                                lib_type = mr.type
                        finally:
                            db_sess.close()
                    except Exception:
                        pass
                # If still unknown, infer from folder content/name to handle non-standard roots.
                if not lib_type:
                    lib_type = _infer_library_type(canonical_folder, media_files, inferred_media_type)

                mi = MediaItem(
                    title=base_title,
                    media_type=inferred_media_type,
                    canonical_path=str(Path(canonical_folder).resolve()),
                    base_title=base_title,
                    media_root=media_root,
                    library_type=lib_type,
                    is_identified=False,
                )
                # Inherit TMDB identity/media_type from parent series folder when scanning season subfolders.
                try:
                    if (season_only or specials_only) and parent_mi is not None:
                        pt = getattr(parent_mi, "tmdb_id", None)
                        if pt:
                            mi.tmdb_id = str(pt)
                            # Inherit media_type strictly (tv/series) to avoid TMDB re-search misclassifying as movie.
                            pmt = getattr(parent_mi, "media_type", None)
                            if pmt:
                                mi.media_type = pmt
                except Exception:
                    pass
                # Hash for lookup
                h = hash_path(canonical_folder)
                if h:
                    setattr(mi, 'canonical_path_hash', h)
                session.add(mi)
                session.flush()  # obtain id
            else:
                # Keep media_type consistent with the current folder contents.
                try:
                    cur = (getattr(mi, 'media_type', None) or '').strip().lower()
                    if inferred_media_type and cur != inferred_media_type:
                        mi.media_type = inferred_media_type
                        session.add(mi)
                except Exception:
                    pass
                # Backfill library_type for existing items with unknown type.
                try:
                    cur_lib = (getattr(mi, 'library_type', None) or '').strip().lower()
                    if not cur_lib:
                        inferred_lib = _infer_library_type(canonical_folder, media_files, inferred_media_type)
                        if inferred_lib:
                            mi.library_type = inferred_lib
                            session.add(mi)
                except Exception:
                    pass
                # Inherit TMDB identity/media_type from parent series folder when scanning season subfolders.
                try:
                    folder_name = Path(canonical_folder).name
                    season_only = bool(re.search(r"(?i)^(?:temporada|season|part|cour|temp(?:\\.|orada)?|s)\\s*0*\\d{1,2}$", folder_name.strip()))
                    specials_only = bool(re.search(r"(?i)^(?:especial(?:es)?|specials?|ovas?|onas?)$", folder_name.strip()))
                    if force_season_number == 0:
                        specials_only = True
                    if not specials_only and title_seed_override:
                        specials_only = bool(re.search(r"(?i)\b(?:especial(?:es)?|specials?|ova(?:s)?|ona(?:s)?)\b", title_seed_override))
                except Exception:
                    season_only = False
                    specials_only = False
                try:
                    if (season_only or specials_only) and parent_mi is not None and getattr(parent_mi, "tmdb_id", None):
                        mi.tmdb_id = str(getattr(parent_mi, "tmdb_id"))
                        pmt = getattr(parent_mi, "media_type", None)
                        if pmt:
                            mi.media_type = pmt
                        session.add(mi)
                except Exception:
                    pass
                # If base_title/title are too generic (e.g. just "Temporada 2"), refresh them from folder context.
                try:
                    cur_bt = str(getattr(mi, 'base_title', '') or '').strip()
                    cur_t = str(getattr(mi, 'title', '') or '').strip()
                    generic = bool(re.search(r"(?i)^(?:temporada|season|part|cour|temp(?:\\.|orada)?|s)\\s*0*\\d{1,2}$", cur_bt)) or bool(re.search(r"(?i)^(?:especial(?:es)?|specials?|ovas?|onas?)$", cur_bt))
                    if generic and base_title:
                        mi.base_title = base_title
                        if cur_t.lower() == cur_bt.lower() or not cur_t:
                            mi.title = base_title
                        session.add(mi)
                except Exception:
                    pass

            # Populate search_titles from folder name variants and filename hints
            try:
                def _extract_series_title_from_filename(filename: str) -> str | None:
                    try:
                        base = Path(filename).stem
                    except Exception:
                        base = str(filename or "")
                    if not base:
                        return None
                    # remove bracket/paren tags
                    try:
                        s = re.sub(r"\[.*?\]", " ", base)
                        s = re.sub(r"\(.*?\)", " ", s)
                    except Exception:
                        s = base
                    # remove common quality/codec/language tags
                    try:
                        s = re.sub(r"(?i)\b(480p|720p|1080p|2160p|4k|hdr|x264|x265|h264|h265|hevc|av1|aac|flac|dts|ac3|webrip|web[-_. ]dl|bluray|bdrip|dvdrip|hdrip|sub|subs|dual|latino|castellano|jap|jpn|eng|spa|es|en|multi)\b", " ", s)
                    except Exception:
                        pass
                    # remove season/episode markers and standalone years
                    try:
                        s = re.sub(r"(?i)\bS\d{1,2}E\d{1,3}\b", " ", s)
                        s = re.sub(r"(?i)\b(?:cap(?:itulo)?|ep(?:isode)?|episode|episodio)\s*0*\d{1,3}\b", " ", s)
                        s = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*0*\d{1,2}\b", " ", s)
                        s = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", s)
                        # leading numbers like "01 - Title"
                        s = re.sub(r"^\s*\d{1,3}\s*[-._]+\s*", " ", s)
                    except Exception:
                        pass
                    s = re.sub(r"[_\\-]+", " ", s)
                    s = re.sub(r"\s+", " ", s).strip()
                    if not s:
                        return None
                    if re.fullmatch(r"\d{1,4}", s):
                        return None
                    return s

                variants = []
                try:
                    variants = normalize_folder_name_for_search(Path(canonical_folder).name)
                except Exception:
                    variants = [Path(canonical_folder).name]
                # also include base_title and folder name raw
                extras = [base_title, Path(canonical_folder).name]

                # filename-derived series candidates (detect multiple titles in one folder)
                title_counts: dict[str, int] = {}
                title_display: dict[str, str] = {}
                try:
                    for fp in media_files:
                        cand = _extract_series_title_from_filename(Path(fp).name)
                        if not cand:
                            continue
                        key = _series_key_from_title(cand)
                        if not key:
                            continue
                        title_counts[key] = title_counts.get(key, 0) + 1
                        if key not in title_display:
                            title_display[key] = cand
                except Exception:
                    title_counts = {}
                    title_display = {}

                multi_candidates: list[str] = []
                if title_counts:
                    total = max(1, sum(title_counts.values()))
                    # keep candidates with meaningful presence
                    for k, c in sorted(title_counts.items(), key=lambda x: (-x[1], x[0])):
                        if c >= 2 and (c / total) >= 0.2:
                            t = title_display.get(k) or k
                            multi_candidates.append(t)
                    # If we detect more than one plausible title, log it for visibility.
                    if len(multi_candidates) >= 2:
                        try:
                            logger.info(
                                "Scanner detected multiple title groups in folder=%s titles=%s",
                                norm_folder,
                                multi_candidates,
                            )
                        except Exception:
                            pass

                # unique preserve order
                seen = set()
                out = []
                for s in (variants + extras + multi_candidates):
                    if not s:
                        continue
                    ss = str(s).strip()
                    if not ss:
                        continue
                    k = ss.lower()
                    if k in seen:
                        continue
                    seen.add(k)
                    out.append(ss)

                if out:
                    # merge with existing search_titles if present
                    try:
                        existing = []
                        if getattr(mi, "search_titles", None):
                            existing = json.loads(getattr(mi, "search_titles") or "[]")
                        for s in existing:
                            if not s:
                                continue
                            k = str(s).strip().lower()
                            if k in seen:
                                continue
                            seen.add(k)
                            out.append(str(s).strip())
                    except Exception:
                        pass
                    # cap list size to avoid bloating
                    out = out[:12]
                    mi.search_titles = json.dumps(out, ensure_ascii=False)
                    session.add(mi)
                    session.commit()
                    # Do NOT call any providers or attempt deterministic resolution here.
                    # Scanner's responsibility is only to detect and register folders/files.
                    # Any provider resolution or enrichment must be performed by the
                    # enrichment dispatcher/service.
            except Exception:
                session.rollback()

            # Upsert file records for each media file in this folder
            t_files0 = time.perf_counter()
            batch_size = 50
            try:
                batch_size = int(os.environ.get("ARCANEA_SCAN_BATCH_SIZE", "50") or 50)
            except Exception:
                batch_size = 50
            batch_size = max(1, batch_size)
            batch_count = 0
            for fp in media_files:
                try:
                    norm = str(Path(fp).resolve())
                    try:
                        st = Path(norm).stat()
                        size = st.st_size
                        mtime = int(st.st_mtime)
                    except Exception:
                        size = None
                        mtime = None

                    existing = session.query(FileRecord).filter(FileRecord.path == norm).first()
                    if existing:
                        changed = False
                        if getattr(existing, 'media_item_id', None) != getattr(mi, 'id', None):
                            setattr(existing, 'media_item_id', getattr(mi, 'id', None))
                            changed = True
                        if size is not None and getattr(existing, 'size', None) != size:
                            setattr(existing, 'size', size)
                            changed = True
                        if mtime is not None and getattr(existing, 'mtime', None) != mtime:
                            setattr(existing, 'mtime', mtime)
                            changed = True
                        # extract index from filename if possible and update
                        try:
                            idx = parse_index(Path(norm).stem)
                            if idx is not None and getattr(existing, 'file_index', None) != idx:
                                setattr(existing, 'file_index', idx)
                                changed = True
                        except Exception:
                            pass
                        if changed:
                            session.add(existing)
                    else:
                        # determine index by trailing digits before extension
                        idx = None
                        try:
                            idx = parse_index(Path(norm).stem)
                        except Exception:
                            idx = None
                        fr = FileRecord(media_item_id=getattr(mi, 'id', None), path=norm, size=size, mtime=mtime, file_index=idx)
                        session.add(fr)
                    batch_count += 1
                    if batch_count % batch_size == 0:
                        session.commit()
                except Exception:
                    logger.exception("Failed upserting file %s in folder %s", fp, norm_folder)
                    session.rollback()
            try:
                session.commit()
            except Exception:
                session.rollback()
            t_files1 = time.perf_counter()

            # Detect MAL id files (id_mal.txt, mal_id.json, *.mal.json) and store mal_id on MediaItem
            try:
                def detect_mal_id(folder_path: str):
                    # common filenames
                    candidates = ['id_mal.txt', 'mal_id.txt', 'mal_id.json', 'id_mal.json']
                    p = Path(folder_path)
                    for name in candidates:
                        fp = p / name
                        if fp.exists() and fp.is_file():
                            try:
                                text = fp.read_text(encoding='utf-8').strip()
                                # if JSON, try parse
                                if text.startswith('{') or text.startswith('['):
                                    try:
                                        j = json.loads(text)
                                        # common key names
                                        for k in ('mal_id', 'id_mal', 'id'):
                                            if k in j:
                                                return str(j[k])
                                    except Exception:
                                        pass
                                # otherwise numeric or with prefix
                                m = re.search(r'(\d+)', text)
                                if m:
                                    return m.group(1)
                            except Exception:
                                continue
                    # also try any .mal.json files
                    for fp in p.glob('*.mal.json'):
                        try:
                            j = json.loads(fp.read_text(encoding='utf-8'))
                            for k in ('mal_id', 'id_mal', 'id'):
                                if k in j:
                                    return str(j[k])
                        except Exception:
                            continue
                    return None

                mal = detect_mal_id(norm_folder)
                if mal:
                    mi.mal_id = str(mal)
                    session.add(mi)
                    session.commit()
            except Exception:
                logger.exception("Failed detecting mal_id for %s", norm_folder)

            # Image handling: prefer local image files in the folder, otherwise fetch provider image later
            try:
                def find_local_image(folder_path: str):
                    p = Path(folder_path)
                    for ext in ('*.jpg','*.jpeg','*.png','*.webp','*.gif'):
                        for fp in p.glob(ext):
                            # prefer filenames like cover.jpg, poster.jpg
                            name = fp.name.lower()
                            if 'cover' in name or 'poster' in name or 'folder' in name:
                                return str(fp.resolve())
                    # fallback to any image
                    for ext in ('*.jpg','*.jpeg','*.png','*.webp','*.gif'):
                        for fp in p.glob(ext):
                            return str(fp.resolve())
                    return None

                local_img = find_local_image(norm_folder)
                if local_img:
                    # create MediaImage entry
                    try:
                        existing_img = session.query(MediaImage).filter(MediaImage.media_item_id == mi.id, MediaImage.local_path == local_img).first()
                        if not existing_img:
                            mi_img = MediaImage(media_item_id=mi.id, source='local', local_path=local_img, priority=10)
                            session.add(mi_img)
                            # also set poster_path on media item if not set
                            if not getattr(mi, 'poster_path', None):
                                mi.poster_path = local_img
                                session.add(mi)
                            session.commit()
                    except Exception:
                        session.rollback()
                else:
                    # no local image found; leave for provider-based download during enrichment
                    pass
            except Exception:
                logger.exception("Failed image detection for %s", norm_folder)

            # Assign logical file_index/order for files in this folder.
            try:
                t_index0 = time.perf_counter()
                # Fetch current file records for this media item
                files = session.query(FileRecord).filter(FileRecord.media_item_id == mi.id).all()
                parsed = []
                for f in files:
                    fname = Path(getattr(f, 'path', '')).name
                    idx = parse_index(Path(fname).stem)
                    parsed.append((f, idx))

                # if any parsed indexes present, assign them; otherwise sort by filename and enumerate
                have = [p for p in parsed if p[1] is not None]
                if have:
                    # assign parsed indexes; for duplicates, fallback to ordering
                    used = set()
                    next_auto = 1
                    for f, idx in sorted(parsed, key=lambda x: (x[1] is None, x[1] or 0, Path(x[0].path).name)):
                        if idx is not None and idx not in used:
                            f.file_index = int(idx)
                            used.add(int(idx))
                        else:
                            # assign next available
                            while next_auto in used:
                                next_auto += 1
                            f.file_index = next_auto
                            used.add(next_auto)
                            next_auto += 1
                else:
                    # none parsed, sort by filename
                    for i, (f, _) in enumerate(sorted(parsed, key=lambda x: Path(x[0].path).name), start=1):
                        f.file_index = i

                for f, _ in parsed:
                    session.add(f)
                session.commit()
                if scan_debug:
                    try:
                        # Sample a few parsed results (avoid log spam).
                        samp = []
                        for f, idx in parsed[: max(0, scan_debug_samples)]:
                            samp.append({"file": Path(getattr(f, "path", "") or "").name, "file_index": idx})
                        logger.debug("[scan-debug] file_index sample folder=%s media_item=%s sample=%s", norm_folder, mi.id, samp)
                    except Exception:
                        pass
                t_index1 = time.perf_counter()
            except Exception:
                logger.exception("Failed assigning file_index for folder %s", norm_folder)
                session.rollback()

            # Maintain normalized series/season/episode tables for this media item (JOIN-friendly).
            try:
                t_norm0 = time.perf_counter()
                # Fetch file records again (now with stable file_index)
                files = session.query(FileRecord).filter(FileRecord.media_item_id == mi.id).all()
                file_indexes = [getattr(f, "file_index", None) for f in files]
                file_record_ids = []
                try:
                    file_record_ids = [int(getattr(f, "id")) for f in files if getattr(f, "id", None) is not None]
                except Exception:
                    file_record_ids = []

                # Upsert Series (key stored in series.provider_id for internal grouping; not exposed directly).
                #
                # `mi.title` can be season-specific (e.g. "Halo - Temporada 2") after TMDB season enrichment.
                # For series grouping we want the base show title ("Halo"), while the Season row keeps the
                # season-specific title/number. Otherwise Season 2 can incorrectly become its own Series.
                season_title_seed = (
                    getattr(mi, "title_localized", None)
                    or getattr(mi, "title_en", None)
                    or getattr(mi, "title", None)
                    or getattr(mi, "base_title", None)
                    or Path(getattr(mi, "canonical_path", "") or norm_folder).name
                    or "Unknown"
                )
                season_title_seed = str(season_title_seed or "").strip() or "Unknown"
                specials_mode = False
                try:
                    if force_season_number == 0:
                        specials_mode = True
                    elif re.search(r"(?i)\b(?:especial(?:es)?|specials?|ova(?:s)?|ona(?:s)?)\b", season_title_seed):
                        specials_mode = True
                except Exception:
                    specials_mode = False

                def _series_base_title_for_display(title: str) -> str:
                    t = (title or "").strip()
                    t = re.sub(r"\[.*?\]", " ", t)
                    t = re.sub(r"\(.*?\)", " ", t)
                    # remove explicit season markers
                    t = re.sub(r"(?i)\b(?:part|season|temporada|cour)\s*0*\d{1,2}\b", " ", t)
                    if specials_mode:
                        t = re.sub(r"(?i)\b(?:especial(?:es)?|specials?|ova(?:s)?|ona(?:s)?|movie)\b", " ", t)
                    t = re.sub(r"[_\\-]+", " ", t)
                    t = re.sub(r"\s+", " ", t).strip()
                    return t

                series_base_title = series_base_title_override or (_series_base_title_for_display(season_title_seed) or season_title_seed)
                # If we have a parent media item (series root) but no parent series row yet (scan order),
                # derive the base title from the parent to keep the series_key stable.
                try:
                    if parent_mi is not None and parent_series is None:
                        pt_seed = (
                            getattr(parent_mi, "title_localized", None)
                            or getattr(parent_mi, "title_en", None)
                            or getattr(parent_mi, "title", None)
                            or getattr(parent_mi, "base_title", None)
                            or ""
                        )
                        pt_seed = str(pt_seed or "").strip()
                        if pt_seed:
                            series_base_title = _series_base_title_for_display(pt_seed) or series_base_title
                except Exception:
                    pass
                # Forced dedupe: if parent folder already has a series_id, always attach children to it.
                series = parent_series
                if series is None:
                    series_key = _series_key_from_title(series_base_title) or series_base_title.strip().lower() or "unknown"
                    series = session.query(Series).filter(Series.provider_id == series_key).first()
                if not series:
                    series = Series(
                        title=series_base_title or "Unknown",
                        provider_id=(_series_key_from_title(series_base_title) or series_base_title.strip().lower() or "unknown"),
                        title_en=series_base_title or None,
                    )
                    session.add(series)
                    session.flush()
                else:
                    try:
                        # Keep anchor title populated for deterministic fallback (best-effort).
                        if not getattr(series, "title_en", None) and series_base_title:
                            series.title_en = series_base_title
                            session.add(series)
                    except Exception:
                        pass

                # Upsert Season and SeasonItem for this media item.
                si = session.query(SeasonItem).filter(SeasonItem.media_item_id == mi.id).first()
                season_number = force_season_number if force_season_number is not None else _infer_season_number(season_title_seed, file_indexes)
                if si:
                    season = session.query(Season).filter(Season.id == si.season_id).first()
                    if not season:
                        season = Season(series_id=series.id, season_number=season_number, title_en=season_title_seed or None)
                        session.add(season)
                        session.flush()
                        si.season_id = season.id
                        session.add(si)
                    else:
                        changed = False
                        if getattr(season, "series_id", None) != series.id:
                            season.series_id = series.id
                            changed = True
                        if getattr(season, "season_number", None) != season_number:
                            season.season_number = season_number
                            changed = True
                        if changed:
                            session.add(season)
                else:
                    season = Season(series_id=series.id, season_number=season_number, title_en=season_title_seed or None)
                    session.add(season)
                    session.flush()
                    si = SeasonItem(season_id=season.id, media_item_id=mi.id)
                    session.add(si)

                session.flush()

                # Preserve any provider-enriched per-episode fields (localized title/synopsis) across rebuilds.
                # The scan may rerun frequently (watcher, manual scan). We rebuild Episode rows for filesystem
                # consistency, but we must not discard already-fetched TMDB localization.
                preserved_by_num: dict[int, dict[str, str | None]] = {}
                try:
                    rows_pres = (
                        session.query(
                            Episode.episode_number,
                            Episode.title,
                            Episode.title_en,
                            Episode.title_localized,
                            Episode.synopsis_localized,
                        )
                        .filter(Episode.season_id == season.id)
                        .all()
                    )
                    for n, t, ten, tloc, syn in rows_pres:
                        if n is None:
                            continue
                        try:
                            nn = int(n)
                        except Exception:
                            continue
                        preserved_by_num[nn] = {
                            "title": t,
                            "title_en": ten,
                            "title_localized": tloc,
                            "synopsis_localized": syn,
                        }
                except Exception:
                    preserved_by_num = {}

                # Ensure episode_file mappings for these files are clean before rebuilding.
                # `episode_file.file_record_id` is UNIQUE globally; if a file was previously linked to a different
                # Episode (e.g. due to prior season inference changes), we must remove that old link first.
                if file_record_ids:
                    try:
                        session.query(EpisodeFile).filter(EpisodeFile.file_record_id.in_(file_record_ids)).delete(synchronize_session=False)  # type: ignore[arg-type]
                        session.flush()
                    except Exception:
                        # If this fails, let the outer handler log it; do not partially rebuild.
                        raise

                # Rebuild Episode + EpisodeFile for this season (keeps it consistent with filesystem).
                # IMPORTANT: don't load full Episode objects before bulk deleting.
                # SQLite can reuse ROWIDs after deletes; if the old objects are still in the identity map,
                # SQLAlchemy emits "Identity map already had an identity..." warnings when new rows reuse ids.
                ep_ids = []
                try:
                    ep_ids = [int(r[0]) for r in session.query(Episode.id).filter(Episode.season_id == season.id).all()]
                except Exception:
                    ep_ids = []
                if ep_ids:
                    try:
                        session.query(EpisodeFile).filter(EpisodeFile.episode_id.in_(ep_ids)).delete(synchronize_session=False)  # type: ignore[arg-type]
                    except Exception:
                        pass
                try:
                    session.query(Episode).filter(Episode.season_id == season.id).delete(synchronize_session=False)
                except Exception:
                    pass
                session.flush()

                # Order by file_index if present; fall back to filename.
                rows = []
                for f in files:
                    try:
                        filename = Path(getattr(f, "path", "") or "").name
                    except Exception:
                        filename = str(getattr(f, "id", ""))
                    rows.append((f, _episode_number_from_file_index(getattr(f, "file_index", None)), filename))

                rows.sort(key=lambda x: (x[1] is None, x[1] if x[1] is not None else 0, x[2]))

                used = set()
                next_auto = 1
                for f, epn, filename in rows:
                    if epn is None or epn <= 0 or epn in used:
                        while next_auto in used:
                            next_auto += 1
                        epn = next_auto
                        next_auto += 1
                    used.add(int(epn))

                    def _is_generic_title(v: str | None) -> bool:
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

                    ep_title_from_name = _clean_episode_title_from_filename(filename)
                    preserved = preserved_by_num.get(int(epn), {}) if isinstance(epn, int) else {}
                    preserved_title = preserved.get("title") if isinstance(preserved, dict) else None
                    preserved_title_en = preserved.get("title_en") if isinstance(preserved, dict) else None

                    # Ensure we always have *something* to render even when filenames are purely numeric.
                    fallback_title_en = f"Episode {int(epn)}"
                    title_en_final = preserved_title_en if not _is_generic_title(preserved_title_en if isinstance(preserved_title_en, str) else None) else (ep_title_from_name or fallback_title_en)
                    title_final = preserved_title if not _is_generic_title(preserved_title if isinstance(preserved_title, str) else None) else (title_en_final or ep_title_from_name or fallback_title_en)

                    ep = Episode(
                        season_id=season.id,
                        episode_number=int(epn),
                        title=title_final,
                        title_en=title_en_final,
                        title_localized=(preserved.get("title_localized") if isinstance(preserved, dict) else None),
                        synopsis_localized=(preserved.get("synopsis_localized") if isinstance(preserved, dict) else None),
                    )
                    session.add(ep)
                    session.flush()
                    session.add(EpisodeFile(episode_id=ep.id, file_record_id=f.id))

                session.commit()
                t_norm1 = time.perf_counter()
                if scan_debug:
                    try:
                        logger.debug(
                            "[scan-debug] normalized sync folder=%s media_item=%s series_id=%s season_id=%s season_number=%s episodes=%s ms=%d",
                            norm_folder,
                            mi.id,
                            getattr(series, "id", None),
                            getattr(season, "id", None),
                            getattr(season, "season_number", None),
                            len(rows),
                            int((t_norm1 - t_norm0) * 1000),
                        )
                    except Exception:
                        pass
            except Exception:
                logger.exception("Failed syncing normalized episode tables for %s", norm_folder)
                session.rollback()

            if scan_debug:
                try:
                    t1 = time.perf_counter()
                    logger.debug(
                        "[scan-debug] folder done folder=%s media_item=%s files=%d upsert_ms=%d index_ms=%d total_ms=%d",
                        norm_folder,
                        getattr(mi, "id", None),
                        len(media_files),
                        int((t_files1 - t_files0) * 1000),
                        int((t_index1 - t_index0) * 1000) if "t_index1" in locals() else -1,
                        int((t1 - t0) * 1000),
                    )
                except Exception:
                    pass

            # Publish last-folder timing/summary in scan status (frontend can ignore if unused).
            try:
                set_scan_state(
                    {
                        "debug": {
                            "folder": norm_folder,
                            "media_item_id": getattr(mi, "id", None),
                            "files": int(len(media_files)),
                            "upsert_ms": int((t_files1 - t_files0) * 1000),
                            "total_ms": int((time.perf_counter() - t0) * 1000),
                        }
                    }
                )
            except Exception:
                pass

            logger.debug("Upserted folder %s with %d files (media_item=%s)", norm_folder, len(media_files), mi.id)
        except Exception:
            logger.exception("Failed processing folder %s", norm_folder)
            session.rollback()
        finally:
            session.close()

    def _is_under_roots(self, path: Path) -> bool:
        try:
            return self._best_matching_root(str(path.resolve())) is not None
        except Exception:
            return False
