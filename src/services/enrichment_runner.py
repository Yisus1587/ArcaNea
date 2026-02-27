import threading
import time
import logging
from typing import Optional

from ..db import SessionLocal
from ..db import models
from .enrichment import enrich_one
from ..core.config import get
from .enrich_state import set_state, clear_state, get_state as get_enrich_state
from . import enrichment as enrichment_service
from .localize_runner import start_localization_job

logger = logging.getLogger(__name__)

_running = False
_run_lock = threading.Lock()
_enrich_call_lock = threading.Lock()
_paused = threading.Event()
_stop = threading.Event()


def set_paused(paused: bool) -> None:
    try:
        if paused:
            _paused.set()
        else:
            _paused.clear()
        try:
            set_state({'paused': bool(_paused.is_set())})
        except Exception:
            pass
    except Exception:
        return


def is_paused() -> bool:
    try:
        return bool(_paused.is_set())
    except Exception:
        return False


def request_stop() -> None:
    try:
        _stop.set()
        try:
            set_state({'stop_requested': True})
        except Exception:
            pass
    except Exception:
        return


def clear_stop() -> None:
    try:
        _stop.clear()
        try:
            set_state({'stop_requested': False})
        except Exception:
            pass
    except Exception:
        return


def enrich_one_serialized(media_item_id: int, providers_override=None, force_media_type=None) -> bool:
    """Run enrich_one under a global lock.

    Prevents overlapping provider calls/DB writes when the background runner is active
    and the API also triggers per-item enrichment refreshes.
    """
    with _enrich_call_lock:
        return bool(enrich_one(int(media_item_id), providers_override=providers_override, force_media_type=force_media_type))


def backfill_episodes_for_media_item_serialized(media_item_id: int) -> dict:
    """Backfill episode titles for one media item under a global lock."""
    with _enrich_call_lock:
        try:
            return enrichment_service.backfill_tmdb_episode_titles_for_media_item(int(media_item_id), manage_state=True)
        except Exception as e:
            logger.exception("Backfill episodes failed for media_item %s", media_item_id)
            return {"ok": False, "detail": str(e), "seasons_updated": 0, "episodes_updated": 0}


def _fetch_pending_ids(session, limit: Optional[int] = None):
    # Consider an item enriched only if there exists a MediaMetadata row with a non-empty provider
    subq = session.query(models.MediaMetadata.media_item_id).filter(models.MediaMetadata.provider != None).filter(models.MediaMetadata.provider != '')
    q = (
        session.query(models.MediaItem.id)
        .filter(~models.MediaItem.id.in_(subq))
        .filter(
            (models.MediaItem.status == None)  # noqa: E711
            | (
                (models.MediaItem.status != 'ERROR')
                & (models.MediaItem.status != 'NO_MATCH')
                & (models.MediaItem.status != 'MANUAL')
                & (models.MediaItem.status != 'OMITTED')
            )
        )
        .order_by(models.MediaItem.id)
    )
    if limit:
        q = q.limit(limit)
    return [r[0] for r in q.all()]


def _enrichment_worker(batch_size: int, rate_per_min: int):
    """Worker that processes pending media items in batches and respects rate limit.

    - batch_size: how many items to fetch per DB query
    - rate_per_min: approximate provider calls per minute
    """
    global _running
    session = SessionLocal()
    try:
        while True:
            if _stop.is_set():
                logger.info('Enrichment runner: stop requested, exiting')
                break

            # Pause gate
            while _paused.is_set() and not _stop.is_set():
                try:
                    set_state({'running': True, 'paused': True, 'current_step': 'paused'})
                except Exception:
                    pass
                time.sleep(0.25)
            if _stop.is_set():
                logger.info('Enrichment runner: stop requested while paused, exiting')
                break

            ids = _fetch_pending_ids(session, limit=batch_size)
            if not ids:
                # Optional finalization: fill missing/generic episode titles from TMDB for TV seasons.
                # This keeps the UI consistent without requiring manual API calls.
                try:
                    if not _stop.is_set():
                        # Run localization as a separate job (can be triggered manually too).
                        try:
                            start_localization_job()
                        except Exception:
                            logger.exception("Final localization job failed to start")
                except Exception:
                    pass

                logger.info('Enrichment runner: no pending items, exiting')
                break
            delay = 60.0 / max(1, rate_per_min)
            for mid in ids:
                if _stop.is_set():
                    break
                while _paused.is_set() and not _stop.is_set():
                    try:
                        set_state({'running': True, 'paused': True, 'current_step': 'paused'})
                    except Exception:
                        pass
                    time.sleep(0.25)
                try:
                    # call enrich_one which will perform provider calls and persist metadata
                    enrich_one_serialized(mid)
                except Exception:
                    logger.exception('Enrichment failed for media_item %s', mid)
                # sleep to respect approximate rate
                time.sleep(delay)
            if _stop.is_set():
                logger.info('Enrichment runner: stop requested, exiting')
                break
            # small pause between batches
            time.sleep(1)
    finally:
        session.close()
        with _run_lock:
            _running = False
        try:
            _paused.clear()
            _stop.clear()
        except Exception:
            pass
        try:
            clear_state()
        except Exception:
            pass


def start_enrichment_job(batch_size: Optional[int] = None, rate_per_min: Optional[int] = None):
    """Start the enrichment runner in a background thread if not already running.

    Reads defaults from config keys `enrichment_batch_size` and `enrichment_rate_per_min` when not provided.
    """
    global _running
    # Preflight: if there is nothing pending, don't start a runner thread. This prevents
    # repeated "idle" runs being triggered by the watcher/scan flow and avoids looping
    # finalization steps indefinitely.
    try:
        s0 = SessionLocal()
        try:
            ids0 = _fetch_pending_ids(s0, limit=1)
        finally:
            s0.close()
        if not ids0:
            return False
    except Exception:
        # If the check fails, fall back to starting (keeps behavior compatible).
        pass
    with _run_lock:
        if _running:
            # detect stale _running flag if shared enrich_state says not running
            try:
                st = get_enrich_state()
                if not st.get('running'):
                    logger.warning('Detected stale _running flag; resetting and starting new runner')
                    _running = False
                else:
                    logger.info('Enrichment runner already running')
                    return False
            except Exception:
                logger.info('Enrichment runner already running')
                return False
        _running = True
    # clear previous stop request if any
    try:
        _stop.clear()
    except Exception:
        pass
    try:
        # reflect runner start in shared enrichment state used by API
        set_state({'running': True, 'paused': bool(_paused.is_set()), 'stop_requested': False, 'current_id': None, 'current_title': None, 'current_step': 'starting'})
    except Exception:
        pass

    bs = int(batch_size or get('enrichment_batch_size') or 20)
    rpm = int(rate_per_min or get('enrichment_rate_per_min') or 60)

    t = threading.Thread(target=_enrichment_worker, args=(bs, rpm), daemon=True, name='enrichment-runner')
    t.start()
    logger.info('Started enrichment runner (batch=%s rate_per_min=%s)', bs, rpm)
    return True


def is_running():
    with _run_lock:
        return _running
