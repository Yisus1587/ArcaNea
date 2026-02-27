"""Shared localization job state for reporting TMDB i18n sync progress."""

from __future__ import annotations

import threading
from typing import Any, Dict


_lock = threading.Lock()
_state: Dict[str, Any] = {
    "running": False,
    "current_step": None,
    "current_title": None,
    "last_updated": None,
    "error": None,
    "result": None,
}


def set_state(updates: Dict[str, Any]) -> None:
    from time import time

    with _lock:
        _state.update(dict(updates or {}))
        _state["last_updated"] = int(time())


def get_state() -> Dict[str, Any]:
    with _lock:
        return dict(_state)


def clear_state() -> None:
    with _lock:
        _state.update(
            {
                "running": False,
                "current_step": None,
                "current_title": None,
                "error": None,
                "result": None,
            }
        )

