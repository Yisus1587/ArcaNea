from __future__ import annotations


def pick_primary_metadata(metadata_out: list[dict] | None) -> dict | None:
    if not metadata_out:
        return None
    try:
        for m in metadata_out:
            if m and m.get("provider"):
                return m
        return metadata_out[-1]
    except Exception:
        return None

