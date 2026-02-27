from __future__ import annotations


def check_tmdb_connectivity() -> dict:
    """Attempt a lightweight TMDB request using configured credentials to verify connectivity."""
    try:
        try:
            from ...providers import provider_tmdb as p_tmdb
        except Exception:
            p_tmdb = None

        ok = False
        detail = None
        if p_tmdb and hasattr(p_tmdb, "tmdb_check_connection"):
            try:
                ok = bool(p_tmdb.tmdb_check_connection(force=True))
            except Exception as e:
                ok = False
                detail = str(e)
        else:
            ok = False
            detail = "provider_tmdb not available"
        return {"ok": bool(ok), "detail": detail}
    except Exception as e:
        return {"ok": False, "detail": str(e)}

