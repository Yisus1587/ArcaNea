from __future__ import annotations

import hashlib
import os
import re


def normalize_path_for_hash(path: str | None) -> str:
    if not path:
        return ""
    try:
        s = str(path).strip()
    except Exception:
        return ""
    if not s:
        return ""

    try:
        s = os.path.normpath(s)
    except Exception:
        pass

    s = s.replace("\\", "/")

    prefix = ""
    if s.startswith("//"):
        prefix = "//"
        s = s[2:]
    s = re.sub(r"/+", "/", s)
    s = prefix + s

    try:
        if os.name == "nt" or re.match(r"^[A-Za-z]:/", s):
            s = s.lower()
    except Exception:
        pass

    return s


def hash_path(path: str | None) -> str:
    norm = normalize_path_for_hash(path)
    if not norm:
        return ""
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()
