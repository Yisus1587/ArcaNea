from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], *, cwd: Path | None = None, env: dict | None = None) -> int:
    try:
        completed = subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env, check=True, shell=True)
        return int(completed.returncode or 0)
    except KeyboardInterrupt:
        return 0

def main() -> int:
    parser = argparse.ArgumentParser(description="Build frontend and run backend (dev).")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default="9800")
    parser.add_argument("--app", default="src.api.asgi:app")
    parser.add_argument("--no-reload", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-serve-frontend", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    fe_dir = root / "arcanea-media-server"

    if not args.skip_build:
        if not fe_dir.is_dir():
            print(f"[dev_run] frontend dir not found: {fe_dir}", file=sys.stderr)
            return 1
        code = run(["npm", "run", "build"], cwd=fe_dir)
        if code != 0:
            return code

    env = os.environ.copy()
    if not args.no_serve_frontend:
        env["ARCANEA_SERVE_FRONTEND"] = "1"

    cmd = [sys.executable, "-m", "uvicorn", args.app, "--host", args.host, "--port", str(args.port)]
    if not args.no_reload:
        cmd.append("--reload")

    return run(cmd, cwd=root, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
