"""Detached command wrapper that leaves a durable terminal-status file."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise SystemExit("missing command")
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("ab", buffering=0) as output:
        proc = subprocess.Popen(command, cwd=args.cwd, stdin=subprocess.DEVNULL,
                                stdout=output, stderr=subprocess.STDOUT)
        exit_code = proc.wait()
    status_path = Path(args.status_file)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = status_path.with_suffix(".tmp")
    temporary.write_text(json.dumps({
        "exit_code": exit_code,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }), encoding="utf-8")
    os.replace(temporary, status_path)
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
