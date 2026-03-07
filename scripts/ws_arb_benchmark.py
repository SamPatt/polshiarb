#!/usr/bin/env python3
"""Run a timed ws_arb_alerts benchmark and summarize heartbeat output."""

from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run scripts/ws_arb_alerts.py for a fixed interval and summarize "
            "alerts, warnings, heartbeat metrics, and reconnect proxies."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8011",
        help="Pair manager API base URL (default: http://127.0.0.1:8011)",
    )
    parser.add_argument(
        "--engine",
        choices=("legacy", "multiplex"),
        required=True,
        help="Streaming engine to benchmark.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=20.0,
        help="How long to run the benchmark before sending SIGINT (default: 20).",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=5.0,
        help="Debug heartbeat interval passed to ws_arb_alerts.py (default: 5).",
    )
    parser.add_argument(
        "--mapping-refresh-seconds",
        type=float,
        default=3600.0,
        help="Mapping refresh interval passed to ws_arb_alerts.py (default: 3600).",
    )
    parser.add_argument(
        "--kalshi-direct-mode",
        action="store_true",
        help="Run ws_arb_alerts.py with --kalshi-direct-mode.",
    )
    parser.add_argument(
        "--kalshi-book-mode",
        choices=("auto", "watch", "poll"),
        default=None,
        help="Optional explicit Kalshi book mode override.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write the JSON summary.",
    )
    parser.add_argument(
        "--output-log",
        type=Path,
        default=None,
        help="Optional path to write the raw runner log lines.",
    )
    return parser.parse_args()


def _build_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(REPO_ROOT / "scripts" / "ws_arb_alerts.py"),
        "--api-base-url",
        args.api_base_url,
        "--engine",
        args.engine,
        "--mapping-refresh-seconds",
        str(args.mapping_refresh_seconds),
        "--debug",
        "--debug-heartbeat-seconds",
        str(args.heartbeat_seconds),
        "--compact-alerts",
    ]
    if args.kalshi_direct_mode:
        command.append("--kalshi-direct-mode")
    if args.kalshi_book_mode:
        command.extend(["--kalshi-book-mode", args.kalshi_book_mode])
    return command


def _parse_scalar(value: str) -> Any:
    if value == "na":
        return None
    if value.startswith("{") or value.startswith("["):
        return ast.literal_eval(value)
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_key_values(payload: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    idx = 0
    length = len(payload)
    while idx < length:
        while idx < length and payload[idx] == " ":
            idx += 1
        if idx >= length:
            break
        eq_idx = payload.find("=", idx)
        if eq_idx == -1:
            break
        key = payload[idx:eq_idx]
        value_start = eq_idx + 1
        if value_start >= length:
            fields[key] = None
            break
        if payload[value_start] in "{[":
            open_char = payload[value_start]
            close_char = "}" if open_char == "{" else "]"
            depth = 0
            value_end = value_start
            while value_end < length:
                char = payload[value_end]
                if char == open_char:
                    depth += 1
                elif char == close_char:
                    depth -= 1
                    if depth == 0:
                        value_end += 1
                        break
                value_end += 1
            value = payload[value_start:value_end]
            idx = value_end + 1
        else:
            next_space = payload.find(" ", value_start)
            if next_space == -1:
                value = payload[value_start:]
                idx = length
            else:
                value = payload[value_start:next_space]
                idx = next_space + 1
        fields[key] = _parse_scalar(value)
    return fields


def _summarize(lines: list[tuple[float, str]]) -> dict[str, Any]:
    raw_lines = [line for _, line in lines]
    alert_lines = [line for line in raw_lines if line.startswith("[ALERT_")]
    warn_lines = [line for line in raw_lines if line.startswith("[warn]")]
    error_lines = [line for line in raw_lines if line.startswith("[error]")]
    info_lines = [line for line in raw_lines if line.startswith("[info]")]
    debug_lines = [line for line in raw_lines if line.startswith("[debug]")]
    heartbeat_lines = [line for line in debug_lines if line.startswith("[debug] heartbeat ")]
    heartbeat_offsets = [offset for offset, line in lines if line.startswith("[debug] heartbeat ")]
    alert_offsets = [offset for offset, line in lines if line.startswith("[ALERT_")]
    client_create_lines = [line for line in debug_lines if "exchange client created" in line]
    client_close_lines = [line for line in debug_lines if "exchange client closed" in line]

    startup_line = next(
        (line for line in info_lines if line.startswith("[info] mapping refresh reason=startup ")),
        None,
    )
    startup_offset = next(
        (offset for offset, line in lines if line.startswith("[info] mapping refresh reason=startup ")),
        None,
    )

    startup_metrics: dict[str, Any] = {}
    if startup_line is not None:
        startup_metrics = _parse_key_values(startup_line.split("mapping refresh ", maxsplit=1)[1])

    heartbeats = [
        _parse_key_values(line.split("heartbeat ", maxsplit=1)[1])
        for line in heartbeat_lines
    ]

    return {
        "line_count": len(raw_lines),
        "counts": {
            "alerts": len(alert_lines),
            "warnings": len(warn_lines),
            "errors": len(error_lines),
            "infos": len(info_lines),
            "debugs": len(debug_lines),
            "client_creates": len(client_create_lines),
            "client_closes": len(client_close_lines),
        },
        "alert_counts": dict(Counter(line.split()[0] for line in alert_lines)),
        "samples": {
            "first_warning": warn_lines[0] if warn_lines else None,
            "first_error": error_lines[0] if error_lines else None,
            "first_alert": alert_lines[0] if alert_lines else None,
        },
        "timing_seconds": {
            "startup_mapping_refresh": startup_offset,
            "first_heartbeat": heartbeat_offsets[0] if heartbeat_offsets else None,
            "first_alert": alert_offsets[0] if alert_offsets else None,
        },
        "startup_metrics": startup_metrics,
        "heartbeat_count": len(heartbeats),
        "last_heartbeat": heartbeats[-1] if heartbeats else None,
        "raw_lines": raw_lines,
    }


def main() -> int:
    args = _parse_args()
    command = _build_command(args)

    proc = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    lines: list[tuple[float, str]] = []
    lines_lock = threading.Lock()
    started_at = time.monotonic()

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            with lines_lock:
                lines.append((time.monotonic() - started_at, line.rstrip("\n")))

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()

    time.sleep(max(0.1, args.duration_seconds))
    if proc.poll() is None:
        proc.send_signal(signal.SIGINT)

    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)

    reader.join(timeout=2.0)
    with lines_lock:
        summary = _summarize(list(lines))

    output = {
        "engine": args.engine,
        "command": command,
        "duration_seconds_requested": args.duration_seconds,
        "duration_seconds_observed": round(time.monotonic() - started_at, 3),
        "exit_code": proc.returncode,
        "summary": {k: v for k, v in summary.items() if k != "raw_lines"},
    }

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    if args.output_log is not None:
        args.output_log.parent.mkdir(parents=True, exist_ok=True)
        args.output_log.write_text("".join(f"{line}\n" for line in summary["raw_lines"]))

    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
