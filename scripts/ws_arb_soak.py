#!/usr/bin/env python3
"""Run a multiplex soak window and summarize freshness/stability signals."""

from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_SCRIPT = REPO_ROOT / "scripts" / "ws_arb_benchmark.py"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a timed multiplex soak window and summarize heartbeat freshness, "
            "stream age snapshots, alerts, and mode transitions."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8011",
        help="Pair manager API base URL (default: http://127.0.0.1:8011)",
    )
    parser.add_argument(
        "--engine",
        choices=("multiplex",),
        default="multiplex",
        help="Engine to validate (default: multiplex).",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=300.0,
        help="How long to run the soak window (default: 300).",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=10.0,
        help="Debug heartbeat interval passed to ws_arb_alerts.py (default: 10).",
    )
    parser.add_argument(
        "--mapping-refresh-seconds",
        type=float,
        default=3600.0,
        help="Mapping refresh interval passed to ws_arb_alerts.py (default: 3600).",
    )
    parser.add_argument(
        "--book-stale-seconds",
        type=float,
        default=15.0,
        help="Staleness threshold to apply to per-stream ages (default: 15).",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=REPO_ROOT / "output" / "soak",
        help="Directory for soak artifacts (default: output/soak).",
    )
    parser.add_argument(
        "--artifact-label",
        default=None,
        help="Optional label suffix for artifacts (default: timestamp).",
    )
    parser.add_argument(
        "--kalshi-direct-mode",
        action="store_true",
        help="Run the soak with --kalshi-direct-mode.",
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
        help="Optional explicit output path for the soak summary JSON.",
    )
    return parser.parse_args()


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


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = int(round((len(ordered) - 1) * pct))
    return ordered[max(0, min(len(ordered) - 1, idx))]


def _max_nested_numeric_dicts(
    snapshots: list[dict[str, Any]],
    *,
    field: str,
) -> dict[str, float]:
    result: dict[str, float] = {}
    for snapshot in snapshots:
        heartbeat = snapshot.get("heartbeat")
        if not isinstance(heartbeat, dict):
            continue
        values = heartbeat.get(field)
        if not isinstance(values, dict):
            continue
        for exchange, value in values.items():
            if not isinstance(exchange, str) or not isinstance(value, (int, float)):
                continue
            result[exchange] = max(result.get(exchange, float("-inf")), float(value))
    return dict(sorted(result.items()))


def _summarize_stream_snapshot(
    heartbeat: dict[str, Any],
    stream_states: list[dict[str, Any]],
    *,
    stale_after_seconds: float,
) -> dict[str, Any]:
    ages = [
        float(item["last_ok_age_s"])
        for item in stream_states
        if isinstance(item.get("last_ok_age_s"), (int, float))
    ]
    fresh_count = sum(1 for age in ages if age <= stale_after_seconds)
    stale_count = sum(1 for age in ages if age > stale_after_seconds)
    never_ok_count = sum(
        1
        for item in stream_states
        if not isinstance(item.get("ok"), int) or int(item["ok"]) <= 0
    )
    stale_after_success_count = sum(
        1
        for item in stream_states
        if isinstance(item.get("ok"), int)
        and int(item["ok"]) > 0
        and isinstance(item.get("last_ok_age_s"), (int, float))
        and float(item["last_ok_age_s"]) > stale_after_seconds
    )
    per_exchange: dict[str, dict[str, int]] = {}
    for item in stream_states:
        exchange = item.get("exchange")
        if not isinstance(exchange, str):
            continue
        bucket = per_exchange.setdefault(
            exchange,
            {
                "stream_count": 0,
                "fresh_after_success_count": 0,
                "stale_after_success_count": 0,
                "never_ok_count": 0,
            },
        )
        bucket["stream_count"] += 1
        ok_count = item.get("ok")
        age = item.get("last_ok_age_s")
        if not isinstance(ok_count, int) or int(ok_count) <= 0:
            bucket["never_ok_count"] += 1
            continue
        if isinstance(age, (int, float)) and float(age) > stale_after_seconds:
            bucket["stale_after_success_count"] += 1
            continue
        bucket["fresh_after_success_count"] += 1
    return {
        "heartbeat": heartbeat,
        "stream_count": len(stream_states),
        "fresh_stream_count": fresh_count,
        "stale_stream_count": stale_count,
        "never_ok_stream_count": never_ok_count,
        "stale_after_success_stream_count": stale_after_success_count,
        "per_exchange": per_exchange,
        "stream_age_p50_s": _percentile(ages, 0.50),
        "stream_age_p95_s": _percentile(ages, 0.95),
        "stream_age_max_s": max(ages) if ages else None,
    }


def _summarize_log_lines(
    lines: list[str], *, stale_after_seconds: float
) -> dict[str, Any]:
    snapshots: list[dict[str, Any]] = []
    current_heartbeat: dict[str, Any] | None = None
    current_streams: list[dict[str, Any]] = []
    exchange_mode_events: Counter[str] = Counter()
    exchange_source_mode_events: Counter[str] = Counter()
    exchange_recovery_events: Counter[str] = Counter()

    for line in lines:
        if line.startswith("[debug] heartbeat "):
            if current_heartbeat is not None:
                snapshots.append(
                    _summarize_stream_snapshot(
                        current_heartbeat,
                        current_streams,
                        stale_after_seconds=stale_after_seconds,
                    )
                )
            current_heartbeat = _parse_key_values(line.split("heartbeat ", maxsplit=1)[1])
            current_streams = []
            continue
        if line.startswith("[debug] stream ") and current_heartbeat is not None:
            current_streams.append(_parse_key_values(line.split("stream ", maxsplit=1)[1]))
            continue
        if line.startswith("[info] exchange_mode "):
            fields = _parse_key_values(line.split("exchange_mode ", maxsplit=1)[1])
            exchange = fields.get("exchange")
            mode = fields.get("mode")
            if isinstance(exchange, str) and isinstance(mode, str):
                exchange_mode_events[f"{exchange}:{mode}"] += 1
            continue
        if line.startswith("[info] exchange_source_mode "):
            fields = _parse_key_values(line.split("exchange_source_mode ", maxsplit=1)[1])
            exchange = fields.get("exchange")
            mode = fields.get("mode")
            if isinstance(exchange, str) and isinstance(mode, str):
                exchange_source_mode_events[f"{exchange}:{mode}"] += 1
            continue
        if line.startswith("[info] exchange_recovery "):
            fields = _parse_key_values(line.split("exchange_recovery ", maxsplit=1)[1])
            exchange = fields.get("exchange")
            reason = fields.get("reason")
            if isinstance(exchange, str) and isinstance(reason, str):
                exchange_recovery_events[f"{exchange}:{reason}"] += 1

    if current_heartbeat is not None:
        snapshots.append(
            _summarize_stream_snapshot(
                current_heartbeat,
                current_streams,
                stale_after_seconds=stale_after_seconds,
            )
        )

    quote_counts = [
        int(snapshot["heartbeat"]["quote_count"])
        for snapshot in snapshots
        if isinstance(snapshot["heartbeat"].get("quote_count"), int)
    ]
    stale_quote_counts = [
        int(snapshot["heartbeat"]["stale_quote_count"])
        for snapshot in snapshots
        if isinstance(snapshot["heartbeat"].get("stale_quote_count"), int)
    ]
    p95_stream_age_values = [
        float(snapshot["stream_age_p95_s"])
        for snapshot in snapshots
        if isinstance(snapshot.get("stream_age_p95_s"), (int, float))
    ]
    max_stream_age_values = [
        float(snapshot["stream_age_max_s"])
        for snapshot in snapshots
        if isinstance(snapshot.get("stream_age_max_s"), (int, float))
    ]
    stale_stream_counts = [int(snapshot["stale_stream_count"]) for snapshot in snapshots]
    never_ok_stream_counts = [int(snapshot["never_ok_stream_count"]) for snapshot in snapshots]
    stale_after_success_counts = [
        int(snapshot["stale_after_success_stream_count"]) for snapshot in snapshots
    ]
    per_exchange_tail_max: dict[str, dict[str, int]] = {}
    for snapshot in snapshots:
        per_exchange = snapshot.get("per_exchange")
        if not isinstance(per_exchange, dict):
            continue
        for exchange, counts in per_exchange.items():
            if not isinstance(exchange, str) or not isinstance(counts, dict):
                continue
            bucket = per_exchange_tail_max.setdefault(
                exchange,
                {
                    "stream_count_max": 0,
                    "fresh_after_success_count_max": 0,
                    "stale_after_success_count_max": 0,
                    "never_ok_count_max": 0,
                },
            )
            for field, max_field in (
                ("stream_count", "stream_count_max"),
                ("fresh_after_success_count", "fresh_after_success_count_max"),
                ("stale_after_success_count", "stale_after_success_count_max"),
                ("never_ok_count", "never_ok_count_max"),
            ):
                value = counts.get(field)
                if isinstance(value, int):
                    bucket[max_field] = max(bucket[max_field], value)

    return {
        "heartbeat_count": len(snapshots),
        "quote_count_min": min(quote_counts) if quote_counts else None,
        "quote_count_max": max(quote_counts) if quote_counts else None,
        "stale_quote_count_max": max(stale_quote_counts) if stale_quote_counts else None,
        "stream_age_p95_s_max": max(p95_stream_age_values) if p95_stream_age_values else None,
        "stream_age_max_s_max": max(max_stream_age_values) if max_stream_age_values else None,
        "stale_stream_count_max": max(stale_stream_counts) if stale_stream_counts else None,
        "never_ok_stream_count_max": max(never_ok_stream_counts) if never_ok_stream_counts else None,
        "stale_after_success_stream_count_max": (
            max(stale_after_success_counts) if stale_after_success_counts else None
        ),
        "per_exchange_tail_max": dict(sorted(per_exchange_tail_max.items())),
        "exchange_updates_per_min_max": _max_nested_numeric_dicts(
            snapshots,
            field="exchange_updates_per_min",
        ),
        "exchange_last_ok_age_p95_s_max": _max_nested_numeric_dicts(
            snapshots,
            field="exchange_last_ok_age_p95_s",
        ),
        "exchange_last_ok_age_max_s_max": _max_nested_numeric_dicts(
            snapshots,
            field="exchange_last_ok_age_max_s",
        ),
        "exchange_latency_p95_ms_max": _max_nested_numeric_dicts(
            snapshots,
            field="exchange_latency_p95_ms",
        ),
        "exchange_latency_max_ms_max": _max_nested_numeric_dicts(
            snapshots,
            field="exchange_latency_max_ms",
        ),
        "exchange_mode_events": dict(sorted(exchange_mode_events.items())),
        "exchange_source_mode_events": dict(sorted(exchange_source_mode_events.items())),
        "exchange_recovery_events": dict(sorted(exchange_recovery_events.items())),
        "snapshots": snapshots,
    }


def _run_benchmark(
    *,
    args: argparse.Namespace,
    artifact_dir: Path,
    artifact_label: str,
) -> tuple[dict[str, Any], Path, Path]:
    benchmark_json = artifact_dir / f"multiplex_soak_{artifact_label}.benchmark.json"
    benchmark_log = artifact_dir / f"multiplex_soak_{artifact_label}.log"
    command = [
        sys.executable,
        str(BENCHMARK_SCRIPT),
        "--api-base-url",
        args.api_base_url,
        "--engine",
        args.engine,
        "--duration-seconds",
        str(args.duration_seconds),
        "--heartbeat-seconds",
        str(args.heartbeat_seconds),
        "--mapping-refresh-seconds",
        str(args.mapping_refresh_seconds),
        "--output-json",
        str(benchmark_json),
        "--output-log",
        str(benchmark_log),
    ]
    if args.kalshi_direct_mode:
        command.append("--kalshi-direct-mode")
    if args.kalshi_book_mode:
        command.extend(["--kalshi-book-mode", args.kalshi_book_mode])

    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Benchmark run failed with exit code {result.returncode}:\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return json.loads(benchmark_json.read_text()), benchmark_json, benchmark_log


def main() -> int:
    args = _parse_args()
    artifact_dir = args.artifact_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_label = args.artifact_label or time.strftime("%Y%m%d_%H%M%S")

    benchmark_output, benchmark_json, benchmark_log = _run_benchmark(
        args=args,
        artifact_dir=artifact_dir,
        artifact_label=artifact_label,
    )
    raw_lines = benchmark_log.read_text().splitlines()
    soak_summary = _summarize_log_lines(raw_lines, stale_after_seconds=args.book_stale_seconds)

    output = {
        "api_base_url": args.api_base_url,
        "engine": args.engine,
        "duration_seconds": args.duration_seconds,
        "heartbeat_seconds": args.heartbeat_seconds,
        "mapping_refresh_seconds": args.mapping_refresh_seconds,
        "book_stale_seconds": args.book_stale_seconds,
        "artifacts": {
            "benchmark_json": str(benchmark_json),
            "benchmark_log": str(benchmark_log),
        },
        "benchmark": benchmark_output,
        "soak_summary": soak_summary,
    }

    output_path = args.output_json or (artifact_dir / f"multiplex_soak_{artifact_label}.json")
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")

    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
