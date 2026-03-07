#!/usr/bin/env python3
"""Run a normalized legacy vs multiplex shadow-run parity comparison."""

from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_SCRIPT = REPO_ROOT / "scripts" / "ws_arb_benchmark.py"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.arb_alerts import OutcomeRef, MappingRow, SemanticMapping, build_semantic_mappings
from app.db import DEFAULT_DB_PATH, list_monitoring_links, load_pair_set


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run legacy and multiplex benchmark windows against the same API and "
            "compare normalized alert parity."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8011",
        help="Pair manager API base URL (default: http://127.0.0.1:8011)",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=20.0,
        help="Duration for each benchmark window (default: 20).",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=5.0,
        help="Debug heartbeat interval for each run (default: 5).",
    )
    parser.add_argument(
        "--mapping-refresh-seconds",
        type=float,
        default=3600.0,
        help="Mapping refresh interval passed to the runner (default: 3600).",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=REPO_ROOT / "output" / "parity",
        help="Directory for benchmark and parity artifacts.",
    )
    parser.add_argument(
        "--artifact-label",
        default="direct",
        help="Label suffix for artifact filenames (default: direct).",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to pair manager sqlite db (default: {DEFAULT_DB_PATH}).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional explicit output path for the parity comparison JSON.",
    )
    return parser.parse_args()


def _run_benchmark(
    *,
    api_base_url: str,
    engine: str,
    duration_seconds: float,
    heartbeat_seconds: float,
    mapping_refresh_seconds: float,
    artifact_dir: Path,
    artifact_stem: str,
    kalshi_direct_mode: bool,
    kalshi_book_mode: str | None,
) -> dict[str, Any]:
    output_json = artifact_dir / f"{artifact_stem}.json"
    output_log = artifact_dir / f"{artifact_stem}.log"

    command = [
        sys.executable,
        str(BENCHMARK_SCRIPT),
        "--api-base-url",
        api_base_url,
        "--engine",
        engine,
        "--duration-seconds",
        str(duration_seconds),
        "--heartbeat-seconds",
        str(heartbeat_seconds),
        "--mapping-refresh-seconds",
        str(mapping_refresh_seconds),
        "--output-json",
        str(output_json),
        "--output-log",
        str(output_log),
    ]
    if kalshi_direct_mode:
        command.append("--kalshi-direct-mode")
    if kalshi_book_mode is not None:
        command.extend(["--kalshi-book-mode", kalshi_book_mode])

    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Benchmark run failed for {engine} with exit code {result.returncode}:\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return json.loads(output_json.read_text())


def _normalize_alert(line: str) -> str:
    tokens = [token for token in line.split() if not token.startswith("ts=")]
    return " ".join(tokens)


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
        next_space = payload.find(" ", value_start)
        if next_space == -1:
            value = payload[value_start:]
            idx = length
        else:
            value = payload[value_start:next_space]
            idx = next_space + 1
        fields[key] = _parse_scalar(value)
    return fields


def _load_normalized_alerts(path: Path) -> list[str]:
    lines = path.read_text().splitlines()
    return [_normalize_alert(line) for line in lines if line.startswith("[ALERT_")]


def _load_stream_states(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    states: dict[tuple[str, str], dict[str, Any]] = {}
    for line in path.read_text().splitlines():
        if not line.startswith("[debug] stream "):
            continue
        fields = _parse_key_values(line.split("stream ", maxsplit=1)[1])
        exchange = fields.get("exchange")
        outcome_id = fields.get("outcome_id")
        if not isinstance(exchange, str) or not isinstance(outcome_id, str):
            continue
        states[(exchange, outcome_id)] = fields
    return states


def _load_mapping_lookup(db_path: Path) -> tuple[dict[tuple[int, str, str], SemanticMapping], list[str]]:
    links = list_monitoring_links(
        db_path=db_path,
        active_only=True,
        include_expired=False,
        dedupe_latest=True,
    )
    rows: list[MappingRow] = []
    pair_ids: set[int] = set()
    for link in links:
        pair_id = link.get("pair_id")
        relation_type = link.get("relation_type")
        kalshi_market_id = link.get("kalshi_market_id")
        polymarket_market_id = link.get("polymarket_market_id")
        kalshi_outcome_id = link.get("kalshi_outcome_id")
        polymarket_outcome_id = link.get("polymarket_outcome_id")
        if not (
            isinstance(pair_id, int)
            and isinstance(relation_type, str)
            and isinstance(kalshi_market_id, str)
            and isinstance(polymarket_market_id, str)
            and isinstance(kalshi_outcome_id, str)
            and isinstance(polymarket_outcome_id, str)
        ):
            continue
        pair_ids.add(pair_id)
        rows.append(
            MappingRow(
                pair_id=pair_id,
                relation_type=relation_type,
                kalshi=OutcomeRef(
                    exchange="kalshi",
                    market_id=kalshi_market_id,
                    outcome_id=kalshi_outcome_id,
                ),
                polymarket=OutcomeRef(
                    exchange="polymarket",
                    market_id=polymarket_market_id,
                    outcome_id=polymarket_outcome_id,
                ),
            )
        )

    pair_details_by_id: dict[int, dict[str, Any]] = {}
    for pair_id in pair_ids:
        payload = load_pair_set(db_path, pair_id)
        if payload is None:
            continue
        pair_details_by_id[pair_id] = {
            "pair": {
                "pair_set": payload.get("pair_set"),
                "markets": payload.get("markets"),
            }
        }

    mappings, warnings = build_semantic_mappings(rows, pair_details_by_id)
    lookup = {
        (mapping.pair_id, mapping.kalshi_market_id, mapping.polymarket_market_id): mapping
        for mapping in mappings
    }
    return lookup, warnings


def _parse_alert_fields(line: str) -> dict[str, Any]:
    parts = line.split(maxsplit=1)
    fields: dict[str, Any] = {"tag": parts[0]}
    if len(parts) == 1:
        return fields
    fields.update(_parse_key_values(parts[1]))
    return fields


def _required_streams_for_metric(
    mapping: SemanticMapping, metric_name: str
) -> list[tuple[str, str]]:
    if metric_name == "same_token_gap_p":
        return [("kalshi", mapping.kalshi_p), ("polymarket", mapping.polymarket_p)]
    if metric_name == "same_token_gap_not_p":
        return [("kalshi", mapping.kalshi_not_p), ("polymarket", mapping.polymarket_not_p)]
    if metric_name == "cross_market_set_1":
        return [("kalshi", mapping.kalshi_p), ("polymarket", mapping.polymarket_not_p)]
    if metric_name == "cross_market_set_2":
        return [("kalshi", mapping.kalshi_not_p), ("polymarket", mapping.polymarket_p)]
    if metric_name == "within_kalshi":
        return [("kalshi", mapping.kalshi_p), ("kalshi", mapping.kalshi_not_p)]
    if metric_name == "within_polymarket":
        return [("polymarket", mapping.polymarket_p), ("polymarket", mapping.polymarket_not_p)]
    return []


def _stream_status(stream_state: dict[str, Any] | None, *, stale_after_seconds: float) -> str:
    if stream_state is None:
        return "missing"
    ok_count = stream_state.get("ok")
    last_ok_age_s = stream_state.get("last_ok_age_s")
    if not isinstance(ok_count, int) or ok_count <= 0:
        return "never_ok"
    if not isinstance(last_ok_age_s, (int, float)):
        return "unknown"
    if float(last_ok_age_s) > stale_after_seconds:
        return "stale"
    return "fresh"


def _build_alert_diagnostics(
    *,
    alerts: dict[str, int],
    mapping_lookup: dict[tuple[int, str, str], SemanticMapping],
    legacy_stream_states: dict[tuple[str, str], dict[str, Any]],
    multiplex_stream_states: dict[tuple[str, str], dict[str, Any]],
    stale_after_seconds: float = 15.0,
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for alert_line, count in sorted(alerts.items()):
        alert = _parse_alert_fields(alert_line)
        pair_id = alert.get("pair_id")
        kalshi_market_id = alert.get("kalshi_market_id")
        polymarket_market_id = alert.get("polymarket_market_id")
        metric_name = alert.get("metric")
        normalized_pair_id = int(pair_id) if isinstance(pair_id, int) else pair_id
        normalized_kalshi_market_id = (
            str(kalshi_market_id) if kalshi_market_id is not None else kalshi_market_id
        )
        normalized_polymarket_market_id = (
            str(polymarket_market_id)
            if polymarket_market_id is not None
            else polymarket_market_id
        )
        lookup_key = (
            normalized_pair_id,
            normalized_kalshi_market_id,
            normalized_polymarket_market_id,
        )
        mapping = mapping_lookup.get(lookup_key)
        required_streams = (
            _required_streams_for_metric(mapping, metric_name)
            if mapping is not None and isinstance(metric_name, str)
            else []
        )
        stream_diagnostics: list[dict[str, Any]] = []
        for stream_key in required_streams:
            legacy_state = legacy_stream_states.get(stream_key)
            multiplex_state = multiplex_stream_states.get(stream_key)
            stream_diagnostics.append(
                {
                    "exchange": stream_key[0],
                    "outcome_id": stream_key[1],
                    "legacy_status": _stream_status(
                        legacy_state, stale_after_seconds=stale_after_seconds
                    ),
                    "legacy_state": legacy_state,
                    "multiplex_status": _stream_status(
                        multiplex_state, stale_after_seconds=stale_after_seconds
                    ),
                    "multiplex_state": multiplex_state,
                }
            )

        diagnosis = "mapping_not_found"
        if mapping is not None and stream_diagnostics:
            legacy_statuses = {item["legacy_status"] for item in stream_diagnostics}
            multiplex_statuses = {item["multiplex_status"] for item in stream_diagnostics}
            if (
                legacy_statuses & {"missing", "never_ok", "stale"}
                and "fresh" in multiplex_statuses
            ):
                diagnosis = "legacy_missing_or_stale_required_quotes"
            elif "fresh" in legacy_statuses and "fresh" in multiplex_statuses:
                diagnosis = "freshness_gap_not_explained_by_final_snapshot"
            else:
                diagnosis = "inconclusive"

        diagnostics.append(
            {
                "alert": alert_line,
                "count": count,
                "pair_id": normalized_pair_id,
                "kalshi_market_id": normalized_kalshi_market_id,
                "polymarket_market_id": normalized_polymarket_market_id,
                "metric_name": metric_name,
                "diagnosis": diagnosis,
                "required_streams": stream_diagnostics,
            }
        )
    return diagnostics


def _counter_delta(left: Counter[str], right: Counter[str]) -> dict[str, int]:
    delta = left - right
    return dict(sorted(delta.items()))


def _build_comparison(
    *,
    legacy: dict[str, Any],
    multiplex: dict[str, Any],
    legacy_alerts: list[str],
    multiplex_alerts: list[str],
) -> dict[str, Any]:
    legacy_counter = Counter(legacy_alerts)
    multiplex_counter = Counter(multiplex_alerts)
    overlap = legacy_counter & multiplex_counter

    legacy_start = legacy["summary"]["startup_metrics"]
    multiplex_start = multiplex["summary"]["startup_metrics"]

    return {
        "startup_metrics_match": legacy_start == multiplex_start,
        "startup_metrics": {
            "legacy": legacy_start,
            "multiplex": multiplex_start,
        },
        "alert_counts": {
            "legacy": legacy["summary"]["alert_counts"],
            "multiplex": multiplex["summary"]["alert_counts"],
        },
        "normalized_alert_totals": {
            "legacy": sum(legacy_counter.values()),
            "multiplex": sum(multiplex_counter.values()),
            "overlap": sum(overlap.values()),
        },
        "exact_alert_parity": legacy_counter == multiplex_counter,
        "legacy_only_alerts": _counter_delta(legacy_counter, multiplex_counter),
        "multiplex_only_alerts": _counter_delta(multiplex_counter, legacy_counter),
        "legacy_exit_code": legacy["exit_code"],
        "multiplex_exit_code": multiplex["exit_code"],
        "legacy_last_heartbeat": legacy["summary"]["last_heartbeat"],
        "multiplex_last_heartbeat": multiplex["summary"]["last_heartbeat"],
    }


def main() -> int:
    args = _parse_args()
    args.artifact_dir.mkdir(parents=True, exist_ok=True)

    legacy_stem = f"legacy_{args.artifact_label}"
    multiplex_stem = f"multiplex_{args.artifact_label}"

    legacy = _run_benchmark(
        api_base_url=args.api_base_url,
        engine="legacy",
        duration_seconds=args.duration_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
        mapping_refresh_seconds=args.mapping_refresh_seconds,
        artifact_dir=args.artifact_dir,
        artifact_stem=legacy_stem,
        kalshi_direct_mode=True,
        kalshi_book_mode="poll",
    )
    multiplex = _run_benchmark(
        api_base_url=args.api_base_url,
        engine="multiplex",
        duration_seconds=args.duration_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
        mapping_refresh_seconds=args.mapping_refresh_seconds,
        artifact_dir=args.artifact_dir,
        artifact_stem=multiplex_stem,
        kalshi_direct_mode=True,
        kalshi_book_mode=None,
    )

    legacy_alerts = _load_normalized_alerts(args.artifact_dir / f"{legacy_stem}.log")
    multiplex_alerts = _load_normalized_alerts(args.artifact_dir / f"{multiplex_stem}.log")
    comparison = _build_comparison(
        legacy=legacy,
        multiplex=multiplex,
        legacy_alerts=legacy_alerts,
        multiplex_alerts=multiplex_alerts,
    )
    legacy_stream_states = _load_stream_states(args.artifact_dir / f"{legacy_stem}.log")
    multiplex_stream_states = _load_stream_states(args.artifact_dir / f"{multiplex_stem}.log")
    mapping_lookup, mapping_warnings = _load_mapping_lookup(args.db_path)
    multiplex_only_diagnostics = _build_alert_diagnostics(
        alerts=comparison["multiplex_only_alerts"],
        mapping_lookup=mapping_lookup,
        legacy_stream_states=legacy_stream_states,
        multiplex_stream_states=multiplex_stream_states,
    )

    output = {
        "api_base_url": args.api_base_url,
        "db_path": str(args.db_path),
        "duration_seconds": args.duration_seconds,
        "heartbeat_seconds": args.heartbeat_seconds,
        "mapping_refresh_seconds": args.mapping_refresh_seconds,
        "artifacts": {
            "legacy_json": str(args.artifact_dir / f"{legacy_stem}.json"),
            "legacy_log": str(args.artifact_dir / f"{legacy_stem}.log"),
            "multiplex_json": str(args.artifact_dir / f"{multiplex_stem}.json"),
            "multiplex_log": str(args.artifact_dir / f"{multiplex_stem}.log"),
        },
        "comparison": comparison,
        "diagnostics": {
            "mapping_warnings": mapping_warnings,
            "multiplex_only_alerts": multiplex_only_diagnostics,
        },
    }

    output_path = args.output_json or (args.artifact_dir / f"shadow_parity_{args.artifact_label}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")

    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
