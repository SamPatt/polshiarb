#!/usr/bin/env python3
"""Continuously monitor mapped Kalshi/Polymarket outcomes and print arb alerts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import threading
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pmxt
import urllib3

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.arb_alerts import (
    QuoteSnapshot,
    build_semantic_mappings,
    evaluate_all_mappings,
    extract_best_prices,
    extract_book_timestamp_ms,
    format_alert_line,
    mapping_stream_keys,
    normalize_monitoring_rows,
    passes_cooldown,
)
from app.kalshi_credentials import build_kalshi_client_kwargs


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Watch mapped Kalshi + Polymarket orderbooks and print top-of-book "
            "arb/deviation alerts."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8011",
        help="Pair manager API base URL (default: http://127.0.0.1:8011)",
    )
    parser.add_argument(
        "--arb-threshold",
        type=float,
        default=0.02,
        help="Minimum YES+NO edge to alert (default: 0.02).",
    )
    parser.add_argument(
        "--deviation-threshold",
        type=float,
        default=0.03,
        help="Minimum same-token cross-market ask gap to alert (default: 0.03).",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=30.0,
        help="Per-opportunity cooldown before re-alerting (default: 30).",
    )
    parser.add_argument(
        "--mapping-refresh-seconds",
        type=float,
        default=60.0,
        help="Refresh mapping set from API every N seconds (default: 60).",
    )
    parser.add_argument(
        "--book-stale-seconds",
        type=float,
        default=15.0,
        help="Ignore quotes older than this many seconds (default: 15).",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Optional PMXT watch_order_book depth limit.",
    )
    parser.add_argument(
        "--include-expired",
        action="store_true",
        help="Include expired pair sets in mapping source.",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive outcome links in mapping source.",
    )
    return parser.parse_args()


def _http_get_json(
    base_url: str,
    path: str,
    *,
    query: dict[str, str] | None = None,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    url = f"{base}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"

    request = Request(url=url, method="GET")
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected non-object JSON from {url}")
        return payload


def _build_exchange(exchange: str) -> Any:
    if exchange == "kalshi":
        return pmxt.Kalshi(**build_kalshi_client_kwargs())
    if exchange == "polymarket":
        return pmxt.Polymarket()
    raise ValueError(f"Unsupported exchange: {exchange}")


class ArbAlertRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

        self.stop_event = threading.Event()
        self._worker_stop_event = threading.Event()

        self._state_lock = threading.Lock()
        self._print_lock = threading.Lock()

        self._mappings = []
        self._quotes: dict[tuple[str, str], QuoteSnapshot] = {}
        self._last_emitted_at: dict[str, float] = {}
        self._active_stream_keys: set[tuple[str, str]] = set()

        self._worker_threads: dict[tuple[str, str], threading.Thread] = {}

    def _print_line(self, line: str) -> None:
        with self._print_lock:
            print(line)

    def _fetch_monitoring_pairs(self) -> dict[str, Any]:
        payload = _http_get_json(
            self.args.api_base_url,
            "/api/monitoring/pairs",
            query={
                "active_only": "false" if self.args.include_inactive else "true",
                "include_expired": "true" if self.args.include_expired else "false",
            },
        )
        if payload.get("ok") is not True:
            raise RuntimeError(
                "Monitoring API query failed: "
                f"error={payload.get('error')} code={payload.get('error_code')}"
            )
        return payload

    def _fetch_pair_details(self, pair_id: int) -> dict[str, Any]:
        payload = _http_get_json(self.args.api_base_url, f"/api/pairs/{pair_id}")
        if payload.get("ok") is not True:
            raise RuntimeError(
                f"Pair detail query failed for pair_id={pair_id}: "
                f"error={payload.get('error')} code={payload.get('error_code')}"
            )
        return payload

    def _load_semantic_mappings(self) -> tuple[list[Any], list[str]]:
        monitoring_payload = self._fetch_monitoring_pairs()
        rows = normalize_monitoring_rows(monitoring_payload)

        pair_ids = sorted({row.pair_id for row in rows})
        pair_details_by_id: dict[int, dict[str, Any]] = {}
        detail_warnings: list[str] = []
        for pair_id in pair_ids:
            try:
                pair_details_by_id[pair_id] = self._fetch_pair_details(pair_id)
            except Exception as exc:  # pragma: no cover - operational path
                detail_warnings.append(
                    f"Failed to load pair detail for pair_id={pair_id}; skipping pair: {exc}"
                )

        usable_rows = [row for row in rows if row.pair_id in pair_details_by_id]
        mappings, warnings = build_semantic_mappings(usable_rows, pair_details_by_id)
        return mappings, [*detail_warnings, *warnings]

    def _restart_workers(self, stream_keys: set[tuple[str, str]]) -> None:
        self._worker_stop_event.set()
        for thread in self._worker_threads.values():
            thread.join(timeout=2.0)

        self._worker_threads = {}
        self._worker_stop_event = threading.Event()

        for exchange, outcome_id in sorted(stream_keys):
            thread = threading.Thread(
                target=self._worker,
                kwargs={
                    "exchange": exchange,
                    "outcome_id": outcome_id,
                    "worker_stop_event": self._worker_stop_event,
                },
                daemon=True,
            )
            self._worker_threads[(exchange, outcome_id)] = thread
            thread.start()

    def _refresh_mappings(self, *, reason: str) -> None:
        mappings, warnings = self._load_semantic_mappings()
        for warning in warnings:
            self._print_line(f"[warn] {warning}")

        next_stream_keys = mapping_stream_keys(mappings)

        with self._state_lock:
            previous_stream_keys = set(self._active_stream_keys)
            self._mappings = mappings
            self._active_stream_keys = set(next_stream_keys)

        if next_stream_keys != previous_stream_keys:
            self._restart_workers(next_stream_keys)
            self._print_line(
                "[info] restarted stream workers "
                f"reason={reason} streams={len(next_stream_keys)}"
            )

        self._print_line(
            "[info] mapping refresh "
            f"reason={reason} mappings={len(mappings)} streams={len(next_stream_keys)}"
        )
        self._evaluate_and_emit()

    def _worker(
        self,
        *,
        exchange: str,
        outcome_id: str,
        worker_stop_event: threading.Event,
    ) -> None:
        backoff_seconds = 1.0

        while not self.stop_event.is_set() and not worker_stop_event.is_set():
            client = None
            try:
                client = _build_exchange(exchange)
                while not self.stop_event.is_set() and not worker_stop_event.is_set():
                    try:
                        book = client.watch_order_book(outcome_id, limit=self.args.depth)
                    except Exception as exc:  # pragma: no cover - operational visibility
                        self._print_line(
                            f"[error] stream failure exchange={exchange} "
                            f"outcome_id={outcome_id} error={exc}"
                        )
                        break

                    best_bid, best_ask = extract_best_prices(book)
                    quote = QuoteSnapshot(
                        exchange=exchange,
                        outcome_id=outcome_id,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        book_timestamp_ms=extract_book_timestamp_ms(book),
                        updated_at=time.time(),
                    )
                    with self._state_lock:
                        self._quotes[(exchange, outcome_id)] = quote
                    self._evaluate_and_emit()

                    # Keep tight loops from spinning if the watch call returns immediately.
                    time.sleep(0.05)
            finally:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

            if self.stop_event.is_set() or worker_stop_event.is_set():
                return
            time.sleep(backoff_seconds)

    def _evaluate_and_emit(self) -> None:
        now = time.time()
        with self._state_lock:
            events = evaluate_all_mappings(
                self._mappings,
                quotes_by_stream=self._quotes,
                now=now,
                arb_threshold=self.args.arb_threshold,
                deviation_threshold=self.args.deviation_threshold,
                stale_after_seconds=self.args.book_stale_seconds,
            )

            lines: list[str] = []
            for event in events:
                if not passes_cooldown(
                    now=now,
                    alert_key=event.key,
                    cooldown_seconds=self.args.cooldown_seconds,
                    last_emitted_at_by_key=self._last_emitted_at,
                ):
                    continue
                lines.append(
                    format_alert_line(
                        event,
                        emitted_at=datetime.fromtimestamp(now, tz=timezone.utc),
                    )
                )

        for line in lines:
            self._print_line(line)

    def run(self) -> int:
        self._print_line(
            "[info] starting ws arb alert runner "
            f"api_base_url={self.args.api_base_url} "
            f"arb_threshold={self.args.arb_threshold} "
            f"deviation_threshold={self.args.deviation_threshold} "
            f"cooldown_seconds={self.args.cooldown_seconds} "
            f"mapping_refresh_seconds={self.args.mapping_refresh_seconds} "
            f"book_stale_seconds={self.args.book_stale_seconds} "
            f"depth={self.args.depth}"
        )

        try:
            self._refresh_mappings(reason="startup")
        except Exception as exc:
            self._print_line(f"[error] initial mapping refresh failed: {exc}")
            return 1

        next_refresh_at = time.time() + max(1.0, self.args.mapping_refresh_seconds)

        try:
            while True:
                if self.stop_event.is_set():
                    break

                now = time.time()
                if now >= next_refresh_at:
                    try:
                        self._refresh_mappings(reason="periodic")
                    except Exception as exc:  # pragma: no cover - operational visibility
                        self._print_line(f"[error] mapping refresh failed: {exc}")
                    next_refresh_at = now + max(1.0, self.args.mapping_refresh_seconds)

                time.sleep(0.25)
        except KeyboardInterrupt:
            self._print_line("\n[info] Ctrl+C received; stopping runner...")
            return 130
        finally:
            self.stop_event.set()
            self._worker_stop_event.set()
            for thread in self._worker_threads.values():
                thread.join(timeout=2.0)

        return 0


def main() -> int:
    args = _parse_args()
    urllib3_major = int(urllib3.__version__.split(".", maxsplit=1)[0])
    if urllib3_major < 2:
        print(
            f"[error] urllib3={urllib3.__version__} detected; PMXT WebSocket/orderbook calls "
            "may fail with PoolKey key_ca_cert_data. Use urllib3>=2."
        )
        return 1

    runner = ArbAlertRunner(args)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
