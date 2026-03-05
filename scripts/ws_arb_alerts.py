#!/usr/bin/env python3
"""Continuously monitor mapped Kalshi/Polymarket outcomes and print arb alerts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
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
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    parser.add_argument(
        "--debug-heartbeat-seconds",
        type=float,
        default=10.0,
        help="Emit periodic debug heartbeat every N seconds when --debug is enabled.",
    )
    parser.add_argument(
        "--compact-alerts",
        action="store_true",
        help="Emit single-line alerts only (disable pretty multi-line formatting).",
    )
    parser.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default="auto",
        help="Terminal color mode for pretty alerts (default: auto).",
    )
    parser.add_argument(
        "--skip-pmxt-sidecar-restart",
        action="store_true",
        help=(
            "Do not restart PMXT sidecar on startup. By default the runner restarts "
            "the sidecar after loading resolved Kalshi credentials for shared WS sessions."
        ),
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


def _build_exchange(
    exchange: str,
    *,
    use_kalshi_sidecar_default: bool = True,
    kalshi_direct_kwargs: dict[str, str] | None = None,
) -> Any:
    if exchange == "kalshi":
        if use_kalshi_sidecar_default:
            # Use sidecar default exchange so PMXT reuses a single Kalshi WS client.
            return pmxt.Kalshi()
        kwargs = kalshi_direct_kwargs or {}
        return pmxt.Kalshi(**kwargs)
    if exchange == "polymarket":
        return pmxt.Polymarket()
    raise ValueError(f"Unsupported exchange: {exchange}")


class ArbAlertRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self._debug_enabled = bool(getattr(args, "debug", False))
        self._debug_heartbeat_seconds = max(
            1.0,
            float(getattr(args, "debug_heartbeat_seconds", 10.0)),
        )

        self.stop_event = threading.Event()
        self._worker_stop_event = threading.Event()

        self._state_lock = threading.Lock()
        self._print_lock = threading.Lock()

        self._mappings = []
        self._quotes: dict[tuple[str, str], QuoteSnapshot] = {}
        self._last_emitted_at: dict[str, float] = {}
        self._active_stream_keys: set[tuple[str, str]] = set()
        self._exchange_retry_not_before: dict[str, float] = {}
        self._exchange_backoff_seconds: dict[str, float] = {}
        self._next_rate_limit_log_at: dict[tuple[str, str], float] = {}
        self._watch_locks: dict[str, threading.Lock] = {
            "kalshi": threading.Lock(),
            "polymarket": threading.Lock(),
        }
        self._client_lock = threading.Lock()
        self._exchange_clients: dict[str, Any] = {}
        self._stream_watch_success_count: dict[tuple[str, str], int] = {}
        self._stream_watch_error_count: dict[tuple[str, str], int] = {}
        self._stream_last_watch_latency_ms: dict[tuple[str, str], float] = {}
        self._stream_last_watch_ok_at: dict[tuple[str, str], float] = {}
        self._exchange_rate_limit_count: dict[str, int] = {}

        self._worker_threads: dict[tuple[str, str], threading.Thread] = {}
        self._kalshi_direct_kwargs = build_kalshi_client_kwargs()
        self._use_kalshi_sidecar_default = True
        self._compact_alerts = bool(getattr(args, "compact_alerts", False))
        self._use_color = self._should_use_color(getattr(args, "color", "auto"))

    def _print_line(self, line: str) -> None:
        with self._print_lock:
            print(line)

    def _debug(self, line: str) -> None:
        if self._debug_enabled:
            self._print_line(f"[debug] {line}")

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
        self._debug(
            f"monitoring payload loaded raw_pairs={len(monitoring_payload.get('pairs') or [])} "
            f"rows={len(rows)}"
        )

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
        self._debug(
            f"semantic mappings built pair_ids={pair_ids} usable_rows={len(usable_rows)} "
            f"mappings={len(mappings)} warnings={len(warnings)}"
        )
        return mappings, [*detail_warnings, *warnings]

    def _restart_workers(self, stream_keys: set[tuple[str, str]]) -> None:
        self._worker_stop_event.set()
        for thread in self._worker_threads.values():
            thread.join(timeout=2.0)

        self._worker_threads = {}
        self._worker_stop_event = threading.Event()
        self._close_all_exchange_clients()

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
            self._debug(f"worker started exchange={exchange} outcome_id={outcome_id}")

    def _refresh_mappings(self, *, reason: str) -> None:
        mappings, warnings = self._load_semantic_mappings()
        for warning in warnings:
            self._print_line(f"[warn] {warning}")

        next_stream_keys = mapping_stream_keys(mappings, canonicalize_kalshi=True)

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

    def _is_rate_limited_error(self, exc: Exception) -> bool:
        return "429" in str(exc)

    def _register_rate_limit(self, *, exchange: str, outcome_id: str, exc: Exception) -> None:
        now = time.time()
        with self._state_lock:
            previous_backoff = self._exchange_backoff_seconds.get(exchange, 1.0)
            next_backoff = min(max(previous_backoff * 2.0, 2.0), 60.0)
            self._exchange_backoff_seconds[exchange] = next_backoff
            retry_not_before = max(
                self._exchange_retry_not_before.get(exchange, 0.0),
                now + next_backoff,
            )
            self._exchange_retry_not_before[exchange] = retry_not_before

            log_key = (exchange, outcome_id)
            should_log = now >= self._next_rate_limit_log_at.get(log_key, 0.0)
            if should_log:
                self._next_rate_limit_log_at[log_key] = now + 10.0

        if should_log:
            delay = max(0.0, retry_not_before - now)
            self._print_line(
                f"[warn] rate limit exchange={exchange} outcome_id={outcome_id} "
                f"retry_in={delay:.1f}s backoff={next_backoff:.1f}s error={exc}"
            )
            self._debug(
                f"rate-limit-detail exchange={exchange} outcome_id={outcome_id} "
                f"error_type={type(exc).__name__} retry_not_before={retry_not_before:.3f}"
            )

    def _mark_stream_success(self, exchange: str) -> None:
        with self._state_lock:
            if self._exchange_backoff_seconds.get(exchange, 1.0) > 1.0:
                self._exchange_backoff_seconds[exchange] = 1.0
            self._exchange_retry_not_before[exchange] = 0.0

    def _wait_for_exchange_retry(
        self,
        *,
        exchange: str,
        worker_stop_event: threading.Event,
    ) -> None:
        while not self.stop_event.is_set() and not worker_stop_event.is_set():
            with self._state_lock:
                retry_not_before = self._exchange_retry_not_before.get(exchange, 0.0)
            now = time.time()
            if retry_not_before <= now:
                return
            time.sleep(min(1.0, retry_not_before - now))

    def _watch_order_book(self, *, client: Any, exchange: str, outcome_id: str) -> Any:
        watch_lock = self._watch_locks.get(exchange)
        if watch_lock is None:
            return client.watch_order_book(outcome_id, limit=self.args.depth)
        with watch_lock:
            return client.watch_order_book(outcome_id, limit=self.args.depth)

    def _loop_delay_seconds(self, exchange: str) -> float:
        # Kalshi is stricter on websocket rate limits; keep request cadence lower.
        if exchange == "kalshi":
            return 0.25
        return 0.05

    def _get_exchange_client(self, exchange: str) -> Any:
        with self._client_lock:
            client = self._exchange_clients.get(exchange)
            if client is None:
                client = _build_exchange(
                    exchange,
                    use_kalshi_sidecar_default=self._use_kalshi_sidecar_default,
                    kalshi_direct_kwargs=self._kalshi_direct_kwargs,
                )
                self._exchange_clients[exchange] = client
                self._debug(
                    f"exchange client created exchange={exchange} client_id={id(client)}"
                )
            return client

    def _prepare_kalshi_sidecar_credentials(self) -> bool:
        api_key = self._kalshi_direct_kwargs.get("api_key")
        private_key = self._kalshi_direct_kwargs.get("private_key")
        if not api_key or not private_key:
            self._print_line(
                "[warn] Kalshi credentials missing; falling back to direct PMXT credential mode "
                "(this may trigger websocket rate limits)."
            )
            return False

        os.environ["KALSHI_API_KEY"] = api_key
        os.environ["KALSHI_PRIVATE_KEY"] = private_key
        self._debug("loaded resolved Kalshi credentials into process env for PMXT sidecar")
        return True

    def _prepare_pmxt_sidecar(self) -> None:
        if not self._prepare_kalshi_sidecar_credentials():
            self._use_kalshi_sidecar_default = False
            self._print_line(
                "[warn] kalshi_client_mode=direct_credentials reason=missing_resolved_credentials"
            )
            return

        if bool(getattr(self.args, "skip_pmxt_sidecar_restart", False)):
            self._print_line(
                "[warn] skipping PMXT sidecar restart; ensure sidecar already has resolved "
                "Kalshi credentials to avoid WS reconnect rate limits."
            )
            self._print_line(
                "[info] kalshi_client_mode=sidecar_default reason=skip_restart_requested"
            )
            return

        try:
            pmxt.restart_server()
            self._debug("pmxt sidecar restarted with resolved Kalshi credentials")
            self._print_line(
                "[info] kalshi_client_mode=sidecar_default reason=sidecar_restarted_with_resolved_credentials"
            )
        except Exception as exc:
            self._use_kalshi_sidecar_default = False
            self._print_line(
                "[warn] failed to restart PMXT sidecar with resolved Kalshi credentials; "
                f"falling back to direct PMXT credential mode error={exc}"
            )
            self._print_line(
                "[warn] kalshi_client_mode=direct_credentials reason=sidecar_restart_failed"
            )

    def _close_exchange_client(self, exchange: str) -> None:
        with self._client_lock:
            client = self._exchange_clients.pop(exchange, None)
        if client is None:
            return
        try:
            client.close()
        except Exception:
            pass
        self._debug(f"exchange client closed exchange={exchange} client_id={id(client)}")

    def _close_all_exchange_clients(self) -> None:
        with self._client_lock:
            exchanges = list(self._exchange_clients.keys())
        for exchange in exchanges:
            self._close_exchange_client(exchange)

    def _should_use_color(self, mode: str) -> bool:
        normalized = (mode or "auto").lower()
        if normalized == "always":
            return True
        if normalized == "never":
            return False
        if os.getenv("NO_COLOR"):
            return False
        return bool(getattr(sys.stdout, "isatty", lambda: False)())

    def _colorize(self, value: str, code: str) -> str:
        if not self._use_color:
            return value
        return f"\033[{code}m{value}\033[0m"

    def _tag_accent(self, tag: str) -> str:
        if tag == "ALERT_ARB_CROSS":
            return self._colorize("cross-market set edge", "1;32")
        if tag == "ALERT_ARB_WITHIN":
            return self._colorize("within-market set edge", "1;36")
        return self._colorize("cross-market deviation", "1;33")

    def _metric_summary(self, event: Any) -> tuple[str, str]:
        if event.metric_name == "cross_market_set_1":
            return (
                "Buy Kalshi P ask + Polymarket NOT_P ask",
                f"Kalshi P ask={event.details.get('kalshi_p_ask', 'na')} | "
                f"Polymarket NOT_P ask={event.details.get('polymarket_not_p_ask', 'na')}",
            )
        if event.metric_name == "cross_market_set_2":
            return (
                "Buy Polymarket P ask + Kalshi NOT_P ask",
                f"Polymarket P ask={event.details.get('polymarket_p_ask', 'na')} | "
                f"Kalshi NOT_P ask={event.details.get('kalshi_not_p_ask', 'na')}",
            )
        if event.metric_name == "within_kalshi":
            return (
                "Buy both sides on Kalshi",
                f"Kalshi P ask={event.details.get('kalshi_p_ask', 'na')} | "
                f"Kalshi NOT_P ask={event.details.get('kalshi_not_p_ask', 'na')}",
            )
        if event.metric_name == "within_polymarket":
            return (
                "Buy both sides on Polymarket",
                f"Polymarket P ask={event.details.get('polymarket_p_ask', 'na')} | "
                f"Polymarket NOT_P ask={event.details.get('polymarket_not_p_ask', 'na')}",
            )
        if event.metric_name == "same_token_gap_p":
            return (
                "Same token P ask-price gap across markets",
                f"Kalshi P ask={event.details.get('kalshi_p_ask', 'na')} | "
                f"Polymarket P ask={event.details.get('polymarket_p_ask', 'na')}",
            )
        if event.metric_name == "same_token_gap_not_p":
            return (
                "Same token NOT_P ask-price gap across markets",
                f"Kalshi NOT_P ask={event.details.get('kalshi_not_p_ask', 'na')} | "
                f"Polymarket NOT_P ask={event.details.get('polymarket_not_p_ask', 'na')}",
            )
        return ("Opportunity detected", "Quote legs unavailable")

    def _format_alert_block(self, event: Any, *, emitted_at: datetime) -> str:
        if self._compact_alerts:
            return format_alert_line(event, emitted_at=emitted_at)

        summary, legs = self._metric_summary(event)
        value = self._colorize(f"{event.metric_value:.4f}", "1;32")
        threshold = self._colorize(f"{event.threshold:.4f}", "1;37")
        markets = (
            f"Kalshi={event.kalshi_market_id} | "
            f"Polymarket={event.polymarket_market_id}"
        )
        metric = self._colorize(event.metric_name, "35")
        relation = self._colorize(event.relation_type, "36")
        divider = self._colorize("-" * 96, "2")

        return "\n".join(
            [
                (
                    f"[{event.tag}] ts={emitted_at.isoformat()} pair_id={event.pair_id} "
                    f"{self._tag_accent(event.tag)}"
                ),
                f"  What: {summary}",
                f"  Metric: {metric} | value={value} threshold={threshold} | relation={relation}",
                f"  Markets: {self._colorize(markets, '2')}",
                f"  Legs: {legs}",
                "  Note: Informational top-of-book signal only (fees, depth, and execution risk excluded).",
                divider,
            ]
        )

    def _worker(
        self,
        *,
        exchange: str,
        outcome_id: str,
        worker_stop_event: threading.Event,
    ) -> None:
        backoff_seconds = 1.0
        stream_key = (exchange, outcome_id)
        self._debug(f"worker loop entering exchange={exchange} outcome_id={outcome_id}")

        while not self.stop_event.is_set() and not worker_stop_event.is_set():
            self._wait_for_exchange_retry(
                exchange=exchange,
                worker_stop_event=worker_stop_event,
            )
            if self.stop_event.is_set() or worker_stop_event.is_set():
                return

            client = self._get_exchange_client(exchange)
            while not self.stop_event.is_set() and not worker_stop_event.is_set():
                self._wait_for_exchange_retry(
                    exchange=exchange,
                    worker_stop_event=worker_stop_event,
                )
                if self.stop_event.is_set() or worker_stop_event.is_set():
                    return
                try:
                    watch_start = time.time()
                    book = self._watch_order_book(
                        client=client,
                        exchange=exchange,
                        outcome_id=outcome_id,
                    )
                    watch_latency_ms = (time.time() - watch_start) * 1000.0
                except Exception as exc:  # pragma: no cover - operational visibility
                    with self._state_lock:
                        self._stream_watch_error_count[stream_key] = (
                            self._stream_watch_error_count.get(stream_key, 0) + 1
                        )
                    if self._is_rate_limited_error(exc):
                        with self._state_lock:
                            self._exchange_rate_limit_count[exchange] = (
                                self._exchange_rate_limit_count.get(exchange, 0) + 1
                            )
                        self._register_rate_limit(
                            exchange=exchange,
                            outcome_id=outcome_id,
                            exc=exc,
                        )
                        continue
                    self._print_line(
                        f"[error] stream failure exchange={exchange} "
                        f"outcome_id={outcome_id} error={exc}"
                    )
                    self._debug(
                        f"non-rate-limit failure exchange={exchange} outcome_id={outcome_id} "
                        f"error_type={type(exc).__name__}"
                    )
                    # Force re-init of this exchange client on non-rate-limit failures.
                    self._close_exchange_client(exchange)
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
                    self._stream_watch_success_count[stream_key] = (
                        self._stream_watch_success_count.get(stream_key, 0) + 1
                    )
                    self._stream_last_watch_latency_ms[stream_key] = watch_latency_ms
                    self._stream_last_watch_ok_at[stream_key] = time.time()
                self._mark_stream_success(exchange)
                self._evaluate_and_emit()

                # Keep tight loops from spinning if the watch call returns immediately.
                time.sleep(self._loop_delay_seconds(exchange))

            if self.stop_event.is_set() or worker_stop_event.is_set():
                return
            time.sleep(backoff_seconds)
        self._debug(f"worker loop exiting exchange={exchange} outcome_id={outcome_id}")

    def _emit_debug_heartbeat(self, now: float) -> None:
        with self._state_lock:
            active_streams = sorted(self._active_stream_keys)
            retry_windows = {
                exchange: max(0.0, ts - now)
                for exchange, ts in self._exchange_retry_not_before.items()
            }
            backoffs = dict(self._exchange_backoff_seconds)
            rate_limit_counts = dict(self._exchange_rate_limit_count)
            success_counts = dict(self._stream_watch_success_count)
            error_counts = dict(self._stream_watch_error_count)
            latencies = dict(self._stream_last_watch_latency_ms)
            last_ok = dict(self._stream_last_watch_ok_at)
            quote_count = len(self._quotes)

        with self._client_lock:
            client_exchanges = sorted(self._exchange_clients.keys())

        self._debug(
            "heartbeat "
            f"active_streams={len(active_streams)} quote_count={quote_count} "
            f"clients={client_exchanges} backoffs={backoffs} retry_in={retry_windows} "
            f"rate_limit_counts={rate_limit_counts}"
        )
        for stream_key in active_streams:
            success = success_counts.get(stream_key, 0)
            errors = error_counts.get(stream_key, 0)
            latency = latencies.get(stream_key)
            ok_age = (now - last_ok[stream_key]) if stream_key in last_ok else None
            self._debug(
                f"stream exchange={stream_key[0]} outcome_id={stream_key[1]} "
                f"ok={success} err={errors} "
                f"last_latency_ms={latency if latency is not None else 'na'} "
                f"last_ok_age_s={ok_age if ok_age is not None else 'na'}"
            )

    def _evaluate_and_emit(self) -> None:
        now = time.time()
        emitted_at = datetime.fromtimestamp(now, tz=timezone.utc)
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
                lines.append(self._format_alert_block(event, emitted_at=emitted_at))

        for line in lines:
            self._print_line(line)

    def run(self) -> int:
        self._prepare_pmxt_sidecar()
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
        self._debug(
            f"debug mode enabled heartbeat_seconds={self._debug_heartbeat_seconds}"
        )

        try:
            self._refresh_mappings(reason="startup")
        except Exception as exc:
            self._print_line(f"[error] initial mapping refresh failed: {exc}")
            return 1

        next_refresh_at = time.time() + max(1.0, self.args.mapping_refresh_seconds)
        next_debug_heartbeat_at = time.time() + self._debug_heartbeat_seconds

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

                if self._debug_enabled and now >= next_debug_heartbeat_at:
                    self._emit_debug_heartbeat(now)
                    next_debug_heartbeat_at = now + self._debug_heartbeat_seconds

                time.sleep(0.25)
        except KeyboardInterrupt:
            self._print_line("\n[info] Ctrl+C received; stopping runner...")
            return 130
        finally:
            self.stop_event.set()
            self._worker_stop_event.set()
            for thread in self._worker_threads.values():
                thread.join(timeout=2.0)
            self._close_all_exchange_clients()

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
