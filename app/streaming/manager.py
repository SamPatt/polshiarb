from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import os
import random
import sys
import threading
import time
from typing import Any, Callable

import pmxt

from app.arb_alerts import (
    QuoteSnapshot,
    build_semantic_mappings,
    extract_best_prices,
    extract_book_timestamp_ms,
    format_alert_line,
    mapping_stream_keys,
    normalize_monitoring_rows,
)
from app.kalshi_credentials import build_kalshi_client_kwargs

from .polymarket_ws import PolymarketMarketWebSocketClient
from .store import QuoteStore
from .types import DebugFn, ExchangeBuilder, ExchangeIngestor, HealthState, PrintFn, StreamKey


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = int(round((len(ordered) - 1) * pct))
    return ordered[max(0, min(len(ordered) - 1, idx))]


@dataclass(frozen=True)
class QueuedQuoteUpdate:
    quote: QuoteSnapshot
    watch_latency_ms: float
    received_at: float
    worker_index: int | None = None


class RollingLatencyTracker:
    def __init__(self, *, sample_limit: int = 4096) -> None:
        self._sample_limit = max(1, sample_limit)
        self._samples_by_metric: dict[str, dict[str, deque[float]]] = {}

    def add(
        self,
        *,
        metric_name: str,
        exchange: str,
        source_label: str | None,
        value_ms: float | None,
    ) -> None:
        if value_ms is None:
            return
        normalized_value = float(value_ms)
        bucket_names = ["overall", exchange]
        if source_label:
            bucket_names.append(f"{exchange}:{source_label}")
        for bucket_name in bucket_names:
            metric_samples = self._samples_by_metric.setdefault(metric_name, {})
            bucket = metric_samples.get(bucket_name)
            if bucket is None:
                bucket = deque(maxlen=self._sample_limit)
                metric_samples[bucket_name] = bucket
            bucket.append(normalized_value)

    def sample_counts(self, metric_name: str) -> dict[str, int]:
        metric_samples = self._samples_by_metric.get(metric_name, {})
        return {
            bucket_name: len(values)
            for bucket_name, values in sorted(metric_samples.items())
            if values
        }

    def percentile_summary(
        self,
        metric_name: str,
        *,
        pct: float,
    ) -> dict[str, float]:
        metric_samples = self._samples_by_metric.get(metric_name, {})
        summary: dict[str, float] = {}
        for bucket_name, values in sorted(metric_samples.items()):
            percentile_value = _percentile(list(values), pct)
            if percentile_value is not None:
                summary[bucket_name] = percentile_value
        return summary


class LegacyExchangeIngestor:
    def __init__(
        self,
        *,
        manager: LegacySubscriptionManager,
        exchange: str,
        outcome_id: str,
        worker_stop_event: threading.Event,
    ) -> None:
        self._manager = manager
        self.exchange = exchange
        self.outcome_id = outcome_id
        self.stream_key = (exchange, outcome_id)
        self._worker_stop_event = worker_stop_event
        self._local_stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._local_stop_event.set()

    def join(self, timeout_seconds: float) -> bool:
        self._thread.join(timeout=max(0.0, timeout_seconds))
        return not self._thread.is_alive()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def should_stop(self) -> bool:
        return (
            self._manager.stop_event.is_set()
            or self._worker_stop_event.is_set()
            or self._local_stop_event.is_set()
        )

    def _run(self) -> None:
        self._manager.worker_loop(self)


class MultiplexExchangeIngestor:
    def __init__(
        self,
        *,
        manager: MultiplexSubscriptionManager,
        exchange: str,
    ) -> None:
        self._manager = manager
        self.exchange = exchange
        self.stream_key = (exchange, "__multiplex__")
        self._local_stop_event = threading.Event()
        self._subscriptions_lock = threading.Lock()
        self._subscriptions: list[str] = []
        self._pending_subscriptions: deque[str] = deque()
        self._priority_subscriptions: deque[str] = deque()
        self._forced_fetch_outcome_ids: set[str] = set()
        self._active_since: dict[str, float] = {}
        self._pending_since: dict[str, float] = {}
        self._dispatch_queue_lock = threading.Lock()
        self._dispatch_queue: deque[QueuedQuoteUpdate] = deque()
        self._dropped_dispatch_count = 0
        self._next_activation_at = 0.0
        self._cursor = 0
        self._dispatch_queue_capacity = max(
            1,
            int(getattr(self._manager.args, "multiplex_dispatch_queue_size", 100)),
        )
        producer_thread_count = self._manager.producer_thread_count(self.exchange)
        self._producer_thread_count = producer_thread_count
        self._producer_threads = [
            threading.Thread(
                target=self._run,
                kwargs={"worker_index": worker_index},
                daemon=True,
            )
            for worker_index in range(producer_thread_count)
        ]
        self._auxiliary_threads: list[threading.Thread] = []
        if self._manager.should_start_direct_quiet_refresh_thread(self.exchange):
            self._auxiliary_threads.append(
                threading.Thread(target=self._run_direct_quiet_refresh, daemon=True)
            )
        self._dispatch_thread = threading.Thread(target=self._run_dispatcher, daemon=True)

    def start(self) -> None:
        for producer_thread in self._producer_threads:
            producer_thread.start()
        for auxiliary_thread in self._auxiliary_threads:
            auxiliary_thread.start()
        self._dispatch_thread.start()

    def stop(self) -> None:
        self._local_stop_event.set()

    def join(self, timeout_seconds: float) -> bool:
        deadline = time.time() + max(0.0, timeout_seconds)
        for producer_thread in self._producer_threads:
            remaining = max(0.0, deadline - time.time())
            producer_thread.join(timeout=remaining)
        for auxiliary_thread in self._auxiliary_threads:
            remaining = max(0.0, deadline - time.time())
            auxiliary_thread.join(timeout=remaining)
        remaining = max(0.0, deadline - time.time())
        self._dispatch_thread.join(timeout=remaining)
        return not self.is_alive()

    def is_alive(self) -> bool:
        return (
            any(thread.is_alive() for thread in self._producer_threads)
            or any(thread.is_alive() for thread in self._auxiliary_threads)
            or self._dispatch_thread.is_alive()
        )

    def producer_thread_count(self) -> int:
        return self._producer_thread_count

    def should_stop(self) -> bool:
        return self._manager.stop_event.is_set() or self._local_stop_event.is_set()

    def replace_subscriptions(self, outcome_ids: set[str]) -> None:
        with self._subscriptions_lock:
            now = time.time()
            if not self._manager.warmup_enabled(self.exchange):
                self._subscriptions = sorted(outcome_ids)
                self._pending_subscriptions = deque()
                self._active_since = {outcome_id: now for outcome_id in self._subscriptions}
                self._pending_since = {}
                self._next_activation_at = 0.0
            else:
                ordered_outcome_ids = sorted(outcome_ids)
                batch_size = self._manager.warmup_batch_size(self.exchange)
                self._subscriptions = ordered_outcome_ids[:batch_size]
                self._pending_subscriptions = deque(ordered_outcome_ids[batch_size:])
                self._active_since = {outcome_id: now for outcome_id in self._subscriptions}
                self._pending_since = {
                    outcome_id: now for outcome_id in self._pending_subscriptions
                }
                self._next_activation_at = self._next_activation_time_locked()
            if not self._subscriptions:
                self._cursor = 0
            else:
                self._cursor %= len(self._subscriptions)
            self._priority_subscriptions = deque(
                outcome_id
                for outcome_id in self._priority_subscriptions
                if outcome_id in self._subscriptions
            )
            self._forced_fetch_outcome_ids = {
                outcome_id
                for outcome_id in self._forced_fetch_outcome_ids
                if outcome_id in self._subscriptions
            }

    def outcome_ids(self) -> set[str]:
        with self._subscriptions_lock:
            return set(self._subscriptions) | set(self._pending_subscriptions)

    def active_outcome_ids(self) -> set[str]:
        with self._subscriptions_lock:
            return set(self._subscriptions)

    def pending_outcome_ids(self) -> set[str]:
        with self._subscriptions_lock:
            return set(self._pending_subscriptions)

    def priority_outcome_ids(self) -> set[str]:
        with self._subscriptions_lock:
            return set(self._priority_subscriptions)

    def forced_fetch_outcome_ids(self) -> set[str]:
        with self._subscriptions_lock:
            return set(self._forced_fetch_outcome_ids)

    def active_since_by_outcome_id(self) -> dict[str, float]:
        with self._subscriptions_lock:
            return dict(self._active_since)

    def dispatch_queue_depth(self) -> int:
        with self._dispatch_queue_lock:
            return len(self._dispatch_queue)

    def dropped_dispatch_count(self) -> int:
        with self._dispatch_queue_lock:
            return self._dropped_dispatch_count

    def subscription_keys(self) -> set[StreamKey]:
        with self._subscriptions_lock:
            return {(self.exchange, outcome_id) for outcome_id in self._subscriptions}

    def prioritize_outcome_ids(self, outcome_ids: list[str]) -> int:
        with self._subscriptions_lock:
            active_outcomes = set(self._subscriptions)
            existing_priority = set(self._priority_subscriptions)
            prioritized: list[str] = []
            for outcome_id in outcome_ids:
                if outcome_id not in active_outcomes or outcome_id in existing_priority:
                    continue
                self._priority_subscriptions.append(outcome_id)
                existing_priority.add(outcome_id)
                prioritized.append(outcome_id)
            return len(prioritized)

    def request_forced_fetch_outcome_ids(self, outcome_ids: list[str]) -> int:
        with self._subscriptions_lock:
            active_outcomes = set(self._subscriptions)
            existing_priority = set(self._priority_subscriptions)
            requested_count = 0
            for outcome_id in outcome_ids:
                if outcome_id not in active_outcomes:
                    continue
                if outcome_id not in existing_priority:
                    self._priority_subscriptions.append(outcome_id)
                    existing_priority.add(outcome_id)
                if outcome_id in self._forced_fetch_outcome_ids:
                    continue
                self._forced_fetch_outcome_ids.add(outcome_id)
                requested_count += 1
            return requested_count

    def should_force_fetch_outcome_id(self, outcome_id: str) -> bool:
        with self._subscriptions_lock:
            return outcome_id in self._forced_fetch_outcome_ids

    def clear_forced_fetch_outcome_id(self, outcome_id: str) -> None:
        with self._subscriptions_lock:
            self._forced_fetch_outcome_ids.discard(outcome_id)

    def apply_subscription_diff(
        self,
        *,
        add_outcome_ids: set[str],
        remove_outcome_ids: set[str],
    ) -> tuple[int, int, int, int, int]:
        with self._subscriptions_lock:
            now = time.time()
            if remove_outcome_ids:
                self._subscriptions = [
                    outcome_id
                    for outcome_id in self._subscriptions
                    if outcome_id not in remove_outcome_ids
                ]
                for outcome_id in remove_outcome_ids:
                    self._active_since.pop(outcome_id, None)
                if self._pending_subscriptions:
                    kept_pending = [
                        outcome_id
                        for outcome_id in self._pending_subscriptions
                        if outcome_id not in remove_outcome_ids
                    ]
                    self._pending_subscriptions = deque(kept_pending)
                for outcome_id in remove_outcome_ids:
                    self._pending_since.pop(outcome_id, None)

            desired_outcome_ids = set(self._subscriptions) | set(self._pending_subscriptions)
            desired_outcome_ids.update(add_outcome_ids)
            keep_count = len(desired_outcome_ids) - len(add_outcome_ids)

            if not self._manager.warmup_enabled(self.exchange):
                self._subscriptions = sorted(desired_outcome_ids)
                self._pending_subscriptions = deque()
                for outcome_id in add_outcome_ids:
                    self._active_since.setdefault(outcome_id, now)
                self._pending_since = {}
                promoted_count = len(add_outcome_ids)
            else:
                pending_adds = sorted(add_outcome_ids - set(self._subscriptions))
                if pending_adds:
                    pending_set = set(self._pending_subscriptions)
                    pending_set.update(pending_adds)
                    self._pending_subscriptions = deque(sorted(pending_set))
                    for outcome_id in pending_adds:
                        self._pending_since.setdefault(outcome_id, now)
                promoted_count = self._fill_active_from_pending_locked()

            self._next_activation_at = self._next_activation_time_locked()
            if not self._subscriptions:
                self._cursor = 0
            else:
                self._cursor %= len(self._subscriptions)
            self._priority_subscriptions = deque(
                outcome_id
                for outcome_id in self._priority_subscriptions
                if outcome_id in self._subscriptions
            )
            self._forced_fetch_outcome_ids = {
                outcome_id
                for outcome_id in self._forced_fetch_outcome_ids
                if outcome_id in self._subscriptions
            }
            return (
                len(add_outcome_ids),
                len(remove_outcome_ids),
                keep_count,
                promoted_count,
                len(self._pending_subscriptions),
            )

    def release_pending_if_due(self, *, now: float) -> tuple[int, int, int]:
        with self._subscriptions_lock:
            if not self._pending_subscriptions:
                self._next_activation_at = 0.0
                return 0, len(self._subscriptions), 0
            if self._next_activation_at > 0.0 and now < self._next_activation_at:
                return 0, len(self._subscriptions), len(self._pending_subscriptions)

            promoted_count = self._activate_pending_locked(
                limit=self._manager.warmup_batch_size(self.exchange)
            )
            self._next_activation_at = self._next_activation_time_locked()
            return promoted_count, len(self._subscriptions), len(self._pending_subscriptions)

    def next_outcome_id(self) -> str | None:
        with self._subscriptions_lock:
            while self._priority_subscriptions:
                priority_outcome_id = self._priority_subscriptions.popleft()
                if priority_outcome_id in self._subscriptions:
                    return priority_outcome_id
            bootstrap_outcomes = [
                outcome_id
                for outcome_id in self._subscriptions
                if not self._manager.stream_has_seen_success((self.exchange, outcome_id))
            ]
            if bootstrap_outcomes:
                bootstrap_outcomes.sort(
                    key=lambda outcome_id: self._active_since.get(outcome_id, 0.0)
                )
                outcome_id = bootstrap_outcomes[self._cursor % len(bootstrap_outcomes)]
                self._cursor = (self._cursor + 1) % len(bootstrap_outcomes)
                return outcome_id
            if not self._subscriptions:
                return None
            outcome_id = self._subscriptions[self._cursor]
            self._cursor = (self._cursor + 1) % len(self._subscriptions)
            return outcome_id

    def enqueue_quote_update(self, quote_update: QueuedQuoteUpdate) -> int:
        dropped_count = 0
        with self._dispatch_queue_lock:
            if len(self._dispatch_queue) >= self._dispatch_queue_capacity:
                self._dispatch_queue.popleft()
                self._dropped_dispatch_count += 1
                dropped_count = 1
            self._dispatch_queue.append(quote_update)
        return dropped_count

    def next_quote_update(self) -> QueuedQuoteUpdate | None:
        with self._dispatch_queue_lock:
            if not self._dispatch_queue:
                return None
            return self._dispatch_queue.popleft()

    def resubscribe_outcome_ids(self, outcome_ids: list[str]) -> tuple[int, int]:
        with self._subscriptions_lock:
            now = time.time()
            subscribed_outcomes = set(self._subscriptions) | set(self._pending_subscriptions)
            requested = [outcome_id for outcome_id in outcome_ids if outcome_id in subscribed_outcomes]
            if not requested:
                return 0, len(self._pending_subscriptions)

            requested_set = set(requested)
            self._subscriptions = [
                outcome_id for outcome_id in self._subscriptions if outcome_id not in requested_set
            ]
            for outcome_id in requested_set:
                self._active_since.pop(outcome_id, None)
            kept_pending = [
                outcome_id
                for outcome_id in self._pending_subscriptions
                if outcome_id not in requested_set
            ]
            self._pending_subscriptions = deque(kept_pending)
            for outcome_id in requested_set:
                self._pending_since.pop(outcome_id, None)
            self._priority_subscriptions = deque(
                outcome_id
                for outcome_id in self._priority_subscriptions
                if outcome_id not in requested_set
            )
            for outcome_id in requested_set:
                self._forced_fetch_outcome_ids.discard(outcome_id)

            if not self._manager.warmup_enabled(self.exchange):
                self._subscriptions = sorted(set(self._subscriptions) | requested_set)
                for outcome_id in requested_set:
                    self._active_since[outcome_id] = now
            else:
                readded_pending = list(self._pending_subscriptions)
                readded_pending.extend(sorted(requested_set))
                self._pending_subscriptions = deque(readded_pending)
                for outcome_id in requested_set:
                    self._pending_since[outcome_id] = now
                self._fill_active_from_pending_locked()

            self._next_activation_at = self._next_activation_time_locked()
            if not self._subscriptions:
                self._cursor = 0
            else:
                self._cursor %= len(self._subscriptions)

        with self._dispatch_queue_lock:
            self._dispatch_queue = deque(
                quote_update
                for quote_update in self._dispatch_queue
                if quote_update.quote.outcome_id not in requested_set
            )
        return len(requested), len(self._pending_subscriptions)

    def disable_outcome_ids(self, outcome_ids: list[str]) -> tuple[int, int]:
        with self._subscriptions_lock:
            subscribed_outcomes = set(self._subscriptions) | set(self._pending_subscriptions)
            disabled_set = {
                outcome_id for outcome_id in outcome_ids if outcome_id in subscribed_outcomes
            }
            if not disabled_set:
                return 0, len(self._pending_subscriptions)

            self._subscriptions = [
                outcome_id for outcome_id in self._subscriptions if outcome_id not in disabled_set
            ]
            self._pending_subscriptions = deque(
                outcome_id
                for outcome_id in self._pending_subscriptions
                if outcome_id not in disabled_set
            )
            for outcome_id in disabled_set:
                self._active_since.pop(outcome_id, None)
                self._pending_since.pop(outcome_id, None)
                self._forced_fetch_outcome_ids.discard(outcome_id)
            self._priority_subscriptions = deque(
                outcome_id
                for outcome_id in self._priority_subscriptions
                if outcome_id not in disabled_set
            )
            if not self._subscriptions:
                self._cursor = 0
            else:
                self._cursor %= len(self._subscriptions)

        with self._dispatch_queue_lock:
            self._dispatch_queue = deque(
                quote_update
                for quote_update in self._dispatch_queue
                if quote_update.quote.outcome_id not in disabled_set
            )
        return len(disabled_set), len(self._pending_subscriptions)

    def _run(self, *, worker_index: int = 0) -> None:
        self._manager.worker_loop(self, worker_index=worker_index)

    def _run_direct_quiet_refresh(self) -> None:
        self._manager.direct_quiet_refresh_loop(self)

    def _run_dispatcher(self) -> None:
        self._manager.dispatch_loop(self)

    def _fill_active_from_pending_locked(self) -> int:
        if not self._manager.warmup_enabled(self.exchange):
            return 0
        available_slots = max(
            0,
            self._manager.warmup_batch_size(self.exchange) - len(self._subscriptions),
        )
        return self._activate_pending_locked(limit=available_slots)

    def _activate_pending_locked(self, *, limit: int) -> int:
        if limit <= 0 or not self._pending_subscriptions:
            return 0

        promoted: list[str] = []
        while self._pending_subscriptions and len(promoted) < limit:
            outcome_id = self._pending_subscriptions.popleft()
            self._pending_since.pop(outcome_id, None)
            promoted.append(outcome_id)
        if promoted:
            self._subscriptions = sorted(set(self._subscriptions) | set(promoted))
            now = time.time()
            for outcome_id in promoted:
                self._active_since[outcome_id] = now
        return len(promoted)

    def _next_activation_time_locked(self) -> float:
        if not self._pending_subscriptions:
            return 0.0
        return time.monotonic() + self._manager.warmup_interval_seconds(self.exchange)


class UnsupportedSubscriptionManager:
    def __init__(self, *, engine: str) -> None:
        self._engine = engine

    def prepare_runtime(self) -> None:
        raise NotImplementedError(
            f"engine={self._engine} is not implemented yet; use --engine legacy for now"
        )

    def replace_streams(self, stream_keys: set[StreamKey]) -> bool:
        return bool(stream_keys)

    def health_state(self, *, now: float) -> HealthState:
        return HealthState(
            active_streams=[],
            quote_count=0,
            stale_quote_count=0,
            client_exchanges=[],
            exchange_subscription_counts={},
            exchange_active_counts={},
            exchange_pending_counts={},
            exchange_priority_counts={},
            exchange_queue_depth_counts={},
            exchange_dispatch_drop_counts={},
            exchange_source_modes={},
            exchange_modes={},
            exchange_recovery_reasons={},
            retry_windows={},
            backoffs={},
            rate_limit_counts={},
            sidecar_error_counts={},
            auth_error_counts={},
            success_counts={},
            error_counts={},
            latencies_ms={},
            last_ok_at={},
        )

    def stop(self, *, reason: str, timeout_seconds: float) -> None:
        return None


class LegacySubscriptionManager:
    def __init__(
        self,
        *,
        args: argparse.Namespace,
        quote_store: QuoteStore,
        stop_event: threading.Event,
        print_line: PrintFn,
        debug: DebugFn,
        exchange_builder: ExchangeBuilder,
        on_quote_update: Callable[[StreamKey], None],
    ) -> None:
        self.args = args
        self._quote_store = quote_store
        self.stop_event = stop_event
        self._print_line = print_line
        self._debug = debug
        self._exchange_builder = exchange_builder
        self._on_quote_update = on_quote_update
        self._kalshi_book_mode = str(getattr(args, "kalshi_book_mode", "auto")).lower()
        self._kalshi_polling_enabled = self._kalshi_book_mode == "poll"

        self._worker_stop_event = threading.Event()
        self._watch_locks: dict[str, threading.Lock] = {
            "kalshi": threading.Lock(),
            "polymarket": threading.Lock(),
        }
        self._state_lock = threading.Lock()
        self._client_lock = threading.Lock()
        self._sidecar_recover_lock = threading.Lock()

        self._next_sidecar_recover_at = 0.0
        self._exchange_retry_not_before: dict[str, float] = {}
        self._exchange_backoff_seconds: dict[str, float] = {}
        self._exchange_source_mode: dict[str, str] = {}
        self._exchange_source_mode_hold_until: dict[str, float] = {}
        self._exchange_source_preference: dict[str, str] = {}
        self._exchange_mode: dict[str, str] = {}
        self._exchange_mode_hold_until: dict[str, float] = {}
        self._exchange_recovery_reason: dict[str, str] = {}
        self._exchange_consecutive_failure_count: dict[str, int] = {}
        self._exchange_success_streak_count: dict[str, int] = {}
        self._worker_exchange_retry_not_before: dict[tuple[str, int], float] = {}
        self._worker_exchange_backoff_seconds: dict[tuple[str, int], float] = {}
        self._worker_exchange_rate_limit_count: dict[tuple[str, int], int] = {}
        self._worker_exchange_success_streak_count: dict[tuple[str, int], int] = {}
        self._next_rate_limit_log_at: dict[StreamKey, float] = {}
        self._next_rate_limit_storm_log_at: dict[str, float] = {}
        self._exchange_clients: dict[str, Any] = {}
        self._polymarket_ws_client: PolymarketMarketWebSocketClient | None = None
        self._stream_watch_success_count: dict[StreamKey, int] = {}
        self._stream_watch_error_count: dict[StreamKey, int] = {}
        self._stream_last_watch_latency_ms: dict[StreamKey, float] = {}
        self._stream_last_watch_ok_at: dict[StreamKey, float] = {}
        self._exchange_rate_limit_count: dict[str, int] = {}
        self._exchange_sidecar_error_count: dict[str, int] = {}
        self._exchange_auth_error_count: dict[str, int] = {}
        self._next_sidecar_error_log_at: dict[str, float] = {}
        self._next_auth_error_log_at: dict[str, float] = {}
        self._ingestors: dict[StreamKey, LegacyExchangeIngestor] = {}

        self._kalshi_direct_kwargs = build_kalshi_client_kwargs()
        self._use_kalshi_sidecar_default = True
        self._backoff_rng = random.Random(0)
        self._exchange_worker_clients: dict[tuple[str, int], Any] = {}

    def prepare_runtime(self) -> None:
        if bool(getattr(self.args, "kalshi_direct_mode", False)):
            self._use_kalshi_sidecar_default = False
            self._print_line("[info] kalshi_client_mode=direct_credentials reason=flag_forced")
            return

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

    def replace_streams(self, stream_keys: set[StreamKey]) -> bool:
        if stream_keys == set(self._ingestors.keys()):
            return False

        self._worker_stop_event.set()
        for ingestor in self._ingestors.values():
            ingestor.stop()
        self._close_all_exchange_clients()
        self._drain_ingestors(timeout_seconds=3.0, reason="restart")

        self._ingestors = {}
        self._worker_stop_event = threading.Event()

        for exchange, outcome_id in sorted(stream_keys):
            ingestor = LegacyExchangeIngestor(
                manager=self,
                exchange=exchange,
                outcome_id=outcome_id,
                worker_stop_event=self._worker_stop_event,
            )
            self._ingestors[(exchange, outcome_id)] = ingestor
            ingestor.start()
            self._debug(f"worker started exchange={exchange} outcome_id={outcome_id}")

        return True

    def health_state(self, *, now: float) -> HealthState:
        with self._state_lock:
            retry_windows = {
                exchange: max(0.0, ts - now)
                for exchange, ts in self._exchange_retry_not_before.items()
            }
            exchange_modes = dict(self._exchange_mode)
            exchange_source_modes = dict(self._exchange_source_mode)
            exchange_recovery_reasons = dict(self._exchange_recovery_reason)
            backoffs = dict(self._exchange_backoff_seconds)
            rate_limit_counts = dict(self._exchange_rate_limit_count)
            sidecar_error_counts = dict(self._exchange_sidecar_error_count)
            auth_error_counts = dict(self._exchange_auth_error_count)
            success_counts = dict(self._stream_watch_success_count)
            error_counts = dict(self._stream_watch_error_count)
            latencies = dict(self._stream_last_watch_latency_ms)
            last_ok = dict(self._stream_last_watch_ok_at)
            worker_retry_not_before = dict(self._worker_exchange_retry_not_before)
            worker_backoffs = dict(self._worker_exchange_backoff_seconds)
            worker_rate_limits = dict(self._worker_exchange_rate_limit_count)

        for (exchange, _worker_index), retry_not_before in worker_retry_not_before.items():
            retry_windows.setdefault(exchange, 0.0)
            if retry_not_before > now:
                retry_windows[exchange] = max(retry_windows[exchange], retry_not_before - now)
        for (exchange, _worker_index), backoff_seconds in worker_backoffs.items():
            if exchange not in backoffs:
                backoffs[exchange] = 0.0
            if worker_rate_limits.get((exchange, _worker_index), 0) > 0:
                backoffs[exchange] = max(backoffs[exchange], backoff_seconds)
        worker_local_exchanges = {
            exchange
            for exchange, _worker_index in worker_rate_limits
        }
        for exchange in worker_local_exchanges:
            rate_limit_counts[exchange] = 0
        for (exchange, _worker_index), count in worker_rate_limits.items():
            if count > 0:
                rate_limit_counts[exchange] = rate_limit_counts.get(exchange, 0) + 1

        with self._client_lock:
            client_exchanges = sorted(
                set(self._exchange_clients.keys())
                | {exchange for exchange, _ in self._exchange_worker_clients.keys()}
            )

        return HealthState(
            active_streams=sorted(self._ingestors.keys()),
            quote_count=self._quote_store.quote_count(),
            stale_quote_count=self._quote_store.stale_quote_count(
                now=now,
                stale_after_seconds=float(getattr(self.args, "book_stale_seconds", 15.0)),
            ),
            client_exchanges=client_exchanges,
            exchange_subscription_counts={
                exchange: sum(1 for stream_exchange, _ in self._ingestors if stream_exchange == exchange)
                for exchange in {stream_exchange for stream_exchange, _ in self._ingestors}
            },
            exchange_active_counts={
                exchange: sum(1 for stream_exchange, _ in self._ingestors if stream_exchange == exchange)
                for exchange in {stream_exchange for stream_exchange, _ in self._ingestors}
            },
            exchange_pending_counts={},
            exchange_priority_counts={},
            exchange_queue_depth_counts={},
            exchange_dispatch_drop_counts={},
            exchange_source_modes=exchange_source_modes,
            exchange_modes=exchange_modes,
            exchange_recovery_reasons=exchange_recovery_reasons,
            retry_windows=retry_windows,
            backoffs=backoffs,
            rate_limit_counts=rate_limit_counts,
            sidecar_error_counts=sidecar_error_counts,
            auth_error_counts=auth_error_counts,
            success_counts=success_counts,
            error_counts=error_counts,
            latencies_ms=latencies,
            last_ok_at=last_ok,
        )

    def stop(self, *, reason: str, timeout_seconds: float) -> None:
        self._worker_stop_event.set()
        for ingestor in self._ingestors.values():
            ingestor.stop()
        self._close_all_exchange_clients()
        self._drain_ingestors(timeout_seconds=timeout_seconds, reason=reason)

    def worker_loop(self, ingestor: LegacyExchangeIngestor) -> None:
        exchange = ingestor.exchange
        outcome_id = ingestor.outcome_id
        stream_key = ingestor.stream_key
        self._debug(f"worker loop entering exchange={exchange} outcome_id={outcome_id}")

        while not ingestor.should_stop():
            self._wait_for_exchange_retry(exchange=exchange, ingestor=ingestor)
            if ingestor.should_stop():
                return

            client = self._get_exchange_client(exchange)
            while not ingestor.should_stop():
                self._wait_for_exchange_retry(exchange=exchange, ingestor=ingestor)
                if ingestor.should_stop():
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
                        self._register_rate_limit(
                            exchange=exchange,
                            outcome_id=outcome_id,
                            exc=exc,
                        )
                        continue
                    if self._is_auth_token_error(exc):
                        self._register_auth_error(
                            exchange=exchange,
                            outcome_id=outcome_id,
                            exc=exc,
                        )
                        if exchange == "kalshi" and self._prepare_kalshi_sidecar_credentials():
                            self._switch_kalshi_to_direct_mode(reason="ws_auth_failed")
                            self._close_exchange_client(exchange)
                            continue
                        self._close_exchange_client(exchange)
                        self._maybe_restart_pmxt_sidecar(
                            reason="auth_token_invalid",
                            exchange=exchange,
                        )
                        continue
                    if self._is_sidecar_unavailable_error(exc):
                        self._register_sidecar_unavailable(
                            exchange=exchange,
                            outcome_id=outcome_id,
                            exc=exc,
                        )
                        self._close_exchange_client(exchange)
                        self._maybe_restart_pmxt_sidecar(
                            reason="connection_refused",
                            exchange=exchange,
                        )
                        continue
                    self._print_line(
                        f"[error] stream failure exchange={exchange} "
                        f"outcome_id={outcome_id} error={exc}"
                    )
                    self._register_exchange_failure(
                        exchange=exchange,
                        reason="stream_failure",
                        outcome_id=outcome_id,
                    )
                    self._debug(
                        f"non-rate-limit failure exchange={exchange} outcome_id={outcome_id} "
                        f"error_type={type(exc).__name__}"
                    )
                    self._close_exchange_client(exchange)
                    break

                best_bid, best_ask = extract_best_prices(book)
                seen_at = time.time()
                quote = QuoteSnapshot(
                    exchange=exchange,
                    outcome_id=outcome_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    book_timestamp_ms=extract_book_timestamp_ms(book),
                    updated_at=seen_at,
                    quote_seen_at=seen_at,
                    source_label=self._legacy_quote_source(exchange),
                )
                with self._state_lock:
                    self._stream_watch_success_count[stream_key] = (
                        self._stream_watch_success_count.get(stream_key, 0) + 1
                    )
                    self._stream_last_watch_latency_ms[stream_key] = watch_latency_ms
                    self._stream_last_watch_ok_at[stream_key] = seen_at
                self._quote_store.upsert_quote(quote)
                self._mark_stream_success(exchange)
                if ingestor.should_stop():
                    return
                self._on_quote_update(stream_key)

                time.sleep(self._loop_delay_seconds(exchange))

            if ingestor.should_stop():
                return
            time.sleep(1.0)
        self._debug(f"worker loop exiting exchange={exchange} outcome_id={outcome_id}")

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

    def _switch_kalshi_to_direct_mode(self, *, reason: str) -> None:
        with self._client_lock:
            if not self._use_kalshi_sidecar_default:
                return
            self._use_kalshi_sidecar_default = False
        self._print_line(f"[warn] kalshi_client_mode=direct_credentials reason={reason}")

    def _wait_for_exchange_retry(
        self,
        *,
        exchange: str,
        ingestor: LegacyExchangeIngestor | MultiplexExchangeIngestor,
        worker_index: int | None = None,
    ) -> None:
        while not ingestor.should_stop():
            with self._state_lock:
                if self._uses_worker_local_rate_limit_state(
                    exchange=exchange,
                    worker_index=worker_index,
                ):
                    assert worker_index is not None
                    retry_not_before = self._worker_exchange_retry_not_before.get(
                        (exchange, worker_index),
                        0.0,
                    )
                else:
                    retry_not_before = self._exchange_retry_not_before.get(exchange, 0.0)
            now = time.time()
            if retry_not_before <= now:
                return
            time.sleep(min(1.0, retry_not_before - now))

    def _watch_order_book(
        self,
        *,
        client: Any,
        exchange: str,
        outcome_id: str,
        worker_index: int | None = None,
    ) -> Any:
        if self._should_use_worker_scoped_client(exchange=exchange, worker_index=worker_index):
            return client.watch_order_book(outcome_id, limit=self.args.depth)
        if exchange == "kalshi" and self._kalshi_polling_enabled:
            watch_lock = self._watch_locks.get(exchange)
            if watch_lock is None:
                return client.fetch_order_book(outcome_id)
            with watch_lock:
                return client.fetch_order_book(outcome_id)

        watch_lock = self._watch_locks.get(exchange)
        if watch_lock is None:
            return client.watch_order_book(outcome_id, limit=self.args.depth)
        with watch_lock:
            return client.watch_order_book(outcome_id, limit=self.args.depth)

    def _legacy_quote_source(self, exchange: str) -> str:
        if exchange == "kalshi" and self._kalshi_polling_enabled:
            return "poll"
        return "watch"

    def _loop_delay_seconds(self, exchange: str) -> float:
        if exchange == "kalshi":
            return 0.25
        return 0.05

    def _get_exchange_client(self, exchange: str) -> Any:
        with self._client_lock:
            client = self._exchange_clients.get(exchange)
            if client is None:
                client = self._exchange_builder(
                    exchange,
                    use_kalshi_sidecar_default=self._use_kalshi_sidecar_default,
                    kalshi_direct_kwargs=self._kalshi_direct_kwargs,
                )
                self._exchange_clients[exchange] = client
                self._debug(
                    f"exchange client created exchange={exchange} client_id={id(client)}"
                )
            return client

    def _should_use_worker_scoped_client(
        self,
        *,
        exchange: str,
        worker_index: int | None,
    ) -> bool:
        return (
            worker_index is not None
            and worker_index >= 0
            and exchange == "kalshi"
            and self._is_multiplex_engine()
            and self._current_exchange_source_mode(exchange) == "watch"
        )

    def _get_exchange_worker_client(self, exchange: str, worker_index: int) -> Any:
        with self._client_lock:
            key = (exchange, worker_index)
            client = self._exchange_worker_clients.get(key)
            if client is None:
                client = self._exchange_builder(
                    exchange,
                    use_kalshi_sidecar_default=self._use_kalshi_sidecar_default,
                    kalshi_direct_kwargs=self._kalshi_direct_kwargs,
                )
                self._exchange_worker_clients[key] = client
                self._debug(
                    "exchange client created "
                    f"exchange={exchange} worker_index={worker_index} client_id={id(client)}"
                )
            return client

    def _get_client_for_worker(
        self,
        *,
        exchange: str,
        worker_index: int | None,
    ) -> Any:
        if self._should_use_worker_scoped_client(exchange=exchange, worker_index=worker_index):
            assert worker_index is not None
            return self._get_exchange_worker_client(exchange, worker_index)
        return self._get_exchange_client(exchange)

    def _close_exchange_client(self, exchange: str) -> None:
        if exchange == "polymarket":
            with self._client_lock:
                polymarket_ws_client = self._polymarket_ws_client
                self._polymarket_ws_client = None
            if polymarket_ws_client is not None:
                try:
                    polymarket_ws_client.close()
                except Exception:
                    pass
                self._debug(
                    "exchange client closed exchange=polymarket client_id="
                    f"{id(polymarket_ws_client)}"
                )
        with self._client_lock:
            worker_clients = [
                ((client_exchange, worker_index), client)
                for (client_exchange, worker_index), client in self._exchange_worker_clients.items()
                if client_exchange == exchange
            ]
            for key, _ in worker_clients:
                self._exchange_worker_clients.pop(key, None)
        for (client_exchange, worker_index), client in worker_clients:
            try:
                client.close()
            except Exception:
                pass
            self._debug(
                "exchange client closed "
                f"exchange={client_exchange} worker_index={worker_index} client_id={id(client)}"
            )
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
            exchanges.extend(
                exchange
                for exchange, _ in self._exchange_worker_clients.keys()
                if exchange not in exchanges
            )
            if self._polymarket_ws_client is not None and "polymarket" not in exchanges:
                exchanges.append("polymarket")
        for exchange in exchanges:
            self._close_exchange_client(exchange)

    def _polymarket_market_ws_url(self) -> str:
        return str(
            getattr(
                self.args,
                "multiplex_polymarket_market_ws_url",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            )
        )

    def _get_polymarket_ws_client(self) -> PolymarketMarketWebSocketClient:
        with self._client_lock:
            client = self._polymarket_ws_client
            if client is None:
                client = PolymarketMarketWebSocketClient(
                    url=self._polymarket_market_ws_url(),
                    ping_interval_seconds=float(
                        getattr(
                            self.args,
                            "multiplex_polymarket_ping_interval_seconds",
                            5.0,
                        )
                    ),
                    receive_timeout_seconds=float(
                        getattr(
                            self.args,
                            "multiplex_polymarket_receive_timeout_seconds",
                            1.0,
                        )
                    ),
                )
                self._polymarket_ws_client = client
                self._debug(
                    f"exchange client created exchange=polymarket client_id={id(client)}"
                )
            return client

    def _drain_ingestors(self, *, timeout_seconds: float, reason: str) -> None:
        pending = dict(self._ingestors)
        if not pending:
            return

        deadline = time.time() + max(0.1, timeout_seconds)
        while pending and time.time() < deadline:
            for stream_key, ingestor in list(pending.items()):
                ingestor.join(timeout_seconds=0.05)
                if not ingestor.is_alive():
                    pending.pop(stream_key, None)

        if pending:
            sample = ", ".join(
                f"{exchange}:{outcome_id}" for exchange, outcome_id in list(pending.keys())[:5]
            )
            self._debug(
                f"worker drain timeout reason={reason} lingering={len(pending)} sample=[{sample}]"
            )

    def _is_rate_limited_error(self, exc: Exception) -> bool:
        return "429" in str(exc)

    def _uses_worker_local_rate_limit_state(
        self,
        *,
        exchange: str,
        worker_index: int | None,
    ) -> bool:
        return (
            worker_index is not None
            and worker_index >= 0
            and exchange == "kalshi"
            and self._is_multiplex_engine()
        )

    def _active_worker_rate_limit_count_locked(self, exchange: str) -> int:
        return sum(
            1
            for (worker_exchange, _worker_index), count in self._worker_exchange_rate_limit_count.items()
            if worker_exchange == exchange and count > 0
        )

    def _is_sidecar_unavailable_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            ("failed to establish a new connection" in message)
            or ("connection refused" in message)
            or ("[errno 111]" in message)
            or ("max retries exceeded with url" in message and "httpconnectionpool" in message)
        )

    def _is_auth_token_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return "unauthorized" in message and "access token" in message

    def _is_missing_order_book_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return "failed to fetch order book" in message and "order not found" in message

    def _register_rate_limit(
        self,
        *,
        exchange: str,
        outcome_id: str,
        exc: Exception,
        worker_index: int | None = None,
    ) -> None:
        if self._uses_worker_local_rate_limit_state(exchange=exchange, worker_index=worker_index):
            assert worker_index is not None
            now = time.time()
            worker_key = (exchange, worker_index)
            with self._state_lock:
                next_count = self._worker_exchange_rate_limit_count.get(worker_key, 0) + 1
                self._worker_exchange_rate_limit_count[worker_key] = next_count
                self._worker_exchange_success_streak_count[worker_key] = 0
                is_storm = next_count >= self._rate_limit_storm_threshold()
                previous_backoff = self._worker_exchange_backoff_seconds.get(worker_key, 1.0)
                next_backoff = min(max(previous_backoff * 2.0, 2.0), 60.0)
                self._worker_exchange_backoff_seconds[worker_key] = next_backoff
                retry_delay = self._jitter_backoff_seconds(next_backoff)
                if is_storm:
                    retry_delay = max(retry_delay, self._rate_limit_storm_min_delay_seconds())
                retry_not_before = max(
                    self._worker_exchange_retry_not_before.get(worker_key, 0.0),
                    now + retry_delay,
                )
                self._worker_exchange_retry_not_before[worker_key] = retry_not_before
                self._exchange_rate_limit_count[exchange] = self._active_worker_rate_limit_count_locked(
                    exchange
                )
                all_workers_blocked = (
                    self._exchange_rate_limit_count[exchange] >= self.producer_thread_count(exchange)
                )
                recovery_reason = (
                    "rate_limit_storm_backoff" if is_storm else "rate_limit_backoff"
                )
                self._exchange_recovery_reason[exchange] = recovery_reason

                log_key = (exchange, outcome_id)
                should_log = now >= self._next_rate_limit_log_at.get(log_key, 0.0)
                if should_log:
                    self._next_rate_limit_log_at[log_key] = now + 10.0
                should_log_storm = is_storm and now >= self._next_rate_limit_storm_log_at.get(
                    exchange,
                    0.0,
                )
                if should_log_storm:
                    self._next_rate_limit_storm_log_at[exchange] = now + 10.0

            if all_workers_blocked:
                self._register_exchange_failure(
                    exchange=exchange,
                    reason="rate_limit_storm" if is_storm else "rate_limit",
                    outcome_id=outcome_id,
                    recovery_reason=recovery_reason,
                )
            else:
                self._set_exchange_mode(
                    exchange=exchange,
                    mode="recovering",
                    reason="rate_limit_storm" if is_storm else "rate_limit",
                    outcome_id=outcome_id,
                )
                self._emit_recovery_policy(
                    exchange=exchange,
                    policy=recovery_reason,
                    mode="recovering",
                    outcome_id=outcome_id,
                )

            if should_log:
                delay = max(0.0, retry_not_before - now)
                self._print_line(
                    f"[warn] rate limit exchange={exchange} outcome_id={outcome_id} "
                    f"worker_index={worker_index} retry_in={delay:.1f}s "
                    f"backoff={next_backoff:.1f}s error={exc}"
                )
                self._debug(
                    f"rate-limit-detail exchange={exchange} outcome_id={outcome_id} "
                    f"worker_index={worker_index} error_type={type(exc).__name__} "
                    f"retry_not_before={retry_not_before:.3f}"
                )
            if should_log_storm:
                delay = max(0.0, retry_not_before - now)
                self._print_line(
                    f"[warn] exchange_rate_limit_storm exchange={exchange} "
                    f"outcome_id={outcome_id} worker_index={worker_index} "
                    f"count={next_count} retry_in={delay:.1f}s"
                )
            self._maybe_enable_kalshi_polling_after_rate_limit(exchange=exchange)
            return

        now = time.time()
        with self._state_lock:
            next_count = self._exchange_rate_limit_count.get(exchange, 0) + 1
            self._exchange_rate_limit_count[exchange] = next_count
            self._exchange_success_streak_count[exchange] = 0
            is_storm = next_count >= self._rate_limit_storm_threshold()
            previous_backoff = self._exchange_backoff_seconds.get(exchange, 1.0)
            next_backoff = min(max(previous_backoff * 2.0, 2.0), 60.0)
            self._exchange_backoff_seconds[exchange] = next_backoff
            retry_delay = self._jitter_backoff_seconds(next_backoff)
            if is_storm:
                retry_delay = max(retry_delay, self._rate_limit_storm_min_delay_seconds())
            retry_not_before = max(
                self._exchange_retry_not_before.get(exchange, 0.0),
                now + retry_delay,
            )
            self._exchange_retry_not_before[exchange] = retry_not_before

            log_key = (exchange, outcome_id)
            should_log = now >= self._next_rate_limit_log_at.get(log_key, 0.0)
            if should_log:
                self._next_rate_limit_log_at[log_key] = now + 10.0
            should_log_storm = is_storm and now >= self._next_rate_limit_storm_log_at.get(
                exchange,
                0.0,
            )
            if should_log_storm:
                self._next_rate_limit_storm_log_at[exchange] = now + 10.0

        self._register_exchange_failure(
            exchange=exchange,
            reason="rate_limit_storm" if is_storm else "rate_limit",
            outcome_id=outcome_id,
            recovery_reason=(
                "rate_limit_storm_backoff" if is_storm else "rate_limit_backoff"
            ),
        )

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
        if should_log_storm:
            delay = max(0.0, retry_not_before - now)
            self._print_line(
                f"[warn] exchange_rate_limit_storm exchange={exchange} "
                f"outcome_id={outcome_id} count={next_count} retry_in={delay:.1f}s"
            )
        self._maybe_enable_kalshi_polling_after_rate_limit(exchange=exchange)

    def _maybe_enable_kalshi_polling_after_rate_limit(self, *, exchange: str) -> None:
        if exchange != "kalshi" or self._kalshi_book_mode != "auto":
            return
        with self._state_lock:
            count = self._exchange_rate_limit_count.get("kalshi", 0)
            fallback_threshold = 5
            if self._is_multiplex_engine():
                fallback_threshold = max(
                    1,
                    int(
                        getattr(
                            self.args,
                            "multiplex_kalshi_watch_fallback_rate_limit_threshold",
                            5,
                        )
                    ),
                )
            should_switch = count >= fallback_threshold
            if should_switch and not self._is_multiplex_engine():
                should_switch = not self._kalshi_polling_enabled
            if should_switch and not self._is_multiplex_engine():
                self._kalshi_polling_enabled = True
        if should_switch:
            if self._is_multiplex_engine():
                self._set_exchange_source_mode(
                    exchange="kalshi",
                    mode="poll",
                    reason="rate_limit_storm_auto_fallback",
                )
                self._print_line(
                    "[warn] kalshi_multiplex_source_mode=poll "
                    "reason=rate_limit_storm_auto_fallback"
                )
            else:
                self._print_line(
                    "[warn] kalshi_orderbook_mode=poll reason=rate_limit_storm_auto_fallback"
                )
            self._close_exchange_client("kalshi")

    def _register_sidecar_unavailable(
        self,
        *,
        exchange: str,
        outcome_id: str,
        exc: Exception,
    ) -> None:
        now = time.time()
        self._register_exchange_failure(
            exchange=exchange,
            reason="sidecar_unavailable",
            outcome_id=outcome_id,
            recovery_reason="sidecar_restart",
        )
        with self._state_lock:
            previous_backoff = self._exchange_backoff_seconds.get(exchange, 1.0)
            next_backoff = min(max(previous_backoff * 1.8, 2.0), 15.0)
            self._exchange_backoff_seconds[exchange] = next_backoff
            retry_delay = self._jitter_backoff_seconds(next_backoff)
            retry_not_before = max(
                self._exchange_retry_not_before.get(exchange, 0.0),
                now + retry_delay,
            )
            self._exchange_retry_not_before[exchange] = retry_not_before
            self._exchange_sidecar_error_count[exchange] = (
                self._exchange_sidecar_error_count.get(exchange, 0) + 1
            )

            should_log = now >= self._next_sidecar_error_log_at.get(exchange, 0.0)
            if should_log:
                self._next_sidecar_error_log_at[exchange] = now + 5.0

        if should_log:
            delay = max(0.0, retry_not_before - now)
            self._print_line(
                f"[warn] sidecar unavailable exchange={exchange} outcome_id={outcome_id} "
                f"retry_in={delay:.1f}s backoff={next_backoff:.1f}s error={exc}"
            )
            self._debug(
                f"sidecar-detail exchange={exchange} outcome_id={outcome_id} "
                f"error_type={type(exc).__name__} retry_not_before={retry_not_before:.3f}"
            )

    def _register_auth_error(
        self,
        *,
        exchange: str,
        outcome_id: str,
        exc: Exception,
    ) -> None:
        now = time.time()
        self._register_exchange_failure(
            exchange=exchange,
            reason="auth_error",
            outcome_id=outcome_id,
            recovery_reason="auth_refresh",
        )
        with self._state_lock:
            previous_backoff = self._exchange_backoff_seconds.get(exchange, 1.0)
            next_backoff = min(max(previous_backoff * 1.6, 2.0), 30.0)
            self._exchange_backoff_seconds[exchange] = next_backoff
            retry_delay = self._jitter_backoff_seconds(next_backoff)
            retry_not_before = max(
                self._exchange_retry_not_before.get(exchange, 0.0),
                now + retry_delay,
            )
            self._exchange_retry_not_before[exchange] = retry_not_before
            self._exchange_auth_error_count[exchange] = (
                self._exchange_auth_error_count.get(exchange, 0) + 1
            )

            should_log = now >= self._next_auth_error_log_at.get(exchange, 0.0)
            if should_log:
                self._next_auth_error_log_at[exchange] = now + 5.0

        if should_log:
            delay = max(0.0, retry_not_before - now)
            self._print_line(
                f"[warn] auth token invalid exchange={exchange} outcome_id={outcome_id} "
                f"retry_in={delay:.1f}s backoff={next_backoff:.1f}s error={exc}"
            )
            self._debug(
                f"auth-detail exchange={exchange} outcome_id={outcome_id} "
                f"error_type={type(exc).__name__} retry_not_before={retry_not_before:.3f}"
            )

    def _maybe_restart_pmxt_sidecar(self, *, reason: str, exchange: str) -> None:
        now = time.time()
        with self._sidecar_recover_lock:
            if now < self._next_sidecar_recover_at:
                return
            self._next_sidecar_recover_at = now + 15.0

        try:
            pmxt.restart_server()
            self._close_all_exchange_clients()
            self._print_line(
                f"[info] pmxt sidecar restarted reason={reason} exchange={exchange}"
            )
        except Exception as exc:
            self._print_line(
                f"[warn] pmxt sidecar restart failed reason={reason} exchange={exchange} error={exc}"
            )

    def _mark_stream_success(
        self,
        exchange: str,
        *,
        worker_index: int | None = None,
    ) -> None:
        now = time.time()
        healthy_source_mode = self._healthy_exchange_source_mode(exchange)
        if self._uses_worker_local_rate_limit_state(exchange=exchange, worker_index=worker_index):
            assert worker_index is not None
            worker_key = (exchange, worker_index)
            with self._state_lock:
                next_success_streak = self._worker_exchange_success_streak_count.get(worker_key, 0) + 1
                self._worker_exchange_success_streak_count[worker_key] = next_success_streak
                current_source_mode = self._exchange_source_mode.get(exchange)
                worker_retry_not_before = self._worker_exchange_retry_not_before.get(
                    worker_key,
                    0.0,
                )
                worker_rate_limit_count = self._worker_exchange_rate_limit_count.get(worker_key, 0)
                exchange_recovery_reason = self._exchange_recovery_reason.get(exchange)
                should_clear_worker_failure = True
                if (
                    current_source_mode == "degraded"
                    and (
                        worker_rate_limit_count > 0
                        or exchange_recovery_reason
                        in {"rate_limit_backoff", "rate_limit_storm_backoff"}
                    )
                ):
                    required_successes = max(
                        1,
                        int(
                            getattr(
                                self.args,
                                "multiplex_kalshi_successes_to_restore_watch",
                                3,
                            )
                        ),
                    )
                    should_clear_worker_failure = (
                        worker_retry_not_before <= now and next_success_streak >= required_successes
                    )
                if not should_clear_worker_failure:
                    return

                self._worker_exchange_retry_not_before[worker_key] = 0.0
                self._worker_exchange_backoff_seconds[worker_key] = 1.0
                self._worker_exchange_rate_limit_count[worker_key] = 0
                self._worker_exchange_success_streak_count[worker_key] = 0
                active_rate_limited_workers = self._active_worker_rate_limit_count_locked(exchange)
                self._exchange_rate_limit_count[exchange] = active_rate_limited_workers
                if active_rate_limited_workers <= 0:
                    self._exchange_retry_not_before[exchange] = 0.0
                    self._exchange_consecutive_failure_count[exchange] = 0
                    self._exchange_success_streak_count[exchange] = 0
                    self._exchange_recovery_reason.pop(exchange, None)

            self._set_exchange_source_mode(
                exchange=exchange,
                mode=healthy_source_mode,
                reason="stream_ok",
            )
            self._set_exchange_mode(
                exchange=exchange,
                mode="healthy" if active_rate_limited_workers <= 0 else "recovering",
                reason="stream_ok",
            )
            return

        should_clear_exchange_failure = True
        with self._state_lock:
            next_success_streak = self._exchange_success_streak_count.get(exchange, 0) + 1
            self._exchange_success_streak_count[exchange] = next_success_streak
            current_source_mode = self._exchange_source_mode.get(exchange)
            retry_not_before = self._exchange_retry_not_before.get(exchange, 0.0)
            rate_limit_count = self._exchange_rate_limit_count.get(exchange, 0)
            recovery_reason = self._exchange_recovery_reason.get(exchange)
            rate_limited_recovery = rate_limit_count > 0 or recovery_reason in {
                "rate_limit_backoff",
                "rate_limit_storm_backoff",
            }
            if (
                exchange == "kalshi"
                and healthy_source_mode == "watch"
                and current_source_mode == "degraded"
                and rate_limited_recovery
            ):
                required_successes = max(
                    1,
                    int(
                        getattr(
                            self.args,
                            "multiplex_kalshi_successes_to_restore_watch",
                            3,
                        )
                    ),
                )
                should_clear_exchange_failure = (
                    retry_not_before <= now and next_success_streak >= required_successes
                )
            if not should_clear_exchange_failure:
                return
            if self._exchange_backoff_seconds.get(exchange, 1.0) > 1.0:
                self._exchange_backoff_seconds[exchange] = 1.0
            self._exchange_retry_not_before[exchange] = 0.0
            self._exchange_consecutive_failure_count[exchange] = 0
            self._exchange_rate_limit_count[exchange] = 0
            self._exchange_success_streak_count[exchange] = 0
            self._exchange_recovery_reason.pop(exchange, None)
        self._set_exchange_source_mode(
            exchange=exchange,
            mode=healthy_source_mode,
            reason="stream_ok",
        )
        self._set_exchange_mode(
            exchange=exchange,
            mode="healthy",
            reason="stream_ok",
        )

    def stream_has_seen_success(self, stream_key: StreamKey) -> bool:
        with self._state_lock:
            return self._stream_watch_success_count.get(stream_key, 0) > 0

    def _failure_threshold(self) -> int:
        return max(
            1,
            int(getattr(self.args, "multiplex_degraded_failure_threshold", 3)),
        )

    def _register_exchange_failure(
        self,
        *,
        exchange: str,
        reason: str,
        outcome_id: str | None = None,
        recovery_reason: str | None = None,
    ) -> None:
        with self._state_lock:
            next_failures = self._exchange_consecutive_failure_count.get(exchange, 0) + 1
            self._exchange_consecutive_failure_count[exchange] = next_failures
            if recovery_reason:
                self._exchange_recovery_reason[exchange] = recovery_reason
        self._set_exchange_source_mode(
            exchange=exchange,
            mode="degraded",
            reason=reason,
            outcome_id=outcome_id,
        )
        mode = "degraded" if next_failures >= self._failure_threshold() else "recovering"
        self._set_exchange_mode(
            exchange=exchange,
            mode=mode,
            reason=reason,
            outcome_id=outcome_id,
        )
        if recovery_reason:
            self._emit_recovery_policy(
                exchange=exchange,
                policy=recovery_reason,
                mode=mode,
                outcome_id=outcome_id,
            )

    def _set_exchange_mode(
        self,
        *,
        exchange: str,
        mode: str,
        reason: str,
        outcome_id: str | None = None,
    ) -> None:
        now = time.monotonic()
        severity = {"healthy": 0, "recovering": 1, "degraded": 2}
        with self._state_lock:
            previous_mode = self._exchange_mode.get(exchange)
            hold_until = self._exchange_mode_hold_until.get(exchange, 0.0)
            if (
                previous_mode is not None
                and now < hold_until
                and severity.get(mode, 0) < severity.get(previous_mode, 0)
            ):
                return
            if previous_mode == mode:
                return
            self._exchange_mode[exchange] = mode
            if mode == "healthy":
                self._exchange_mode_hold_until[exchange] = 0.0
            else:
                self._exchange_mode_hold_until[exchange] = (
                    now + self._mode_hysteresis_seconds()
                )

        detail = f" reason={reason}"
        if outcome_id:
            detail += f" outcome_id={outcome_id}"
        self._print_line(f"[info] exchange_mode exchange={exchange} mode={mode}{detail}")

    def _set_exchange_source_mode(
        self,
        *,
        exchange: str,
        mode: str,
        reason: str,
        outcome_id: str | None = None,
    ) -> None:
        if not self._is_multiplex_engine():
            return
        now = time.monotonic()
        with self._state_lock:
            previous_mode = self._exchange_source_mode.get(exchange)
            hold_until = self._exchange_source_mode_hold_until.get(exchange, 0.0)
            if previous_mode == mode:
                return
            if mode == "watch" and now < hold_until:
                return
            self._exchange_source_mode[exchange] = mode
            if mode == "poll" and reason == "rate_limit_storm_auto_fallback":
                self._exchange_source_mode_hold_until[exchange] = (
                    now + self._source_mode_hysteresis_seconds()
                )
            elif mode in {"watch", "direct"}:
                self._exchange_source_mode_hold_until[exchange] = 0.0

        detail = f" reason={reason}"
        if outcome_id:
            detail += f" outcome_id={outcome_id}"
        self._print_line(
            f"[info] exchange_source_mode exchange={exchange} mode={mode}{detail}"
        )

    def _healthy_exchange_source_mode(self, exchange: str) -> str:
        with self._state_lock:
            preferred_mode = self._exchange_source_preference.get(exchange)
        if preferred_mode in {"watch", "poll", "direct"}:
            return preferred_mode
        if exchange == "polymarket":
            configured_mode = str(
                getattr(self.args, "multiplex_polymarket_source_mode", "poll")
            ).lower()
            if configured_mode == "poll":
                return "poll"
            return "direct"
        if exchange == "kalshi" and self._kalshi_book_mode != "poll":
            return "watch"
        return "poll"

    def _initial_multiplex_source_mode(self, exchange: str) -> str:
        if exchange == "polymarket":
            configured_mode = str(
                getattr(self.args, "multiplex_polymarket_source_mode", "poll")
            ).lower()
            if configured_mode == "poll":
                return "poll"
            return "direct"
        if exchange == "kalshi" and self._kalshi_book_mode != "poll":
            return "watch"
        return "poll"

    def _current_exchange_source_mode(self, exchange: str) -> str:
        with self._state_lock:
            current_mode = self._exchange_source_mode.get(exchange)
        if current_mode in {"watch", "poll", "direct", "degraded"}:
            return current_mode
        return self._healthy_exchange_source_mode(exchange)

    def _source_mode_hysteresis_seconds(self) -> float:
        if not self._is_multiplex_engine():
            return 0.0
        return max(
            0.0,
            float(getattr(self.args, "multiplex_source_mode_hysteresis_seconds", 5.0)),
        )

    def _emit_recovery_policy(
        self,
        *,
        exchange: str,
        policy: str,
        mode: str,
        outcome_id: str | None = None,
    ) -> None:
        detail = f" policy={policy} mode={mode}"
        if outcome_id:
            detail += f" outcome_id={outcome_id}"
        self._print_line(f"[info] exchange_recovery exchange={exchange}{detail}")

    def _is_multiplex_engine(self) -> bool:
        return str(getattr(self.args, "engine", "legacy")).lower() == "multiplex"

    def _backoff_jitter_ratio(self) -> float:
        if not self._is_multiplex_engine():
            return 0.0
        ratio = float(getattr(self.args, "multiplex_backoff_jitter_ratio", 0.2))
        return min(max(ratio, 0.0), 0.5)

    def _jitter_backoff_seconds(self, backoff_seconds: float) -> float:
        ratio = self._backoff_jitter_ratio()
        if ratio <= 0.0:
            return backoff_seconds
        low = max(0.0, 1.0 - ratio)
        high = 1.0 + ratio
        return backoff_seconds * self._backoff_rng.uniform(low, high)

    def _mode_hysteresis_seconds(self) -> float:
        if not self._is_multiplex_engine():
            return 0.0
        return max(
            0.0,
            float(getattr(self.args, "multiplex_mode_hysteresis_seconds", 1.0)),
        )

    def _rate_limit_storm_threshold(self) -> int:
        if not self._is_multiplex_engine():
            return 999999
        return max(
            2,
            int(getattr(self.args, "multiplex_rate_limit_storm_threshold", 3)),
        )

    def _rate_limit_storm_min_delay_seconds(self) -> float:
        if not self._is_multiplex_engine():
            return 0.0
        return max(
            1.0,
            float(getattr(self.args, "multiplex_rate_limit_storm_min_delay_seconds", 5.0)),
        )


class MultiplexSubscriptionManager(LegacySubscriptionManager):
    def __init__(
        self,
        *,
        args: argparse.Namespace,
        quote_store: QuoteStore,
        stop_event: threading.Event,
        print_line: PrintFn,
        debug: DebugFn,
        exchange_builder: ExchangeBuilder,
        on_quote_update: Callable[[StreamKey], None],
    ) -> None:
        super().__init__(
            args=args,
            quote_store=quote_store,
            stop_event=stop_event,
            print_line=print_line,
            debug=debug,
            exchange_builder=exchange_builder,
            on_quote_update=on_quote_update,
        )
        self._exchange_ingestors: dict[str, MultiplexExchangeIngestor] = {}
        self._next_aging_refresh_at: dict[StreamKey, float] = {}
        self._next_stale_recover_at: dict[StreamKey, float] = {}

    def prepare_runtime(self) -> None:
        super().prepare_runtime()
        self._print_line(
            "[info] multiplex_orderbook_mode=exchange_level_ingestion "
            f"kalshi_initial_source_mode={self._initial_multiplex_source_mode('kalshi')} "
            f"polymarket_initial_source_mode={self._initial_multiplex_source_mode('polymarket')}"
        )

    def warmup_enabled(self, exchange: str) -> bool:
        return exchange == "kalshi" and self.warmup_batch_size(exchange) > 0

    def warmup_batch_size(self, exchange: str) -> int:
        if exchange != "kalshi":
            return 0
        return max(0, int(getattr(self.args, "multiplex_warmup_batch_size", 25)))

    def warmup_interval_seconds(self, exchange: str) -> float:
        if exchange != "kalshi":
            return 0.0
        return max(0.01, float(getattr(self.args, "multiplex_warmup_interval_seconds", 0.5)))

    def producer_thread_count(self, exchange: str) -> int:
        if exchange == "kalshi":
            default_count = 2
            attr_name = "multiplex_kalshi_worker_count"
        else:
            default_count = 1
            attr_name = "multiplex_polymarket_worker_count"
        requested_count = max(1, int(getattr(self.args, attr_name, default_count)))
        if (
            exchange == "polymarket"
            and self._initial_multiplex_source_mode(exchange) != "direct"
            and requested_count > 1
        ):
            self._debug(
                "multiplex worker clamp exchange=polymarket "
                f"requested={requested_count} effective=1 reason=pmxt_fetch_not_concurrency_safe"
            )
            return 1
        return requested_count

    def stale_recovery_seconds(self) -> float:
        default_seconds = max(1.0, float(getattr(self.args, "book_stale_seconds", 15.0)))
        return max(
            0.25,
            float(getattr(self.args, "multiplex_stale_recovery_seconds", default_seconds)),
        )

    def stale_recovery_cooldown_seconds(self) -> float:
        default_seconds = max(0.5, self.stale_recovery_seconds() / 2.0)
        return max(
            0.25,
            float(
                getattr(
                    self.args,
                    "multiplex_stale_recovery_cooldown_seconds",
                    default_seconds,
                )
            ),
        )

    def stale_recovery_batch_size(self) -> int:
        return max(
            1,
            int(getattr(self.args, "multiplex_stale_recovery_batch_size", 5)),
        )

    def effective_stale_recovery_batch_size(
        self,
        *,
        exchange: str,
        candidate_count: int,
    ) -> int:
        base_size = self.stale_recovery_batch_size()
        if exchange != "kalshi":
            return base_size
        return max(
            base_size,
            min(candidate_count, max(1, self.producer_thread_count(exchange)) * base_size),
        )

    def aging_refresh_seconds(self) -> float:
        default_seconds = max(0.25, self.stale_recovery_seconds() * 0.7)
        configured_value = getattr(self.args, "multiplex_aging_refresh_seconds", default_seconds)
        configured_seconds = (
            default_seconds if configured_value is None else float(configured_value)
        )
        return min(self.stale_recovery_seconds(), max(0.25, configured_seconds))

    def aging_refresh_cooldown_seconds(self) -> float:
        default_seconds = max(0.25, self.aging_refresh_seconds() / 2.0)
        configured_value = getattr(
            self.args,
            "multiplex_aging_refresh_cooldown_seconds",
            default_seconds,
        )
        return max(
            0.25,
            default_seconds if configured_value is None else float(configured_value),
        )

    def aging_refresh_batch_size(self) -> int:
        return max(
            1,
            int(getattr(self.args, "multiplex_aging_refresh_batch_size", 2)),
        )

    def effective_aging_refresh_batch_size(
        self,
        *,
        exchange: str,
        candidate_count: int,
    ) -> int:
        base_size = self.aging_refresh_batch_size()
        if exchange != "kalshi":
            return base_size
        return max(
            base_size,
            min(candidate_count, max(1, self.producer_thread_count(exchange)) * base_size),
        )

    def polymarket_direct_quiet_refresh_seconds(self) -> float:
        default_seconds = min(
            max(1.0, float(getattr(self.args, "book_stale_seconds", 15.0))),
            5.0,
        )
        return max(
            0.5,
            float(
                getattr(
                    self.args,
                    "multiplex_polymarket_direct_quiet_refresh_seconds",
                    default_seconds,
                )
            ),
        )

    def polymarket_direct_quiet_refresh_batch_size(self) -> int:
        return max(
            1,
            int(
                getattr(
                    self.args,
                    "multiplex_polymarket_direct_quiet_refresh_batch_size",
                    5,
                )
            ),
        )

    def replace_streams(self, stream_keys: set[StreamKey]) -> bool:
        next_by_exchange: dict[str, set[str]] = {}
        for exchange, outcome_id in stream_keys:
            next_by_exchange.setdefault(exchange, set()).add(outcome_id)

        current_by_exchange = {
            exchange: ingestor.outcome_ids()
            for exchange, ingestor in self._exchange_ingestors.items()
        }
        current_stream_keys: set[StreamKey] = set()
        for exchange, outcome_ids in current_by_exchange.items():
            current_stream_keys.update((exchange, outcome_id) for outcome_id in outcome_ids)
        if current_stream_keys == stream_keys:
            return False

        removed_ingestors: dict[str, MultiplexExchangeIngestor] = {}
        removed_exchanges = sorted(set(current_by_exchange.keys()) - set(next_by_exchange.keys()))
        for exchange in removed_exchanges:
            ingestor = self._exchange_ingestors.pop(exchange)
            removed_ingestors[exchange] = ingestor
            ingestor.stop()
            self._close_exchange_client(exchange)
            with self._state_lock:
                self._exchange_source_mode.pop(exchange, None)
                self._exchange_source_mode_hold_until.pop(exchange, None)
                self._exchange_source_preference.pop(exchange, None)
            self._debug(
                f"multiplex subscriptions updated exchange={exchange} add=0 remove={len(current_by_exchange[exchange])} keep=0 total=0"
            )

        for exchange, outcome_ids in sorted(next_by_exchange.items()):
            ingestor = self._exchange_ingestors.get(exchange)
            if ingestor is None:
                ingestor = MultiplexExchangeIngestor(manager=self, exchange=exchange)
                ingestor.replace_subscriptions(outcome_ids)
                self._exchange_ingestors[exchange] = ingestor
                with self._state_lock:
                    self._exchange_source_preference[exchange] = (
                        self._initial_multiplex_source_mode(exchange)
                    )
                self._set_exchange_source_mode(
                    exchange=exchange,
                    mode=self._healthy_exchange_source_mode(exchange),
                    reason="subscription_start",
                )
                ingestor.start()
                self._debug(
                    f"multiplex ingestor started exchange={exchange} streams={len(outcome_ids)} workers={ingestor.producer_thread_count()}"
                )
                self._debug(
                    f"multiplex subscriptions updated exchange={exchange} add={len(outcome_ids)} remove=0 keep=0 total={len(outcome_ids)}"
                )
                pending_count = len(ingestor.pending_outcome_ids())
                if pending_count > 0:
                    self._debug(
                        f"multiplex warmup queued exchange={exchange} active={len(ingestor.active_outcome_ids())} pending={pending_count}"
                    )
            else:
                current_outcome_ids = current_by_exchange.get(exchange, set())
                add_outcome_ids = outcome_ids - current_outcome_ids
                remove_outcome_ids = current_outcome_ids - outcome_ids
                add_count, remove_count, keep_count, promoted_count, pending_count = ingestor.apply_subscription_diff(
                    add_outcome_ids=add_outcome_ids,
                    remove_outcome_ids=remove_outcome_ids,
                )
                self._debug(
                    f"multiplex subscriptions updated exchange={exchange} "
                    f"add={add_count} remove={remove_count} keep={keep_count} total={len(outcome_ids)}"
                )
                if add_count > 0 or remove_count > 0:
                    self._debug(
                        f"multiplex warmup state exchange={exchange} active={len(ingestor.active_outcome_ids())} pending={pending_count} promoted_now={promoted_count}"
                    )

        if removed_exchanges:
            self._drain_exchange_ingestors(
                ingestors=removed_ingestors,
                timeout_seconds=3.0,
                reason="restart",
            )
        return True

    def health_state(self, *, now: float) -> HealthState:
        health = super().health_state(now=now)
        active_streams: set[StreamKey] = set()
        exchange_subscription_counts: dict[str, int] = {}
        exchange_active_counts: dict[str, int] = {}
        exchange_pending_counts: dict[str, int] = {}
        exchange_priority_counts: dict[str, int] = {}
        exchange_queue_depth_counts: dict[str, int] = {}
        exchange_dispatch_drop_counts: dict[str, int] = {}
        for ingestor in self._exchange_ingestors.values():
            active_streams.update(ingestor.subscription_keys())
            exchange_subscription_counts[ingestor.exchange] = len(ingestor.outcome_ids())
            exchange_active_counts[ingestor.exchange] = len(ingestor.active_outcome_ids())
            exchange_pending_counts[ingestor.exchange] = len(ingestor.pending_outcome_ids())
            exchange_priority_counts[ingestor.exchange] = len(ingestor.priority_outcome_ids())
            dispatch_queue_depth = ingestor.dispatch_queue_depth()
            exchange_queue_depth_counts[ingestor.exchange] = (
                exchange_pending_counts[ingestor.exchange]
                + exchange_priority_counts[ingestor.exchange]
                + dispatch_queue_depth
            )
            exchange_dispatch_drop_counts[ingestor.exchange] = ingestor.dropped_dispatch_count()
        return HealthState(
            active_streams=sorted(active_streams),
            quote_count=health.quote_count,
            stale_quote_count=health.stale_quote_count,
            client_exchanges=health.client_exchanges,
            exchange_subscription_counts=exchange_subscription_counts,
            exchange_active_counts=exchange_active_counts,
            exchange_pending_counts=exchange_pending_counts,
            exchange_priority_counts=exchange_priority_counts,
            exchange_queue_depth_counts=exchange_queue_depth_counts,
            exchange_dispatch_drop_counts=exchange_dispatch_drop_counts,
            exchange_source_modes=health.exchange_source_modes,
            exchange_modes=health.exchange_modes,
            exchange_recovery_reasons=health.exchange_recovery_reasons,
            retry_windows=health.retry_windows,
            backoffs=health.backoffs,
            rate_limit_counts=health.rate_limit_counts,
            sidecar_error_counts=health.sidecar_error_counts,
            auth_error_counts=health.auth_error_counts,
            success_counts=health.success_counts,
            error_counts=health.error_counts,
            latencies_ms=health.latencies_ms,
            last_ok_at=health.last_ok_at,
        )

    def stop(self, *, reason: str, timeout_seconds: float) -> None:
        for ingestor in self._exchange_ingestors.values():
            ingestor.stop()
        self._close_all_exchange_clients()
        self._drain_exchange_ingestors(
            ingestors=dict(self._exchange_ingestors),
            timeout_seconds=timeout_seconds,
            reason=reason,
        )

    def _should_use_polymarket_direct_ws(self, exchange: str) -> bool:
        return exchange == "polymarket" and self._healthy_exchange_source_mode(exchange) == "direct"

    def should_start_direct_quiet_refresh_thread(self, exchange: str) -> bool:
        return self._should_use_polymarket_direct_ws(exchange)

    def _enqueue_multiplex_quote(
        self,
        *,
        ingestor: MultiplexExchangeIngestor,
        exchange: str,
        outcome_id: str,
        best_bid: float | None,
        best_ask: float | None,
        book_timestamp_ms: int | None,
        received_at: float,
        source_latency_ms: float | None,
        source_label: str,
        worker_index: int | None = None,
    ) -> None:
        quote = QuoteSnapshot(
            exchange=exchange,
            outcome_id=outcome_id,
            best_bid=best_bid,
            best_ask=best_ask,
            book_timestamp_ms=book_timestamp_ms,
            updated_at=received_at,
            quote_seen_at=received_at,
            source_label=source_label,
        )
        watch_latency_ms = 0.0 if source_latency_ms is None else max(0.0, source_latency_ms)
        dropped_count = ingestor.enqueue_quote_update(
            QueuedQuoteUpdate(
                quote=quote,
                watch_latency_ms=watch_latency_ms,
                received_at=received_at,
                worker_index=worker_index,
            )
        )
        if dropped_count > 0:
            self._debug(
                f"multiplex dispatch queue dropped exchange={exchange} "
                f"outcome_id={outcome_id} dropped={dropped_count} "
                f"depth={ingestor.dispatch_queue_depth()}"
            )

    def _run_polymarket_direct_loop(self, ingestor: MultiplexExchangeIngestor) -> None:
        exchange = ingestor.exchange
        self._debug("multiplex direct ws loop entering exchange=polymarket")

        while not ingestor.should_stop():
            desired_outcome_ids = ingestor.outcome_ids()
            if not desired_outcome_ids:
                time.sleep(0.05)
                continue

            try:
                client = self._get_polymarket_ws_client()
                add_outcome_ids, remove_outcome_ids = client.sync_subscriptions(desired_outcome_ids)
                if add_outcome_ids or remove_outcome_ids:
                    self._debug(
                        "multiplex direct ws sync exchange=polymarket "
                        f"add={len(add_outcome_ids)} remove={len(remove_outcome_ids)} "
                        f"total={len(desired_outcome_ids)}"
                    )
                quote_updates = client.recv_quote_updates()
            except TimeoutError:
                self._refresh_polymarket_direct_quiet_books(
                    ingestor=ingestor,
                    now=time.time(),
                )
                continue
            except Exception as exc:  # pragma: no cover - operational visibility
                self._print_line(
                    "[error] multiplex direct ws failure exchange=polymarket "
                    f"error={exc}"
                )
                self._register_exchange_failure(
                    exchange=exchange,
                    reason="direct_ws_failure",
                    recovery_reason="direct_ws_reconnect",
                )
                self._close_exchange_client(exchange)
                time.sleep(1.0)
                continue

            if not quote_updates:
                continue

            for quote_update in quote_updates:
                if quote_update.outcome_id not in desired_outcome_ids:
                    continue
                self._enqueue_multiplex_quote(
                    ingestor=ingestor,
                    exchange=exchange,
                    outcome_id=quote_update.outcome_id,
                    best_bid=quote_update.best_bid,
                    best_ask=quote_update.best_ask,
                    book_timestamp_ms=quote_update.book_timestamp_ms,
                    received_at=quote_update.received_at,
                    source_latency_ms=quote_update.source_latency_ms,
                    source_label=quote_update.source_label,
                )

        self._close_exchange_client(exchange)
        self._debug("multiplex direct ws loop exiting exchange=polymarket")

    def direct_quiet_refresh_loop(self, ingestor: MultiplexExchangeIngestor) -> None:
        exchange = ingestor.exchange
        if not self._should_use_polymarket_direct_ws(exchange):
            return

        self._debug("multiplex direct quiet refresh loop entering exchange=polymarket")
        refresh_interval_seconds = min(
            0.5,
            max(0.1, self.polymarket_direct_quiet_refresh_seconds() / 4.0),
        )
        while not ingestor.should_stop():
            if not ingestor.outcome_ids():
                time.sleep(0.05)
                continue
            self._refresh_polymarket_direct_quiet_books(
                ingestor=ingestor,
                now=time.time(),
            )
            time.sleep(refresh_interval_seconds)
        self._debug("multiplex direct quiet refresh loop exiting exchange=polymarket")

    def _refresh_polymarket_direct_quiet_books(
        self,
        *,
        ingestor: MultiplexExchangeIngestor,
        now: float,
    ) -> None:
        exchange = ingestor.exchange
        if exchange != "polymarket":
            return

        refresh_after_seconds = self.polymarket_direct_quiet_refresh_seconds()
        refresh_batch_size = self.polymarket_direct_quiet_refresh_batch_size()
        refresh_candidates: list[tuple[int, float, str]] = []
        with self._state_lock:
            last_ok = dict(self._stream_last_watch_ok_at)
            next_recover_at = dict(self._next_stale_recover_at)
        active_since = ingestor.active_since_by_outcome_id()

        for outcome_id in sorted(ingestor.active_outcome_ids()):
            stream_key = (exchange, outcome_id)
            if now < next_recover_at.get(stream_key, 0.0):
                continue
            last_ok_at = last_ok.get(stream_key)
            if last_ok_at is None:
                active_at = active_since.get(outcome_id)
                if active_at is None or (now - active_at) < refresh_after_seconds:
                    continue
                refresh_candidates.append((0, -(now - active_at), outcome_id))
                continue
            age_seconds = now - last_ok_at
            if age_seconds < refresh_after_seconds:
                continue
            refresh_candidates.append((1, -age_seconds, outcome_id))

        if not refresh_candidates:
            return

        refresh_candidates.sort()
        selected_outcome_ids = [
            outcome_id for _, _, outcome_id in refresh_candidates[:refresh_batch_size]
        ]
        try:
            client = self._get_exchange_client(exchange)
        except Exception:
            return

        refreshed_count = 0
        for outcome_id in selected_outcome_ids:
            try:
                fetch_started_at = time.time()
                book = self._fetch_order_book(
                    client=client,
                    exchange=exchange,
                    outcome_id=outcome_id,
                )
                source_latency_ms = None
                book_timestamp_ms = extract_book_timestamp_ms(book)
                if book_timestamp_ms is not None:
                    source_latency_ms = (time.time() * 1000.0) - float(book_timestamp_ms)
                best_bid, best_ask = extract_best_prices(book)
                self._enqueue_multiplex_quote(
                    ingestor=ingestor,
                    exchange=exchange,
                    outcome_id=outcome_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    book_timestamp_ms=book_timestamp_ms,
                    received_at=time.time(),
                    source_latency_ms=source_latency_ms,
                    source_label="quiet_refresh",
                )
                with self._state_lock:
                    self._next_stale_recover_at[(exchange, outcome_id)] = (
                        now + refresh_after_seconds
                    )
                refreshed_count += 1
                self._debug(
                    "multiplex direct quiet refresh detail "
                    f"exchange=polymarket outcome_id={outcome_id} "
                    f"fetch_ms={(time.time() - fetch_started_at) * 1000.0:.3f}"
                )
            except Exception as exc:  # pragma: no cover - operational visibility
                if self._is_missing_order_book_error(exc):
                    disabled_count, pending_count = ingestor.disable_outcome_ids([outcome_id])
                    if disabled_count > 0:
                        stream_key = (exchange, outcome_id)
                        self._quote_store.deactivate_streams({stream_key})
                        self._print_line(
                            "[warn] disabled invalid order book outcome "
                            f"exchange={exchange} outcome_id={outcome_id} "
                            "reason=order_book_not_found"
                        )
                        self._debug(
                            "multiplex direct quiet refresh disabled invalid outcome "
                            f"exchange={exchange} outcome_id={outcome_id} "
                            f"remaining={len(ingestor.outcome_ids())} pending={pending_count}"
                        )
                continue

        if refreshed_count > 0:
            self._debug(
                "multiplex direct quiet refresh exchange=polymarket "
                f"outcomes={','.join(selected_outcome_ids[:5])} count={refreshed_count} "
                f"eligible={len(refresh_candidates)}"
            )

    def worker_loop(
        self,
        ingestor: MultiplexExchangeIngestor,
        *,
        worker_index: int = 0,
    ) -> None:
        exchange = ingestor.exchange
        self._debug(
            f"multiplex worker loop entering exchange={exchange} worker_index={worker_index}"
        )

        if self._should_use_polymarket_direct_ws(exchange):
            self._run_polymarket_direct_loop(ingestor)
            return

        while not ingestor.should_stop():
            promoted_count, active_count, pending_count = ingestor.release_pending_if_due(
                now=time.monotonic()
            )
            if promoted_count > 0:
                self._debug(
                    f"multiplex warmup released exchange={exchange} promoted={promoted_count} active={active_count} pending={pending_count}"
                )
            self._promote_aging_outcomes(
                exchange=exchange,
                ingestor=ingestor,
                now=time.time(),
            )
            self._recover_stale_outcomes(
                exchange=exchange,
                ingestor=ingestor,
                now=time.time(),
            )
            outcome_id = ingestor.next_outcome_id()
            if outcome_id is None:
                time.sleep(0.05)
                continue

            self._wait_for_exchange_retry(exchange=exchange, ingestor=ingestor)
            if ingestor.should_stop():
                return

            client = self._get_client_for_worker(
                exchange=exchange,
                worker_index=worker_index,
            )
            try:
                watch_start = time.time()
                book = self._fetch_multiplex_order_book(
                    client=client,
                    exchange=exchange,
                    outcome_id=outcome_id,
                    worker_index=worker_index,
                )
                watch_latency_ms = (time.time() - watch_start) * 1000.0
            except Exception as exc:  # pragma: no cover - operational visibility
                stream_key = (exchange, outcome_id)
                with self._state_lock:
                    self._stream_watch_error_count[stream_key] = (
                        self._stream_watch_error_count.get(stream_key, 0) + 1
                    )
                if self._is_rate_limited_error(exc):
                    self._register_rate_limit(
                        exchange=exchange,
                        outcome_id=outcome_id,
                        exc=exc,
                        worker_index=worker_index,
                    )
                    continue
                if self._is_auth_token_error(exc):
                    self._register_auth_error(
                        exchange=exchange,
                        outcome_id=outcome_id,
                        exc=exc,
                    )
                    if exchange == "kalshi" and self._prepare_kalshi_sidecar_credentials():
                        self._switch_kalshi_to_direct_mode(reason="ws_auth_failed")
                        self._close_exchange_client(exchange)
                        continue
                    self._close_exchange_client(exchange)
                    self._maybe_restart_pmxt_sidecar(
                        reason="auth_token_invalid",
                        exchange=exchange,
                    )
                    continue
                if self._is_sidecar_unavailable_error(exc):
                    self._register_sidecar_unavailable(
                        exchange=exchange,
                        outcome_id=outcome_id,
                        exc=exc,
                    )
                    self._close_exchange_client(exchange)
                    self._maybe_restart_pmxt_sidecar(
                        reason="connection_refused",
                        exchange=exchange,
                    )
                    continue
                if exchange == "polymarket" and self._is_missing_order_book_error(exc):
                    disabled_count, pending_count = ingestor.disable_outcome_ids([outcome_id])
                    if disabled_count > 0:
                        stream_key = (exchange, outcome_id)
                        self._quote_store.deactivate_streams({stream_key})
                        self._print_line(
                            "[warn] disabled invalid order book outcome "
                            f"exchange={exchange} outcome_id={outcome_id} "
                            "reason=order_book_not_found"
                        )
                        self._debug(
                            "multiplex disabled invalid outcome "
                            f"exchange={exchange} outcome_id={outcome_id} "
                            f"remaining={len(ingestor.outcome_ids())} pending={pending_count}"
                        )
                        continue
                self._print_line(
                    f"[error] multiplex stream failure exchange={exchange} "
                    f"outcome_id={outcome_id} error={exc}"
                )
                self._register_exchange_failure(
                    exchange=exchange,
                    reason="stream_failure",
                    outcome_id=outcome_id,
                )
                self._debug(
                    f"multiplex non-rate-limit failure exchange={exchange} "
                    f"outcome_id={outcome_id} error_type={type(exc).__name__}"
                )
                self._close_exchange_client(exchange)
                time.sleep(1.0)
                continue

            best_bid, best_ask = extract_best_prices(book)
            seen_at = time.time()
            self._enqueue_multiplex_quote(
                ingestor=ingestor,
                exchange=exchange,
                outcome_id=outcome_id,
                best_bid=best_bid,
                best_ask=best_ask,
                book_timestamp_ms=extract_book_timestamp_ms(book),
                received_at=seen_at,
                source_latency_ms=watch_latency_ms,
                source_label=self._current_exchange_source_mode(exchange),
                worker_index=worker_index,
            )
            time.sleep(self._loop_delay_seconds(exchange))

        self._debug(
            f"multiplex worker loop exiting exchange={exchange} worker_index={worker_index}"
        )

    def dispatch_loop(self, ingestor: MultiplexExchangeIngestor) -> None:
        exchange = ingestor.exchange
        self._debug(f"multiplex dispatch loop entering exchange={exchange}")

        while not ingestor.should_stop() or ingestor.dispatch_queue_depth() > 0:
            quote_update = ingestor.next_quote_update()
            if quote_update is None:
                time.sleep(0.01)
                continue

            quote = quote_update.quote
            stream_key = (quote.exchange, quote.outcome_id)
            with self._state_lock:
                self._stream_watch_success_count[stream_key] = (
                    self._stream_watch_success_count.get(stream_key, 0) + 1
                )
                self._stream_last_watch_latency_ms[stream_key] = quote_update.watch_latency_ms
                self._stream_last_watch_ok_at[stream_key] = quote_update.received_at
            ingestor.clear_forced_fetch_outcome_id(quote.outcome_id)
            stored_quote = replace(quote, updated_at=time.time())
            self._quote_store.upsert_quote(stored_quote)
            self._mark_stream_success(
                exchange,
                worker_index=quote_update.worker_index,
            )
            self._on_quote_update(stream_key)

        self._debug(f"multiplex dispatch loop exiting exchange={exchange}")

    def _fetch_multiplex_order_book(
        self,
        *,
        client: Any,
        exchange: str,
        outcome_id: str,
        worker_index: int | None = None,
    ) -> Any:
        if exchange == "kalshi" and self._current_exchange_source_mode(exchange) == "watch":
            ingestor = self._exchange_ingestors.get(exchange)
            if ingestor is not None and ingestor.should_force_fetch_outcome_id(outcome_id):
                return self._fetch_order_book(
                    client=client,
                    exchange=exchange,
                    outcome_id=outcome_id,
                    worker_index=worker_index,
                )
            return self._watch_order_book(
                client=client,
                exchange=exchange,
                outcome_id=outcome_id,
                worker_index=worker_index,
            )
        return self._fetch_order_book(
            client=client,
            exchange=exchange,
            outcome_id=outcome_id,
            worker_index=worker_index,
        )

    def _fetch_order_book(
        self,
        *,
        client: Any,
        exchange: str,
        outcome_id: str,
        worker_index: int | None = None,
    ) -> Any:
        if self._should_use_worker_scoped_client(exchange=exchange, worker_index=worker_index):
            return client.fetch_order_book(outcome_id)
        watch_lock = self._watch_locks.get(exchange)
        if watch_lock is None:
            return client.fetch_order_book(outcome_id)
        with watch_lock:
            return client.fetch_order_book(outcome_id)

    def _loop_delay_seconds(self, exchange: str) -> float:
        if exchange == "kalshi":
            return 0.05
        return 0.01

    def _drain_exchange_ingestors(
        self,
        *,
        ingestors: dict[str, MultiplexExchangeIngestor],
        timeout_seconds: float,
        reason: str,
    ) -> None:
        pending = dict(ingestors)
        deadline = time.time() + max(0.1, timeout_seconds)
        while pending and time.time() < deadline:
            for exchange, ingestor in list(pending.items()):
                ingestor.join(timeout_seconds=0.05)
                if not ingestor.is_alive():
                    pending.pop(exchange, None)
        if pending:
            sample = ", ".join(sorted(pending.keys()))
            self._debug(
                f"multiplex drain timeout reason={reason} lingering={len(pending)} sample=[{sample}]"
            )

    def _recover_stale_outcomes(
        self,
        *,
        exchange: str,
        ingestor: MultiplexExchangeIngestor,
        now: float,
    ) -> None:
        stale_after_seconds = self.stale_recovery_seconds()
        cooldown_seconds = self.stale_recovery_cooldown_seconds()
        stale_candidates: list[tuple[int, float, str]] = []

        with self._state_lock:
            last_ok = dict(self._stream_last_watch_ok_at)
            next_recover_at = dict(self._next_stale_recover_at)
        active_since = ingestor.active_since_by_outcome_id()

        for outcome_id in sorted(ingestor.active_outcome_ids()):
            stream_key = (exchange, outcome_id)
            last_ok_at = last_ok.get(stream_key)
            if last_ok_at is None:
                active_at = active_since.get(outcome_id)
                if active_at is None or (now - active_at) < stale_after_seconds:
                    continue
                stale_age_seconds = now - active_at
                never_ok_rank = 0
            elif (now - last_ok_at) < stale_after_seconds:
                continue
            else:
                stale_age_seconds = now - last_ok_at
                never_ok_rank = 1
            if now < next_recover_at.get(stream_key, 0.0):
                continue
            stale_candidates.append((never_ok_rank, -stale_age_seconds, outcome_id))

        if not stale_candidates:
            return

        current_source_mode = self._current_exchange_source_mode(exchange)
        used_forced_fetch = exchange == "kalshi" and current_source_mode == "watch"
        stale_candidates.sort()
        base_recovery_budget = self.effective_stale_recovery_batch_size(
            exchange=exchange,
            candidate_count=len(stale_candidates),
        )
        if used_forced_fetch:
            recovery_budget = base_recovery_budget
        else:
            recovery_budget = max(
                0,
                base_recovery_budget - len(ingestor.priority_outcome_ids()),
            )
        if recovery_budget <= 0:
            return
        stale_outcome_ids = [
            outcome_id for _, _, outcome_id in stale_candidates[:recovery_budget]
        ]
        if used_forced_fetch:
            recovered_count = ingestor.request_forced_fetch_outcome_ids(stale_outcome_ids)
            pending_count = len(ingestor.pending_outcome_ids())
            if recovered_count <= 0:
                return
        else:
            recovered_count = ingestor.prioritize_outcome_ids(stale_outcome_ids)
            pending_count = len(ingestor.pending_outcome_ids())
            if recovered_count <= 0:
                return

        if recovered_count <= 0:
            return

        with self._state_lock:
            for outcome_id in stale_outcome_ids:
                self._next_stale_recover_at[(exchange, outcome_id)] = now + cooldown_seconds
            self._exchange_recovery_reason[exchange] = "stale_refresh"
        self._set_exchange_mode(
            exchange=exchange,
            mode="recovering",
            reason="stale_recovery",
            outcome_id=stale_outcome_ids[0] if stale_outcome_ids else None,
        )
        self._emit_recovery_policy(
            exchange=exchange,
            policy="stale_refresh",
            mode="recovering",
            outcome_id=stale_outcome_ids[0] if stale_outcome_ids else None,
        )

        joined_outcomes = ",".join(stale_outcome_ids[:5])
        if used_forced_fetch:
            self._debug(
                f"multiplex forced snapshot refresh exchange={exchange} "
                f"outcomes={joined_outcomes} count={recovered_count} "
                f"eligible={len(stale_candidates)} pending={pending_count}"
            )
        else:
            self._debug(
                f"multiplex stale recovery exchange={exchange} "
                f"outcomes={joined_outcomes} count={recovered_count} "
                f"eligible={len(stale_candidates)}"
            )

    def _promote_aging_outcomes(
        self,
        *,
        exchange: str,
        ingestor: MultiplexExchangeIngestor,
        now: float,
    ) -> None:
        refresh_after_seconds = self.aging_refresh_seconds()
        stale_after_seconds = self.stale_recovery_seconds()
        cooldown_seconds = self.aging_refresh_cooldown_seconds()
        aging_candidates: list[tuple[float, str]] = []

        with self._state_lock:
            last_ok = dict(self._stream_last_watch_ok_at)
            next_refresh_at = dict(self._next_aging_refresh_at)

        for outcome_id in sorted(ingestor.active_outcome_ids()):
            stream_key = (exchange, outcome_id)
            last_ok_at = last_ok.get(stream_key)
            if last_ok_at is None:
                continue
            age_seconds = now - last_ok_at
            if age_seconds < refresh_after_seconds or age_seconds >= stale_after_seconds:
                continue
            if now < next_refresh_at.get(stream_key, 0.0):
                continue
            aging_candidates.append((-age_seconds, outcome_id))

        if not aging_candidates:
            return

        base_refresh_budget = self.effective_aging_refresh_batch_size(
            exchange=exchange,
            candidate_count=len(aging_candidates),
        )
        refresh_budget = max(
            0,
            base_refresh_budget - len(ingestor.priority_outcome_ids()),
        )
        if refresh_budget <= 0:
            return

        aging_candidates.sort()
        outcome_ids = [
            outcome_id for _, outcome_id in aging_candidates[:refresh_budget]
        ]
        if exchange == "kalshi" and self._current_exchange_source_mode(exchange) == "watch":
            refreshed_count = ingestor.request_forced_fetch_outcome_ids(outcome_ids)
        else:
            refreshed_count = ingestor.prioritize_outcome_ids(outcome_ids)
        if refreshed_count <= 0:
            return

        with self._state_lock:
            for outcome_id in outcome_ids:
                self._next_aging_refresh_at[(exchange, outcome_id)] = now + cooldown_seconds

        joined_outcomes = ",".join(outcome_ids[:5])
        self._debug(
            f"multiplex aging refresh exchange={exchange} outcomes={joined_outcomes} "
            f"count={refreshed_count} eligible={len(aging_candidates)} "
            f"threshold_s={refresh_after_seconds:.3f}"
        )


class ArbAlertRunner:
    def __init__(
        self,
        args: argparse.Namespace,
        *,
        http_get_json: Callable[..., dict[str, Any]],
        exchange_builder: ExchangeBuilder,
    ) -> None:
        self.args = args
        self._http_get_json = http_get_json
        self._debug_enabled = bool(getattr(args, "debug", False))
        self._debug_heartbeat_seconds = max(
            1.0,
            float(getattr(args, "debug_heartbeat_seconds", 10.0)),
        )
        self._compact_alerts = bool(getattr(args, "compact_alerts", False))
        self._use_color = self._should_use_color(getattr(args, "color", "auto"))

        self.stop_event = threading.Event()
        self._print_lock = threading.Lock()
        self._store = QuoteStore()
        self._engine_name = str(getattr(args, "engine", "legacy")).lower()
        self._runner_started_at = time.time()
        self._quote_update_count = 0
        self._quote_updates_since_last_eval = 0
        self._last_quote_update_at = 0.0
        self._last_eval_completed_at = 0.0
        self._last_eval_duration_ms = 0.0
        self._last_eval_mapping_count = 0
        self._last_eval_raw_event_count = 0
        self._last_eval_due_event_count = 0
        self._last_eval_emitted_count = 0
        self._eval_count = 0
        self._latency_tracker = RollingLatencyTracker()
        self._mapping_subscription_debounce_seconds = max(
            0.0,
            float(getattr(args, "mapping_subscription_debounce_seconds", 0.5)),
        )
        self._last_applied_stream_keys: set[StreamKey] = set()
        self._pending_stream_keys: set[StreamKey] | None = None
        self._pending_stream_reason: str | None = None
        self._pending_stream_apply_at = 0.0
        self._subscription_manager = self._build_subscription_manager(
            exchange_builder=exchange_builder,
        )

    def _build_subscription_manager(self, *, exchange_builder: ExchangeBuilder) -> Any:
        if self._engine_name == "legacy":
            return LegacySubscriptionManager(
                args=self.args,
                quote_store=self._store,
                stop_event=self.stop_event,
                print_line=self._print_line,
                debug=self._debug,
                exchange_builder=exchange_builder,
                on_quote_update=self._handle_quote_update,
            )
        if self._engine_name == "multiplex":
            return MultiplexSubscriptionManager(
                args=self.args,
                quote_store=self._store,
                stop_event=self.stop_event,
                print_line=self._print_line,
                debug=self._debug,
                exchange_builder=exchange_builder,
                on_quote_update=self._handle_quote_update,
            )
        return UnsupportedSubscriptionManager(engine=self._engine_name)

    def _record_latency_metrics(
        self,
        *,
        exchange: str,
        source_label: str | None = None,
        quote_seen_to_alert_ms: float | None,
        book_timestamp_to_alert_ms: float | None,
        exchange_update_to_store_ms: float | None,
        store_to_alert_ms: float | None,
    ) -> None:
        self._latency_tracker.add(
            metric_name="quote_seen_to_alert_ms",
            exchange=exchange,
            source_label=source_label,
            value_ms=quote_seen_to_alert_ms,
        )
        self._latency_tracker.add(
            metric_name="book_timestamp_to_alert_ms",
            exchange=exchange,
            source_label=source_label,
            value_ms=book_timestamp_to_alert_ms,
        )
        self._latency_tracker.add(
            metric_name="exchange_update_to_store_ms",
            exchange=exchange,
            source_label=source_label,
            value_ms=exchange_update_to_store_ms,
        )
        self._latency_tracker.add(
            metric_name="store_to_alert_ms",
            exchange=exchange,
            source_label=source_label,
            value_ms=store_to_alert_ms,
        )

    def _latency_metric_fields(self, metric_name: str) -> tuple[dict[str, int], dict[str, float], dict[str, float], dict[str, float]]:
        return (
            self._latency_tracker.sample_counts(metric_name),
            self._latency_tracker.percentile_summary(metric_name, pct=0.50),
            self._latency_tracker.percentile_summary(metric_name, pct=0.95),
            self._latency_tracker.percentile_summary(metric_name, pct=0.99),
        )

    def _print_line(self, line: str) -> None:
        with self._print_lock:
            print(line)

    def _debug(self, line: str) -> None:
        if self._debug_enabled:
            self._print_line(f"[debug] {line}")

    def _fetch_monitoring_pairs(self) -> dict[str, Any]:
        payload = self._http_get_json(
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
        payload = self._http_get_json(self.args.api_base_url, f"/api/pairs/{pair_id}")
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

    def _clear_pending_stream_update(self) -> None:
        self._pending_stream_keys = None
        self._pending_stream_reason = None
        self._pending_stream_apply_at = 0.0

    def _apply_stream_subscription_update(
        self,
        *,
        stream_keys: set[StreamKey],
        reason: str,
    ) -> None:
        desired_stream_keys = set(stream_keys)
        if desired_stream_keys == self._last_applied_stream_keys:
            return
        self._subscription_manager.replace_streams(desired_stream_keys)
        self._last_applied_stream_keys = desired_stream_keys
        self._print_line(
            "[info] updated stream subscriptions "
            f"reason={reason} streams={len(desired_stream_keys)}"
        )

    def _should_debounce_stream_update(self, *, reason: str) -> bool:
        return (
            self._engine_name == "multiplex"
            and reason != "startup"
            and self._mapping_subscription_debounce_seconds > 0.0
        )

    def _queue_stream_subscription_update(
        self,
        *,
        stream_keys: set[StreamKey],
        reason: str,
        now: float,
    ) -> None:
        desired_stream_keys = set(stream_keys)
        if desired_stream_keys == self._last_applied_stream_keys:
            if self._pending_stream_keys is not None:
                self._debug(
                    f"cleared debounced stream subscription update reason={reason} "
                    f"streams={len(desired_stream_keys)}"
                )
            self._clear_pending_stream_update()
            return

        if not self._should_debounce_stream_update(reason=reason):
            self._clear_pending_stream_update()
            self._apply_stream_subscription_update(
                stream_keys=desired_stream_keys,
                reason=reason,
            )
            return

        self._pending_stream_keys = desired_stream_keys
        self._pending_stream_reason = reason
        self._pending_stream_apply_at = now + self._mapping_subscription_debounce_seconds
        self._debug(
            "debounced stream subscription update "
            f"reason={reason} streams={len(desired_stream_keys)} "
            f"apply_in={self._mapping_subscription_debounce_seconds:.3f}s"
        )

    def _flush_pending_stream_subscription_update(self, now: float) -> bool:
        if self._pending_stream_keys is None or now < self._pending_stream_apply_at:
            return False

        stream_keys = set(self._pending_stream_keys)
        reason = self._pending_stream_reason or "periodic"
        self._clear_pending_stream_update()
        self._apply_stream_subscription_update(
            stream_keys=stream_keys,
            reason=reason,
        )
        return True

    def _refresh_mappings(self, *, reason: str) -> None:
        mappings, warnings = self._load_semantic_mappings()
        for warning in warnings:
            self._print_line(f"[warn] {warning}")

        next_stream_keys = mapping_stream_keys(mappings, canonicalize_kalshi=True)
        previous_stream_keys, active_stream_keys = self._store.replace_mappings(
            mappings,
            next_stream_keys,
        )

        if active_stream_keys != previous_stream_keys:
            self._queue_stream_subscription_update(
                stream_keys=active_stream_keys,
                reason=reason,
                now=time.time(),
            )

        self._print_line(
            "[info] mapping refresh "
            f"reason={reason} mappings={len(mappings)} streams={len(active_stream_keys)}"
        )
        self._evaluate_and_emit()

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

    def _outcome_label(self, label: str | None, fallback: str) -> str:
        if isinstance(label, str) and label.strip():
            return label.strip()
        return fallback

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

    def _alert_latency_fields(self, evaluated_event: Any) -> dict[str, str]:
        fields: dict[str, str] = {}
        trigger_stream_key = getattr(evaluated_event, "trigger_stream_key", None)
        if isinstance(trigger_stream_key, tuple) and len(trigger_stream_key) == 2:
            fields["trigger_exchange"] = str(trigger_stream_key[0])
            fields["trigger_outcome_id"] = str(trigger_stream_key[1])
        trigger_quote_source = getattr(evaluated_event, "trigger_quote_source", None)
        if isinstance(trigger_quote_source, str) and trigger_quote_source:
            fields["trigger_source"] = trigger_quote_source
        for field_name in (
            "quote_seen_to_alert_ms",
            "book_timestamp_to_alert_ms",
            "exchange_update_to_store_ms",
            "store_to_alert_ms",
        ):
            raw_value = getattr(evaluated_event, field_name, None)
            if isinstance(raw_value, (int, float)):
                fields[field_name] = f"{float(raw_value):.3f}"
        return fields

    def _format_alert_block(self, evaluated_event: Any, *, emitted_at: datetime) -> str:
        event = getattr(evaluated_event, "event", evaluated_event)
        latency_fields = self._alert_latency_fields(evaluated_event)
        if self._compact_alerts:
            return format_alert_line(
                event,
                emitted_at=emitted_at,
                extra_fields=latency_fields,
            )

        summary, legs = self._metric_summary(event)
        value = self._colorize(f"{event.metric_value:.4f}", "1;32")
        threshold = self._colorize(f"{event.threshold:.4f}", "1;37")
        kalshi_name = event.kalshi_market_title or "Kalshi market"
        polymarket_name = event.polymarket_market_title or "Polymarket market"
        markets = (
            f"Kalshi={kalshi_name} ({event.kalshi_market_id}) | "
            f"Polymarket={polymarket_name} ({event.polymarket_market_id})"
        )
        outcomes = (
            f"Kalshi P={self._outcome_label(event.kalshi_p_label, 'YES')} "
            f"| Kalshi NOT_P={self._outcome_label(event.kalshi_not_p_label, 'NO')} "
            f"|| Polymarket P={self._outcome_label(event.polymarket_p_label, 'YES')} "
            f"| Polymarket NOT_P={self._outcome_label(event.polymarket_not_p_label, 'NO')}"
        )
        metric = self._colorize(event.metric_name, "35")
        relation = self._colorize(event.relation_type, "36")
        divider = self._colorize("-" * 96, "2")
        latency_line = "  Latency: unavailable"
        if latency_fields:
            latency_line = "  Latency: " + " ".join(
                f"{key}={value}" for key, value in sorted(latency_fields.items())
            )

        return "\n".join(
            [
                (
                    f"[{event.tag}] ts={emitted_at.isoformat()} pair_id={event.pair_id} "
                    f"{self._tag_accent(event.tag)}"
                ),
                f"  What: {summary}",
                f"  Metric: {metric} | value={value} threshold={threshold} | relation={relation}",
                f"  Markets: {self._colorize(markets, '2')}",
                f"  Outcomes: {outcomes}",
                f"  Legs: {legs}",
                latency_line,
                "  Note: Informational top-of-book signal only (fees, depth, and execution risk excluded).",
                divider,
            ]
        )

    def _emit_debug_heartbeat(self, now: float) -> None:
        health = self._subscription_manager.health_state(now=now)
        eval_age_s = "na"
        if self._last_eval_completed_at > 0.0:
            eval_age_s = f"{(now - self._last_eval_completed_at):.3f}"
        quote_update_age_s = "na"
        if self._last_quote_update_at > 0.0:
            quote_update_age_s = f"{(now - self._last_quote_update_at):.3f}"
        uptime_seconds = max(1e-6, now - self._runner_started_at)
        evals_per_min = (self._eval_count * 60.0) / uptime_seconds
        quote_updates_per_min = (self._quote_update_count * 60.0) / uptime_seconds
        exchange_update_counts: dict[str, int] = {}
        exchange_updates_per_min: dict[str, float] = {}
        exchange_last_ok_age_p95_s: dict[str, float] = {}
        exchange_last_ok_age_max_s: dict[str, float] = {}
        exchange_latency_p95_ms: dict[str, float] = {}
        exchange_latency_max_ms: dict[str, float] = {}
        per_exchange_ages: dict[str, list[float]] = {}
        per_exchange_latencies: dict[str, list[float]] = {}
        for stream_key in health.active_streams:
            exchange = stream_key[0]
            exchange_update_counts[exchange] = (
                exchange_update_counts.get(exchange, 0)
                + int(health.success_counts.get(stream_key, 0))
            )
            last_ok_at = health.last_ok_at.get(stream_key)
            if last_ok_at is not None:
                per_exchange_ages.setdefault(exchange, []).append(now - last_ok_at)
            latency_ms = health.latencies_ms.get(stream_key)
            if latency_ms is not None:
                per_exchange_latencies.setdefault(exchange, []).append(float(latency_ms))
        for exchange, count in exchange_update_counts.items():
            exchange_updates_per_min[exchange] = (count * 60.0) / uptime_seconds
        for exchange, ages in per_exchange_ages.items():
            p95_age = _percentile(ages, 0.95)
            if p95_age is not None:
                exchange_last_ok_age_p95_s[exchange] = p95_age
                exchange_last_ok_age_max_s[exchange] = max(ages)
        for exchange, latencies in per_exchange_latencies.items():
            p95_latency = _percentile(latencies, 0.95)
            if p95_latency is not None:
                exchange_latency_p95_ms[exchange] = p95_latency
                exchange_latency_max_ms[exchange] = max(latencies)
        (
            quote_seen_to_alert_sample_count,
            quote_seen_to_alert_p50_ms,
            quote_seen_to_alert_p95_ms,
            quote_seen_to_alert_p99_ms,
        ) = self._latency_metric_fields("quote_seen_to_alert_ms")
        (
            book_timestamp_to_alert_sample_count,
            book_timestamp_to_alert_p50_ms,
            book_timestamp_to_alert_p95_ms,
            book_timestamp_to_alert_p99_ms,
        ) = self._latency_metric_fields("book_timestamp_to_alert_ms")
        (
            exchange_update_to_store_sample_count,
            exchange_update_to_store_p50_ms,
            exchange_update_to_store_p95_ms,
            exchange_update_to_store_p99_ms,
        ) = self._latency_metric_fields("exchange_update_to_store_ms")
        (
            store_to_alert_sample_count,
            store_to_alert_p50_ms,
            store_to_alert_p95_ms,
            store_to_alert_p99_ms,
        ) = self._latency_metric_fields("store_to_alert_ms")
        self._debug(
            "heartbeat "
            f"active_streams={len(health.active_streams)} quote_count={health.quote_count} "
            f"stale_quote_count={health.stale_quote_count} "
            f"clients={health.client_exchanges} exchange_source_modes={health.exchange_source_modes} "
            f"exchange_modes={health.exchange_modes} "
            f"exchange_recovery_reasons={health.exchange_recovery_reasons} "
            f"exchange_subscription_counts={health.exchange_subscription_counts} "
            f"exchange_active_counts={health.exchange_active_counts} "
            f"exchange_pending_counts={health.exchange_pending_counts} "
            f"exchange_priority_counts={health.exchange_priority_counts} "
            f"exchange_queue_depth_counts={health.exchange_queue_depth_counts} "
            f"exchange_dispatch_drop_counts={health.exchange_dispatch_drop_counts} "
            f"last_eval_duration_ms={self._last_eval_duration_ms:.3f} eval_age_s={eval_age_s} "
            f"last_eval_mapping_count={self._last_eval_mapping_count} "
            f"last_eval_raw_event_count={self._last_eval_raw_event_count} "
            f"last_eval_due_event_count={self._last_eval_due_event_count} "
            f"last_eval_emitted_count={self._last_eval_emitted_count} "
            f"eval_count={self._eval_count} evals_per_min={evals_per_min:.3f} "
            f"quote_update_count={self._quote_update_count} "
            f"quote_updates_since_last_eval={self._quote_updates_since_last_eval} "
            f"last_quote_update_age_s={quote_update_age_s} "
            f"quote_updates_per_min={quote_updates_per_min:.3f} "
            f"exchange_update_counts={exchange_update_counts} "
            f"exchange_updates_per_min={exchange_updates_per_min} "
            f"exchange_last_ok_age_p95_s={exchange_last_ok_age_p95_s} "
            f"exchange_last_ok_age_max_s={exchange_last_ok_age_max_s} "
            f"exchange_latency_p95_ms={exchange_latency_p95_ms} "
            f"exchange_latency_max_ms={exchange_latency_max_ms} "
            f"quote_seen_to_alert_sample_count={quote_seen_to_alert_sample_count} "
            f"quote_seen_to_alert_p50_ms={quote_seen_to_alert_p50_ms} "
            f"quote_seen_to_alert_p95_ms={quote_seen_to_alert_p95_ms} "
            f"quote_seen_to_alert_p99_ms={quote_seen_to_alert_p99_ms} "
            f"book_timestamp_to_alert_sample_count={book_timestamp_to_alert_sample_count} "
            f"book_timestamp_to_alert_p50_ms={book_timestamp_to_alert_p50_ms} "
            f"book_timestamp_to_alert_p95_ms={book_timestamp_to_alert_p95_ms} "
            f"book_timestamp_to_alert_p99_ms={book_timestamp_to_alert_p99_ms} "
            f"exchange_update_to_store_sample_count={exchange_update_to_store_sample_count} "
            f"exchange_update_to_store_p50_ms={exchange_update_to_store_p50_ms} "
            f"exchange_update_to_store_p95_ms={exchange_update_to_store_p95_ms} "
            f"exchange_update_to_store_p99_ms={exchange_update_to_store_p99_ms} "
            f"store_to_alert_sample_count={store_to_alert_sample_count} "
            f"store_to_alert_p50_ms={store_to_alert_p50_ms} "
            f"store_to_alert_p95_ms={store_to_alert_p95_ms} "
            f"store_to_alert_p99_ms={store_to_alert_p99_ms} "
            f"backoffs={health.backoffs} "
            f"retry_in={health.retry_windows} rate_limit_counts={health.rate_limit_counts} "
            f"sidecar_error_counts={health.sidecar_error_counts} "
            f"auth_error_counts={health.auth_error_counts}"
        )
        for stream_key in health.active_streams:
            success = health.success_counts.get(stream_key, 0)
            errors = health.error_counts.get(stream_key, 0)
            latency = health.latencies_ms.get(stream_key)
            ok_age = (now - health.last_ok_at[stream_key]) if stream_key in health.last_ok_at else None
            self._debug(
                f"stream exchange={stream_key[0]} outcome_id={stream_key[1]} "
                f"ok={success} err={errors} "
                f"last_latency_ms={latency if latency is not None else 'na'} "
                f"last_ok_age_s={ok_age if ok_age is not None else 'na'}"
            )

    def _evaluate_and_emit(self, *, changed_stream_key: StreamKey | None = None) -> None:
        if self.stop_event.is_set():
            return
        eval_started_at = time.time()
        now = eval_started_at
        emitted_at = datetime.fromtimestamp(now, tz=timezone.utc)
        evaluation = self._store.due_alert_events(
            now=now,
            arb_threshold=self.args.arb_threshold,
            deviation_threshold=self.args.deviation_threshold,
            stale_after_seconds=self.args.book_stale_seconds,
            cooldown_seconds=self.args.cooldown_seconds,
            changed_stream_key=changed_stream_key,
        )
        self._last_eval_mapping_count = evaluation.evaluated_mapping_count
        self._last_eval_raw_event_count = evaluation.raw_event_count
        self._last_eval_due_event_count = len(evaluation.due_events)
        if evaluation.trigger_stream_key is not None:
            self._record_latency_metrics(
                exchange=evaluation.trigger_stream_key[0],
                source_label=evaluation.trigger_quote_source,
                quote_seen_to_alert_ms=evaluation.quote_seen_to_alert_ms,
                book_timestamp_to_alert_ms=evaluation.book_timestamp_to_alert_ms,
                exchange_update_to_store_ms=evaluation.exchange_update_to_store_ms,
                store_to_alert_ms=evaluation.store_to_alert_ms,
            )
        emitted_count = 0
        for event in evaluation.due_events:
            self._print_line(self._format_alert_block(event, emitted_at=emitted_at))
            emitted_count += 1
        self._last_eval_completed_at = time.time()
        self._last_eval_duration_ms = (self._last_eval_completed_at - eval_started_at) * 1000.0
        self._last_eval_emitted_count = emitted_count
        self._quote_updates_since_last_eval = 0
        self._eval_count += 1

    def _handle_quote_update(self, stream_key: StreamKey) -> None:
        self._quote_update_count += 1
        self._quote_updates_since_last_eval += 1
        self._last_quote_update_at = time.time()
        self._evaluate_and_emit(changed_stream_key=stream_key)

    def run(self) -> int:
        try:
            self._subscription_manager.prepare_runtime()
        except NotImplementedError as exc:
            self._print_line(f"[error] {exc}")
            return 1

        self._print_line(
            "[info] starting ws arb alert runner "
            f"engine={self._engine_name} "
            f"api_base_url={self.args.api_base_url} "
            f"arb_threshold={self.args.arb_threshold} "
            f"deviation_threshold={self.args.deviation_threshold} "
            f"cooldown_seconds={self.args.cooldown_seconds} "
            f"mapping_refresh_seconds={self.args.mapping_refresh_seconds} "
            f"book_stale_seconds={self.args.book_stale_seconds} "
            f"depth={self.args.depth} "
            f"kalshi_book_mode={getattr(self.args, 'kalshi_book_mode', 'auto')}"
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

                self._flush_pending_stream_subscription_update(now)

                if self._debug_enabled and now >= next_debug_heartbeat_at:
                    self._emit_debug_heartbeat(now)
                    next_debug_heartbeat_at = now + self._debug_heartbeat_seconds

                time.sleep(0.25)
        except KeyboardInterrupt:
            self._print_line("\n[info] Ctrl+C received; stopping runner...")
            return 130
        finally:
            self.stop_event.set()
            self._subscription_manager.stop(reason="shutdown", timeout_seconds=1.5)

        return 0
