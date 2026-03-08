from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Any

from app.arb_alerts import (
    AlertEvent,
    QuoteSnapshot,
    SemanticMapping,
    canonical_stream_key,
    evaluate_all_mappings,
    evaluate_mapping,
    mapping_stream_keys,
    passes_cooldown,
)

from .types import StreamKey


@dataclass(frozen=True)
class EvaluatedAlertEvent:
    event: AlertEvent
    trigger_stream_key: StreamKey | None
    trigger_quote_source: str | None
    quote_seen_to_alert_ms: float | None
    book_timestamp_to_alert_ms: float | None
    exchange_update_to_store_ms: float | None
    store_to_alert_ms: float | None


@dataclass(frozen=True)
class QuoteEvaluationResult:
    trigger_stream_key: StreamKey | None
    trigger_quote_source: str | None
    evaluated_mapping_count: int
    raw_event_count: int
    due_events: list[EvaluatedAlertEvent]
    quote_seen_to_alert_ms: float | None
    book_timestamp_to_alert_ms: float | None
    exchange_update_to_store_ms: float | None
    store_to_alert_ms: float | None


class QuoteStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mappings: list[SemanticMapping] = []
        self._mapping_indexes_by_stream_key: dict[StreamKey, set[int]] = {}
        self._quotes: dict[StreamKey, QuoteSnapshot] = {}
        self._active_stream_keys: set[StreamKey] = set()
        self._last_emitted_at: dict[str, float] = {}

    def replace_mappings(
        self,
        mappings: list[SemanticMapping],
        stream_keys: set[StreamKey],
    ) -> tuple[set[StreamKey], set[StreamKey]]:
        with self._lock:
            previous_stream_keys = set(self._active_stream_keys)
            self._mappings = list(mappings)
            self._mapping_indexes_by_stream_key = self._build_mapping_index(mappings)
            self._active_stream_keys = set(stream_keys)
            return previous_stream_keys, set(self._active_stream_keys)

    def upsert_quote(self, quote: QuoteSnapshot) -> None:
        with self._lock:
            self._quotes[(quote.exchange, quote.outcome_id)] = quote

    def deactivate_streams(self, stream_keys: set[StreamKey]) -> None:
        with self._lock:
            self._active_stream_keys.difference_update(stream_keys)
            for stream_key in stream_keys:
                self._quotes.pop(stream_key, None)

    def quote_count(self) -> int:
        with self._lock:
            return len(self._quotes)

    def stale_quote_count(
        self,
        *,
        now: float,
        stale_after_seconds: float,
    ) -> int:
        with self._lock:
            stale_count = 0
            for stream_key in self._active_stream_keys:
                quote = self._quotes.get(stream_key)
                if quote is None or (now - quote.updated_at) > stale_after_seconds:
                    stale_count += 1
            return stale_count

    def due_alert_events(
        self,
        *,
        now: float,
        arb_threshold: float,
        deviation_threshold: float,
        stale_after_seconds: float,
        cooldown_seconds: float,
        changed_stream_key: StreamKey | None = None,
    ) -> QuoteEvaluationResult:
        with self._lock:
            if changed_stream_key is not None:
                changed_stream_key = canonical_stream_key(
                    changed_stream_key[0],
                    changed_stream_key[1],
                    canonicalize_kalshi=True,
                )
            mappings = self._mappings
            if changed_stream_key is not None:
                mapping_indexes = self._mapping_indexes_by_stream_key.get(changed_stream_key, set())
                mappings = [self._mappings[idx] for idx in sorted(mapping_indexes)]

            if not mappings:
                return QuoteEvaluationResult(
                    trigger_stream_key=changed_stream_key,
                    trigger_quote_source=None,
                    evaluated_mapping_count=0,
                    raw_event_count=0,
                    due_events=[],
                    quote_seen_to_alert_ms=None,
                    book_timestamp_to_alert_ms=None,
                    exchange_update_to_store_ms=None,
                    store_to_alert_ms=None,
                )

            if changed_stream_key is None:
                events = evaluate_all_mappings(
                    mappings,
                    quotes_by_stream=self._quotes,
                    now=now,
                    arb_threshold=arb_threshold,
                    deviation_threshold=deviation_threshold,
                    stale_after_seconds=stale_after_seconds,
                )
            else:
                events = []
                for mapping in mappings:
                    events.extend(
                        evaluate_mapping(
                            mapping,
                            quotes_by_stream=self._quotes,
                            now=now,
                            arb_threshold=arb_threshold,
                            deviation_threshold=deviation_threshold,
                            stale_after_seconds=stale_after_seconds,
                        )
                    )

            trigger_quote = self._quotes.get(changed_stream_key) if changed_stream_key else None
            trigger_quote_source = trigger_quote.source_label if trigger_quote is not None else None
            quote_seen_at = None
            if trigger_quote is not None:
                quote_seen_at = trigger_quote.quote_seen_at
                if quote_seen_at is None:
                    quote_seen_at = trigger_quote.updated_at
            quote_seen_to_alert_ms = None
            if quote_seen_at is not None:
                quote_seen_to_alert_ms = (now - quote_seen_at) * 1000.0
            store_to_alert_ms = None
            book_timestamp_to_alert_ms = None
            exchange_update_to_store_ms = None
            if trigger_quote is not None:
                store_to_alert_ms = (now - trigger_quote.updated_at) * 1000.0
                if trigger_quote.book_timestamp_ms is not None:
                    book_timestamp_to_alert_ms = (now * 1000.0) - float(
                        trigger_quote.book_timestamp_ms
                    )
                    exchange_update_to_store_ms = (trigger_quote.updated_at * 1000.0) - float(
                        trigger_quote.book_timestamp_ms
                    )

            due_events: list[EvaluatedAlertEvent] = []
            for event in events:
                if not passes_cooldown(
                    now=now,
                    alert_key=event.key,
                    cooldown_seconds=cooldown_seconds,
                    last_emitted_at_by_key=self._last_emitted_at,
                ):
                    continue
                due_events.append(
                    EvaluatedAlertEvent(
                        event=event,
                        trigger_stream_key=changed_stream_key,
                        trigger_quote_source=trigger_quote_source,
                        quote_seen_to_alert_ms=quote_seen_to_alert_ms,
                        book_timestamp_to_alert_ms=book_timestamp_to_alert_ms,
                        exchange_update_to_store_ms=exchange_update_to_store_ms,
                        store_to_alert_ms=store_to_alert_ms,
                    )
                )

            return QuoteEvaluationResult(
                trigger_stream_key=changed_stream_key,
                trigger_quote_source=trigger_quote_source,
                evaluated_mapping_count=len(mappings),
                raw_event_count=len(events),
                due_events=due_events,
                quote_seen_to_alert_ms=quote_seen_to_alert_ms,
                book_timestamp_to_alert_ms=book_timestamp_to_alert_ms,
                exchange_update_to_store_ms=exchange_update_to_store_ms,
                store_to_alert_ms=store_to_alert_ms,
            )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "mappings": list(self._mappings),
                "quotes": dict(self._quotes),
                "active_streams": sorted(self._active_stream_keys),
                "last_emitted_at": dict(self._last_emitted_at),
            }

    def _build_mapping_index(
        self,
        mappings: list[SemanticMapping],
    ) -> dict[StreamKey, set[int]]:
        mapping_indexes_by_stream_key: dict[StreamKey, set[int]] = {}
        for idx, mapping in enumerate(mappings):
            for stream_key in mapping_stream_keys([mapping], canonicalize_kalshi=True):
                normalized_stream_key = canonical_stream_key(
                    stream_key[0],
                    stream_key[1],
                    canonicalize_kalshi=True,
                )
                mapping_indexes_by_stream_key.setdefault(normalized_stream_key, set()).add(idx)
        return mapping_indexes_by_stream_key
