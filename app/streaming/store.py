from __future__ import annotations

import threading
from typing import Any

from app.arb_alerts import (
    AlertEvent,
    QuoteSnapshot,
    SemanticMapping,
    evaluate_all_mappings,
    passes_cooldown,
)

from .types import StreamKey


class QuoteStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mappings: list[SemanticMapping] = []
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
            self._active_stream_keys = set(stream_keys)
            return previous_stream_keys, set(self._active_stream_keys)

    def upsert_quote(self, quote: QuoteSnapshot) -> None:
        with self._lock:
            self._quotes[(quote.exchange, quote.outcome_id)] = quote

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
    ) -> list[AlertEvent]:
        with self._lock:
            events = evaluate_all_mappings(
                self._mappings,
                quotes_by_stream=self._quotes,
                now=now,
                arb_threshold=arb_threshold,
                deviation_threshold=deviation_threshold,
                stale_after_seconds=stale_after_seconds,
            )

            due_events: list[AlertEvent] = []
            for event in events:
                if not passes_cooldown(
                    now=now,
                    alert_key=event.key,
                    cooldown_seconds=cooldown_seconds,
                    last_emitted_at_by_key=self._last_emitted_at,
                ):
                    continue
                due_events.append(event)
            return due_events

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "mappings": list(self._mappings),
                "quotes": dict(self._quotes),
                "active_streams": sorted(self._active_stream_keys),
                "last_emitted_at": dict(self._last_emitted_at),
            }
