from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from websockets.sync.client import connect


DEFAULT_POLYMARKET_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass(frozen=True)
class PolymarketQuoteUpdate:
    outcome_id: str
    best_bid: float | None
    best_ask: float | None
    book_timestamp_ms: int | None
    received_at: float
    source_latency_ms: float | None
    source_label: str


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _best_price(levels: Any, *, side: str) -> float | None:
    if not isinstance(levels, list):
        return None
    numeric_levels = [
        price
        for level in levels
        if isinstance(level, dict) and (price := _coerce_float(level.get("price"))) is not None
    ]
    if not numeric_levels:
        return None
    if side == "bid":
        return max(numeric_levels)
    return min(numeric_levels)


def _direct_ws_source_label(event_type: str) -> str:
    normalized = event_type.strip().lower().replace("-", "_")
    if not normalized:
        return "direct_ws_unknown"
    return f"direct_ws_{normalized}"


def parse_polymarket_ws_quote_updates(raw_message: str) -> list[PolymarketQuoteUpdate]:
    if raw_message in {"PING", "PONG"}:
        return []

    payload = json.loads(raw_message)
    messages = payload if isinstance(payload, list) else [payload]
    received_at = time.time()
    updates: list[PolymarketQuoteUpdate] = []

    for message in messages:
        if not isinstance(message, dict):
            continue
        event_type = str(message.get("event_type") or "")

        if event_type == "best_bid_ask":
            outcome_id = message.get("asset_id")
            if not isinstance(outcome_id, str) or not outcome_id:
                continue
            book_timestamp_ms = _coerce_int(message.get("timestamp"))
            source_latency_ms = None
            if book_timestamp_ms is not None:
                source_latency_ms = (received_at * 1000.0) - float(book_timestamp_ms)
            updates.append(
                PolymarketQuoteUpdate(
                    outcome_id=outcome_id,
                    best_bid=_coerce_float(message.get("best_bid")),
                    best_ask=_coerce_float(message.get("best_ask")),
                    book_timestamp_ms=book_timestamp_ms,
                    received_at=received_at,
                    source_latency_ms=source_latency_ms,
                    source_label=_direct_ws_source_label(event_type),
                )
            )
            continue

        if event_type == "book":
            outcome_id = message.get("asset_id")
            if not isinstance(outcome_id, str) or not outcome_id:
                continue
            book_timestamp_ms = _coerce_int(message.get("timestamp"))
            source_latency_ms = None
            if book_timestamp_ms is not None:
                source_latency_ms = (received_at * 1000.0) - float(book_timestamp_ms)
            updates.append(
                PolymarketQuoteUpdate(
                    outcome_id=outcome_id,
                    best_bid=_best_price(message.get("bids") or message.get("buys"), side="bid"),
                    best_ask=_best_price(message.get("asks") or message.get("sells"), side="ask"),
                    book_timestamp_ms=book_timestamp_ms,
                    received_at=received_at,
                    source_latency_ms=source_latency_ms,
                    source_label=_direct_ws_source_label(event_type),
                )
            )
            continue

        if event_type == "price_change":
            book_timestamp_ms = _coerce_int(message.get("timestamp"))
            for price_change in message.get("price_changes") or []:
                if not isinstance(price_change, dict):
                    continue
                outcome_id = price_change.get("asset_id")
                if not isinstance(outcome_id, str) or not outcome_id:
                    continue
                source_latency_ms = None
                if book_timestamp_ms is not None:
                    source_latency_ms = (received_at * 1000.0) - float(book_timestamp_ms)
                updates.append(
                    PolymarketQuoteUpdate(
                        outcome_id=outcome_id,
                        best_bid=_coerce_float(price_change.get("best_bid")),
                        best_ask=_coerce_float(price_change.get("best_ask")),
                        book_timestamp_ms=book_timestamp_ms,
                        received_at=received_at,
                        source_latency_ms=source_latency_ms,
                        source_label=_direct_ws_source_label(event_type),
                    )
                )
    return updates


class PolymarketMarketWebSocketClient:
    def __init__(
        self,
        *,
        url: str = DEFAULT_POLYMARKET_MARKET_WS_URL,
        open_timeout_seconds: float = 10.0,
        ping_interval_seconds: float = 5.0,
        receive_timeout_seconds: float = 1.0,
    ) -> None:
        self._url = url
        self._open_timeout_seconds = max(0.1, open_timeout_seconds)
        self._ping_interval_seconds = max(1.0, ping_interval_seconds)
        self._receive_timeout_seconds = max(0.1, receive_timeout_seconds)
        self._connection: Any | None = None
        self._subscribed_outcome_ids: set[str] = set()
        self._last_ping_at = 0.0

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        self._subscribed_outcome_ids = set()
        self._last_ping_at = 0.0
        if connection is not None:
            connection.close()

    def sync_subscriptions(self, outcome_ids: set[str]) -> tuple[set[str], set[str]]:
        desired_outcome_ids = {outcome_id for outcome_id in outcome_ids if outcome_id}
        if not desired_outcome_ids:
            self.close()
            return set(), set()

        if self._connection is None:
            self._connection = connect(
                self._url,
                open_timeout=self._open_timeout_seconds,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=1.0,
            )
            self._send_subscription(
                assets_ids=sorted(desired_outcome_ids),
                operation=None,
            )
            self._subscribed_outcome_ids = set(desired_outcome_ids)
            self._last_ping_at = time.monotonic()
            return set(desired_outcome_ids), set()

        add_outcome_ids = desired_outcome_ids - self._subscribed_outcome_ids
        remove_outcome_ids = self._subscribed_outcome_ids - desired_outcome_ids
        if remove_outcome_ids:
            self._send_subscription(
                assets_ids=sorted(remove_outcome_ids),
                operation="unsubscribe",
            )
        if add_outcome_ids:
            self._send_subscription(
                assets_ids=sorted(add_outcome_ids),
                operation="subscribe",
            )
        self._subscribed_outcome_ids = set(desired_outcome_ids)
        return add_outcome_ids, remove_outcome_ids

    def recv_quote_updates(self) -> list[PolymarketQuoteUpdate]:
        if self._connection is None:
            return []

        now = time.monotonic()
        if (now - self._last_ping_at) >= self._ping_interval_seconds:
            self._connection.send("PING")
            self._last_ping_at = now

        raw_message = self._connection.recv(timeout=self._receive_timeout_seconds)
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        if not isinstance(raw_message, str):
            return []
        if raw_message == "PONG":
            return []
        return parse_polymarket_ws_quote_updates(raw_message)

    def _send_subscription(
        self,
        *,
        assets_ids: list[str],
        operation: str | None,
    ) -> None:
        if self._connection is None or not assets_ids:
            return
        payload: dict[str, Any] = {
            "type": "market",
            "assets_ids": assets_ids,
            "custom_feature_enabled": True,
        }
        if operation is not None:
            payload["operation"] = operation
        self._connection.send(json.dumps(payload))
