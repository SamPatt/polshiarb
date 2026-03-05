from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class OutcomeRef:
    exchange: str
    market_id: str
    outcome_id: str


@dataclass(frozen=True)
class MappingRow:
    pair_id: int
    relation_type: str
    kalshi: OutcomeRef
    polymarket: OutcomeRef


@dataclass(frozen=True)
class SemanticMapping:
    pair_id: int
    relation_type: str
    kalshi_market_id: str
    polymarket_market_id: str
    kalshi_p: str
    kalshi_not_p: str
    polymarket_p: str
    polymarket_not_p: str


@dataclass(frozen=True)
class QuoteSnapshot:
    exchange: str
    outcome_id: str
    best_bid: float | None
    best_ask: float | None
    book_timestamp_ms: int | None
    updated_at: float


@dataclass(frozen=True)
class AlertEvent:
    key: str
    tag: str
    pair_id: int
    kalshi_market_id: str
    polymarket_market_id: str
    relation_type: str
    metric_name: str
    metric_value: float
    threshold: float
    details: dict[str, str]


def level_price(level: Any) -> float | None:
    if isinstance(level, dict):
        value = level.get("price")
    else:
        value = getattr(level, "price", None)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def level_size(level: Any) -> float | None:
    if isinstance(level, dict):
        value = level.get("size")
    else:
        value = getattr(level, "size", None)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def book_levels(book: Any, side: str) -> list[Any]:
    if isinstance(book, dict):
        raw = book.get(side)
    else:
        raw = getattr(book, side, None)
    if isinstance(raw, list):
        return raw
    return []


def best_level(levels: list[Any], side: str) -> tuple[float | None, float | None]:
    priced_levels: list[tuple[float, float | None]] = []
    for level in levels:
        price = level_price(level)
        if price is None:
            continue
        priced_levels.append((price, level_size(level)))

    if not priced_levels:
        return None, None

    if side == "bid":
        return max(priced_levels, key=lambda item: item[0])
    return min(priced_levels, key=lambda item: item[0])


def extract_best_prices(book: Any) -> tuple[float | None, float | None]:
    bids = book_levels(book, "bids")
    asks = book_levels(book, "asks")
    best_bid, _ = best_level(bids, "bid")
    best_ask, _ = best_level(asks, "ask")
    return best_bid, best_ask


def extract_book_timestamp_ms(book: Any) -> int | None:
    if isinstance(book, dict):
        raw = book.get("timestamp")
    else:
        raw = getattr(book, "timestamp", None)
    if isinstance(raw, (int, float)):
        return int(raw)
    return None


def format_timestamp_ms(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return "unknown"
    dt = datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc)
    return dt.isoformat()


def normalize_monitoring_rows(monitoring_pairs_payload: dict[str, Any]) -> list[MappingRow]:
    pairs = monitoring_pairs_payload.get("pairs")
    if not isinstance(pairs, list):
        return []

    rows: list[MappingRow] = []
    for pair in pairs:
        pair_id = pair.get("pair_id")
        mappings = pair.get("mappings")
        if not isinstance(pair_id, int) or not isinstance(mappings, list):
            continue

        for mapping in mappings:
            relation_type = str(mapping.get("relation_type") or "same_direction")
            legs = mapping.get("legs")
            if not isinstance(legs, list):
                continue

            kalshi_leg: dict[str, Any] | None = None
            polymarket_leg: dict[str, Any] | None = None
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                exchange = leg.get("exchange")
                if exchange == "kalshi":
                    kalshi_leg = leg
                elif exchange == "polymarket":
                    polymarket_leg = leg

            if kalshi_leg is None or polymarket_leg is None:
                continue

            k_market_id = kalshi_leg.get("market_id")
            k_outcome_id = kalshi_leg.get("outcome_id")
            p_market_id = polymarket_leg.get("market_id")
            p_outcome_id = polymarket_leg.get("outcome_id")
            if not all(
                isinstance(v, str) and v
                for v in (k_market_id, k_outcome_id, p_market_id, p_outcome_id)
            ):
                continue

            rows.append(
                MappingRow(
                    pair_id=pair_id,
                    relation_type=relation_type,
                    kalshi=OutcomeRef(
                        exchange="kalshi",
                        market_id=k_market_id,
                        outcome_id=k_outcome_id,
                    ),
                    polymarket=OutcomeRef(
                        exchange="polymarket",
                        market_id=p_market_id,
                        outcome_id=p_outcome_id,
                    ),
                )
            )

    return rows


def _market_side_lookup(
    pair_details_payload: dict[str, Any],
) -> dict[tuple[str, str, str], str]:
    pair_obj = pair_details_payload.get("pair")
    if not isinstance(pair_obj, dict):
        return {}

    markets = pair_obj.get("markets")
    if not isinstance(markets, dict):
        return {}

    side_map: dict[tuple[str, str, str], str] = {}

    for exchange in ("kalshi", "polymarket"):
        exchange_markets = markets.get(exchange)
        if not isinstance(exchange_markets, list):
            continue

        for market in exchange_markets:
            if not isinstance(market, dict):
                continue
            market_id = market.get("market_id")
            raw_snapshot = market.get("raw_snapshot")
            source = raw_snapshot if isinstance(raw_snapshot, dict) else market
            if not isinstance(market_id, str) or not market_id:
                continue

            for side in ("yes", "no"):
                outcome_obj = source.get(side)
                if not isinstance(outcome_obj, dict):
                    continue
                outcome_id = outcome_obj.get("outcome_id")
                if isinstance(outcome_id, str) and outcome_id:
                    side_map[(exchange, market_id, outcome_id)] = side

    return side_map


def build_semantic_mappings(
    monitoring_rows: list[MappingRow],
    pair_details_by_id: dict[int, dict[str, Any]],
) -> tuple[list[SemanticMapping], list[str]]:
    warnings: list[str] = []
    grouped: dict[tuple[int, str, str], dict[str, Any]] = {}

    side_lookup_by_pair: dict[int, dict[tuple[str, str, str], str]] = {}
    for pair_id, payload in pair_details_by_id.items():
        side_lookup_by_pair[pair_id] = _market_side_lookup(payload)

    for row in monitoring_rows:
        side_lookup = side_lookup_by_pair.get(row.pair_id, {})
        k_side = side_lookup.get(("kalshi", row.kalshi.market_id, row.kalshi.outcome_id))
        p_side = side_lookup.get(
            ("polymarket", row.polymarket.market_id, row.polymarket.outcome_id)
        )
        if k_side not in {"yes", "no"} or p_side not in {"yes", "no"}:
            warnings.append(
                "Skipping mapping with unknown side "
                f"pair_id={row.pair_id} relation_type={row.relation_type} "
                f"kalshi=({row.kalshi.market_id},{row.kalshi.outcome_id},{k_side}) "
                f"polymarket=({row.polymarket.market_id},{row.polymarket.outcome_id},{p_side})"
            )
            continue

        if row.relation_type == "same_direction" and p_side != k_side:
            warnings.append(
                "Skipping mapping with invalid same_direction side pairing "
                f"pair_id={row.pair_id} kalshi_side={k_side} polymarket_side={p_side}"
            )
            continue

        if row.relation_type == "inverse" and p_side == k_side:
            warnings.append(
                "Skipping mapping with invalid inverse side pairing "
                f"pair_id={row.pair_id} kalshi_side={k_side} polymarket_side={p_side}"
            )
            continue

        group_key = (row.pair_id, row.kalshi.market_id, row.polymarket.market_id)
        group = grouped.setdefault(
            group_key,
            {
                "pair_id": row.pair_id,
                "relation_type": row.relation_type,
                "kalshi_market_id": row.kalshi.market_id,
                "polymarket_market_id": row.polymarket.market_id,
                "kalshi_p": None,
                "kalshi_not_p": None,
                "polymarket_p": None,
                "polymarket_not_p": None,
            },
        )

        if group["relation_type"] != row.relation_type:
            warnings.append(
                "Skipping conflicting relation_type rows for same market pair "
                f"pair_id={row.pair_id} kalshi_market_id={row.kalshi.market_id} "
                f"polymarket_market_id={row.polymarket.market_id}"
            )
            continue

        semantic_side = "p" if k_side == "yes" else "not_p"
        if semantic_side == "p":
            group["kalshi_p"] = row.kalshi.outcome_id
            group["polymarket_p"] = row.polymarket.outcome_id
        else:
            group["kalshi_not_p"] = row.kalshi.outcome_id
            group["polymarket_not_p"] = row.polymarket.outcome_id

    mappings: list[SemanticMapping] = []
    for group in grouped.values():
        required = (
            group["kalshi_p"],
            group["kalshi_not_p"],
            group["polymarket_p"],
            group["polymarket_not_p"],
        )
        if not all(isinstance(item, str) and item for item in required):
            warnings.append(
                "Skipping incomplete semantic mapping "
                f"pair_id={group['pair_id']} kalshi_market_id={group['kalshi_market_id']} "
                f"polymarket_market_id={group['polymarket_market_id']}"
            )
            continue

        mappings.append(
            SemanticMapping(
                pair_id=group["pair_id"],
                relation_type=group["relation_type"],
                kalshi_market_id=group["kalshi_market_id"],
                polymarket_market_id=group["polymarket_market_id"],
                kalshi_p=group["kalshi_p"],
                kalshi_not_p=group["kalshi_not_p"],
                polymarket_p=group["polymarket_p"],
                polymarket_not_p=group["polymarket_not_p"],
            )
        )

    mappings.sort(
        key=lambda item: (
            item.pair_id,
            item.kalshi_market_id,
            item.polymarket_market_id,
        )
    )
    return mappings, warnings


def outcome_stream_keys(mapping: SemanticMapping) -> set[tuple[str, str]]:
    return {
        ("kalshi", mapping.kalshi_p),
        ("kalshi", mapping.kalshi_not_p),
        ("polymarket", mapping.polymarket_p),
        ("polymarket", mapping.polymarket_not_p),
    }


def mapping_stream_keys(mappings: list[SemanticMapping]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for mapping in mappings:
        keys.update(outcome_stream_keys(mapping))
    return keys


def _quote_is_fresh(
    quote: QuoteSnapshot | None,
    *,
    now: float,
    stale_after_seconds: float,
) -> bool:
    if quote is None:
        return False
    return (now - quote.updated_at) <= stale_after_seconds


def _quote_ask(
    quotes_by_stream: dict[tuple[str, str], QuoteSnapshot],
    exchange: str,
    outcome_id: str,
    *,
    now: float,
    stale_after_seconds: float,
) -> float | None:
    quote = quotes_by_stream.get((exchange, outcome_id))
    if not _quote_is_fresh(quote, now=now, stale_after_seconds=stale_after_seconds):
        return None
    return quote.best_ask


def evaluate_mapping(
    mapping: SemanticMapping,
    *,
    quotes_by_stream: dict[tuple[str, str], QuoteSnapshot],
    now: float,
    arb_threshold: float,
    deviation_threshold: float,
    stale_after_seconds: float,
) -> list[AlertEvent]:
    k_p = _quote_ask(
        quotes_by_stream,
        "kalshi",
        mapping.kalshi_p,
        now=now,
        stale_after_seconds=stale_after_seconds,
    )
    k_not_p = _quote_ask(
        quotes_by_stream,
        "kalshi",
        mapping.kalshi_not_p,
        now=now,
        stale_after_seconds=stale_after_seconds,
    )
    pm_p = _quote_ask(
        quotes_by_stream,
        "polymarket",
        mapping.polymarket_p,
        now=now,
        stale_after_seconds=stale_after_seconds,
    )
    pm_not_p = _quote_ask(
        quotes_by_stream,
        "polymarket",
        mapping.polymarket_not_p,
        now=now,
        stale_after_seconds=stale_after_seconds,
    )

    events: list[AlertEvent] = []

    if k_p is not None and pm_not_p is not None:
        edge = 1.0 - (k_p + pm_not_p)
        if edge >= arb_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "arb_cross_1"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_ARB_CROSS",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="cross_market_set_1",
                    metric_value=edge,
                    threshold=arb_threshold,
                    details={
                        "kalshi_p_ask": f"{k_p:.4f}",
                        "polymarket_not_p_ask": f"{pm_not_p:.4f}",
                    },
                )
            )

    if pm_p is not None and k_not_p is not None:
        edge = 1.0 - (pm_p + k_not_p)
        if edge >= arb_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "arb_cross_2"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_ARB_CROSS",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="cross_market_set_2",
                    metric_value=edge,
                    threshold=arb_threshold,
                    details={
                        "polymarket_p_ask": f"{pm_p:.4f}",
                        "kalshi_not_p_ask": f"{k_not_p:.4f}",
                    },
                )
            )

    if k_p is not None and k_not_p is not None:
        edge = 1.0 - (k_p + k_not_p)
        if edge >= arb_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "arb_within_kalshi"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_ARB_WITHIN",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="within_kalshi",
                    metric_value=edge,
                    threshold=arb_threshold,
                    details={
                        "kalshi_p_ask": f"{k_p:.4f}",
                        "kalshi_not_p_ask": f"{k_not_p:.4f}",
                    },
                )
            )

    if pm_p is not None and pm_not_p is not None:
        edge = 1.0 - (pm_p + pm_not_p)
        if edge >= arb_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "arb_within_polymarket"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_ARB_WITHIN",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="within_polymarket",
                    metric_value=edge,
                    threshold=arb_threshold,
                    details={
                        "polymarket_p_ask": f"{pm_p:.4f}",
                        "polymarket_not_p_ask": f"{pm_not_p:.4f}",
                    },
                )
            )

    if k_p is not None and pm_p is not None:
        gap = abs(k_p - pm_p)
        if gap >= deviation_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "deviation_p"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_DEVIATION",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="same_token_gap_p",
                    metric_value=gap,
                    threshold=deviation_threshold,
                    details={
                        "kalshi_p_ask": f"{k_p:.4f}",
                        "polymarket_p_ask": f"{pm_p:.4f}",
                    },
                )
            )

    if k_not_p is not None and pm_not_p is not None:
        gap = abs(k_not_p - pm_not_p)
        if gap >= deviation_threshold:
            events.append(
                AlertEvent(
                    key=(
                        "deviation_not_p"
                        f"|pair={mapping.pair_id}"
                        f"|k={mapping.kalshi_market_id}"
                        f"|pm={mapping.polymarket_market_id}"
                    ),
                    tag="ALERT_DEVIATION",
                    pair_id=mapping.pair_id,
                    kalshi_market_id=mapping.kalshi_market_id,
                    polymarket_market_id=mapping.polymarket_market_id,
                    relation_type=mapping.relation_type,
                    metric_name="same_token_gap_not_p",
                    metric_value=gap,
                    threshold=deviation_threshold,
                    details={
                        "kalshi_not_p_ask": f"{k_not_p:.4f}",
                        "polymarket_not_p_ask": f"{pm_not_p:.4f}",
                    },
                )
            )

    return events


def evaluate_all_mappings(
    mappings: list[SemanticMapping],
    *,
    quotes_by_stream: dict[tuple[str, str], QuoteSnapshot],
    now: float,
    arb_threshold: float,
    deviation_threshold: float,
    stale_after_seconds: float,
) -> list[AlertEvent]:
    events: list[AlertEvent] = []
    for mapping in mappings:
        events.extend(
            evaluate_mapping(
                mapping,
                quotes_by_stream=quotes_by_stream,
                now=now,
                arb_threshold=arb_threshold,
                deviation_threshold=deviation_threshold,
                stale_after_seconds=stale_after_seconds,
            )
        )
    return events


def passes_cooldown(
    *,
    now: float,
    alert_key: str,
    cooldown_seconds: float,
    last_emitted_at_by_key: dict[str, float],
) -> bool:
    if cooldown_seconds <= 0:
        last_emitted_at_by_key[alert_key] = now
        return True

    previous = last_emitted_at_by_key.get(alert_key)
    if previous is None or (now - previous) >= cooldown_seconds:
        last_emitted_at_by_key[alert_key] = now
        return True
    return False


def format_alert_line(event: AlertEvent, *, emitted_at: datetime | None = None) -> str:
    ts = emitted_at or datetime.now(timezone.utc)
    detail_blob = " ".join(f"{key}={value}" for key, value in sorted(event.details.items()))
    return (
        f"[{event.tag}] "
        f"ts={ts.isoformat()} "
        f"pair_id={event.pair_id} "
        f"kalshi_market_id={event.kalshi_market_id} "
        f"polymarket_market_id={event.polymarket_market_id} "
        f"relation_type={event.relation_type} "
        f"metric={event.metric_name} "
        f"value={event.metric_value:.4f} "
        f"threshold={event.threshold:.4f} "
        f"{detail_blob}"
    )
