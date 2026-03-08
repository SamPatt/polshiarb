from __future__ import annotations

import time

from app.arb_alerts import (
    QuoteSnapshot,
    SemanticMapping,
    build_semantic_mappings,
    evaluate_all_mappings,
    mapping_stream_keys,
    normalize_monitoring_rows,
    passes_cooldown,
)
from app.streaming.store import QuoteStore


def _pair_details_payload() -> dict:
    return {
        "ok": True,
        "pair": {
            "pair_set": {"id": 1},
            "markets": {
                "kalshi": [
                    {
                        "market_id": "KX-1",
                        "raw_snapshot": {
                            "yes": {"outcome_id": "KX-YES"},
                            "no": {"outcome_id": "KX-NO"},
                        },
                    }
                ],
                "polymarket": [
                    {
                        "market_id": "PM-1",
                        "raw_snapshot": {
                            "yes": {"outcome_id": "PM-YES"},
                            "no": {"outcome_id": "PM-NO"},
                        },
                    }
                ],
            },
        },
    }


def _quote(exchange: str, outcome_id: str, ask: float | None, now: float) -> QuoteSnapshot:
    return QuoteSnapshot(
        exchange=exchange,
        outcome_id=outcome_id,
        best_bid=None,
        best_ask=ask,
        book_timestamp_ms=None,
        updated_at=now,
    )


def test_mapping_normalization_same_direction_builds_p_and_not_p() -> None:
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }

    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, warnings = build_semantic_mappings(rows, {1: _pair_details_payload()})
    assert warnings == []
    assert len(mappings) == 1

    mapping = mappings[0]
    assert mapping.kalshi_p == "KX-YES"
    assert mapping.kalshi_not_p == "KX-NO"
    assert mapping.polymarket_p == "PM-YES"
    assert mapping.polymarket_not_p == "PM-NO"


def test_mapping_normalization_inverse_builds_p_and_not_p() -> None:
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "inverse",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                    {
                        "relation_type": "inverse",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                ],
            }
        ],
    }

    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, warnings = build_semantic_mappings(rows, {1: _pair_details_payload()})
    assert warnings == []
    assert len(mappings) == 1

    mapping = mappings[0]
    assert mapping.kalshi_p == "KX-YES"
    assert mapping.kalshi_not_p == "KX-NO"
    assert mapping.polymarket_p == "PM-NO"
    assert mapping.polymarket_not_p == "PM-YES"


def test_edge_formulas_trigger_on_threshold_boundary() -> None:
    now = time.time()
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: _pair_details_payload()})

    quotes = {
        ("kalshi", "KX-YES"): _quote("kalshi", "KX-YES", 0.60, now),
        ("kalshi", "KX-NO"): _quote("kalshi", "KX-NO", 0.39, now),
        ("polymarket", "PM-YES"): _quote("polymarket", "PM-YES", 0.59, now),
        ("polymarket", "PM-NO"): _quote("polymarket", "PM-NO", 0.38, now),
    }
    events = evaluate_all_mappings(
        mappings,
        quotes_by_stream=quotes,
        now=now,
        arb_threshold=0.02,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
    )
    metrics = {event.metric_name for event in events}
    assert "cross_market_set_1" in metrics
    assert "cross_market_set_2" in metrics
    assert "within_polymarket" in metrics


def test_deviation_formula_triggers_on_threshold_boundary() -> None:
    now = time.time()
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: _pair_details_payload()})

    quotes = {
        ("kalshi", "KX-YES"): _quote("kalshi", "KX-YES", 0.60, now),
        ("kalshi", "KX-NO"): _quote("kalshi", "KX-NO", 0.40, now),
        ("polymarket", "PM-YES"): _quote("polymarket", "PM-YES", 0.57, now),
        ("polymarket", "PM-NO"): _quote("polymarket", "PM-NO", 0.39, now),
    }
    events = evaluate_all_mappings(
        mappings,
        quotes_by_stream=quotes,
        now=now,
        arb_threshold=0.50,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
    )
    metrics = {event.metric_name for event in events}
    assert "same_token_gap_p" in metrics
    assert "same_token_gap_not_p" not in metrics


def test_stale_quotes_suppress_alerts() -> None:
    now = time.time()
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: _pair_details_payload()})

    quotes = {
        ("kalshi", "KX-YES"): _quote("kalshi", "KX-YES", 0.60, now - 20.0),
        ("kalshi", "KX-NO"): _quote("kalshi", "KX-NO", 0.38, now),
        ("polymarket", "PM-YES"): _quote("polymarket", "PM-YES", 0.55, now),
        ("polymarket", "PM-NO"): _quote("polymarket", "PM-NO", 0.34, now),
    }
    events = evaluate_all_mappings(
        mappings,
        quotes_by_stream=quotes,
        now=now,
        arb_threshold=0.02,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
    )
    metrics = {event.metric_name for event in events}
    assert "cross_market_set_1" not in metrics
    assert "within_kalshi" not in metrics
    assert "same_token_gap_p" not in metrics


def test_quote_store_evaluates_only_pairs_affected_by_changed_stream() -> None:
    now = time.time()
    mappings = [
        SemanticMapping(
            pair_id=1,
            relation_type="same_direction",
            kalshi_market_id="KX-1",
            polymarket_market_id="PM-1",
            kalshi_p="KX1-YES",
            kalshi_not_p="KX1-NO",
            polymarket_p="PM1-YES",
            polymarket_not_p="PM1-NO",
        ),
        SemanticMapping(
            pair_id=2,
            relation_type="same_direction",
            kalshi_market_id="KX-2",
            polymarket_market_id="PM-2",
            kalshi_p="KX2-YES",
            kalshi_not_p="KX2-NO",
            polymarket_p="PM2-YES",
            polymarket_not_p="PM2-NO",
        ),
    ]
    store = QuoteStore()
    store.replace_mappings(
        mappings,
        mapping_stream_keys(mappings, canonicalize_kalshi=True),
    )

    quotes = {
        ("kalshi", "KX1-YES"): _quote("kalshi", "KX1-YES", 0.60, now),
        ("kalshi", "KX1-NO"): _quote("kalshi", "KX1-NO", 0.39, now),
        ("polymarket", "PM1-YES"): _quote("polymarket", "PM1-YES", 0.59, now),
        ("polymarket", "PM1-NO"): _quote("polymarket", "PM1-NO", 0.38, now),
        ("kalshi", "KX2-YES"): _quote("kalshi", "KX2-YES", 0.60, now),
        ("kalshi", "KX2-NO"): _quote("kalshi", "KX2-NO", 0.39, now),
        ("polymarket", "PM2-YES"): _quote("polymarket", "PM2-YES", 0.59, now),
        ("polymarket", "PM2-NO"): _quote("polymarket", "PM2-NO", 0.38, now),
    }
    for quote in quotes.values():
        store.upsert_quote(quote)

    result = store.due_alert_events(
        now=now,
        arb_threshold=0.02,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
        cooldown_seconds=0.0,
        changed_stream_key=("polymarket", "PM1-YES"),
    )

    assert result.evaluated_mapping_count == 1
    assert result.raw_event_count > 0
    assert {item.event.pair_id for item in result.due_events} == {1}


def test_passes_cooldown_blocks_repeats_until_window_expires() -> None:
    last_emitted_at: dict[str, float] = {}
    key = "test-key"
    assert passes_cooldown(
        now=100.0,
        alert_key=key,
        cooldown_seconds=30.0,
        last_emitted_at_by_key=last_emitted_at,
    )
    assert not passes_cooldown(
        now=120.0,
        alert_key=key,
        cooldown_seconds=30.0,
        last_emitted_at_by_key=last_emitted_at,
    )
    assert passes_cooldown(
        now=130.0,
        alert_key=key,
        cooldown_seconds=30.0,
        last_emitted_at_by_key=last_emitted_at,
    )


def test_missing_asks_skip_evaluation() -> None:
    now = time.time()
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: _pair_details_payload()})

    quotes = {
        ("kalshi", "KX-YES"): _quote("kalshi", "KX-YES", None, now),
        ("kalshi", "KX-NO"): _quote("kalshi", "KX-NO", None, now),
        ("polymarket", "PM-YES"): _quote("polymarket", "PM-YES", None, now),
        ("polymarket", "PM-NO"): _quote("polymarket", "PM-NO", None, now),
    }
    events = evaluate_all_mappings(
        mappings,
        quotes_by_stream=quotes,
        now=now,
        arb_threshold=0.02,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
    )
    assert events == []


def test_kalshi_stream_key_canonicalization_dedupes_no_suffix() -> None:
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    pair_payload = _pair_details_payload()
    pair_payload["pair"]["markets"]["kalshi"][0]["raw_snapshot"]["yes"]["outcome_id"] = "KX-YES"
    pair_payload["pair"]["markets"]["kalshi"][0]["raw_snapshot"]["no"]["outcome_id"] = "KX-YES-NO"
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: pair_payload})

    stream_keys = mapping_stream_keys(mappings, canonicalize_kalshi=True)
    assert ("kalshi", "KX-YES") in stream_keys
    assert ("kalshi", "KX-YES-NO") not in stream_keys
    assert ("polymarket", "PM-YES") in stream_keys
    assert ("polymarket", "PM-NO") in stream_keys


def test_kalshi_no_ask_is_derived_from_yes_bid_when_no_stream_absent() -> None:
    now = time.time()
    monitoring_payload = {
        "ok": True,
        "pairs": [
            {
                "pair_id": 1,
                "mappings": [
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-YES"},
                        ],
                    },
                    {
                        "relation_type": "same_direction",
                        "legs": [
                            {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES-NO"},
                            {"exchange": "polymarket", "market_id": "PM-1", "outcome_id": "PM-NO"},
                        ],
                    },
                ],
            }
        ],
    }
    pair_payload = _pair_details_payload()
    pair_payload["pair"]["markets"]["kalshi"][0]["raw_snapshot"]["yes"]["outcome_id"] = "KX-YES"
    pair_payload["pair"]["markets"]["kalshi"][0]["raw_snapshot"]["no"]["outcome_id"] = "KX-YES-NO"
    rows = normalize_monitoring_rows(monitoring_payload)
    mappings, _ = build_semantic_mappings(rows, {1: pair_payload})

    quotes = {
        # Only canonical Kalshi stream key is present.
        ("kalshi", "KX-YES"): QuoteSnapshot(
            exchange="kalshi",
            outcome_id="KX-YES",
            best_bid=0.60,
            best_ask=0.61,
            book_timestamp_ms=None,
            updated_at=now,
        ),
        ("polymarket", "PM-YES"): _quote("polymarket", "PM-YES", 0.55, now),
        ("polymarket", "PM-NO"): _quote("polymarket", "PM-NO", 0.38, now),
    }
    events = evaluate_all_mappings(
        mappings,
        quotes_by_stream=quotes,
        now=now,
        arb_threshold=0.02,
        deviation_threshold=0.03,
        stale_after_seconds=15.0,
    )
    metric_values = {event.metric_name: event.metric_value for event in events}
    # Derived NO ask = 1 - YES bid = 0.40 -> within_kalshi edge = 1 - (0.61 + 0.40) = -0.01
    # cross_market_set_2 edge = 1 - (0.55 + 0.40) = 0.05
    assert "cross_market_set_2" in metric_values
    assert abs(metric_values["cross_market_set_2"] - 0.05) < 1e-9
