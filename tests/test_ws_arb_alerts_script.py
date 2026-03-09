from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import threading
import time
from types import ModuleType

import pytest

from app.arb_alerts import QuoteSnapshot, SemanticMapping
from app.streaming.manager import (
    MultiplexExchangeIngestor,
    MultiplexSubscriptionManager,
    QueuedQuoteUpdate,
)
from app.streaming.polymarket_ws import parse_polymarket_ws_quote_updates
from app.streaming.store import QuoteStore


def _load_script_module() -> ModuleType:
    return _load_named_script_module("ws_arb_alerts")


def _load_named_script_module(stem: str) -> ModuleType:
    script_path = Path(__file__).resolve().parent.parent / "scripts" / f"{stem}.py"
    spec = importlib.util.spec_from_file_location("ws_arb_alerts_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("engine", ["legacy", "multiplex"])
def test_script_emits_cross_within_and_deviation_alert_tags(monkeypatch, engine: str) -> None:
    module = _load_script_module()

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
    pair_details_payload = {
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

    def fake_http_get_json(base_url, path, **kwargs):  # noqa: ANN001
        if path == "/api/monitoring/pairs":
            return monitoring_payload
        if path == "/api/pairs/1":
            return pair_details_payload
        raise AssertionError(f"Unexpected path: {path}")

    ask_by_outcome = {
        "KX-YES": 0.60,
        "KX-NO": 0.38,
        "PM-YES": 0.55,
        "PM-NO": 0.34,
    }

    class FakeExchange:
        def watch_order_book(self, outcome_id, limit=None):  # noqa: ANN001
            ask = ask_by_outcome[outcome_id]
            time.sleep(0.02)
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": max(0.0, ask - 0.01), "size": 10.0}],
                "asks": [{"price": ask, "size": 10.0}],
            }

        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            ask = ask_by_outcome[outcome_id]
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": max(0.0, ask - 0.01), "size": 10.0}],
                "asks": [{"price": ask, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(module, "_http_get_json", fake_http_get_json)
    monkeypatch.setattr(
        module,
        "_build_exchange",
        lambda exchange, **kwargs: FakeExchange(),
    )

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine=engine,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    runner._subscription_manager._print_line = runner._print_line  # type: ignore[attr-defined]

    run_thread = threading.Thread(target=runner.run, daemon=True)
    run_thread.start()

    deadline = time.time() + 3.0
    while time.time() < deadline:
        if (
            any(line.startswith("[ALERT_ARB_CROSS]") for line in lines)
            and any(line.startswith("[ALERT_ARB_WITHIN]") for line in lines)
            and any(line.startswith("[ALERT_DEVIATION]") for line in lines)
        ):
            break
        time.sleep(0.05)

    runner.stop_event.set()
    run_thread.join(timeout=3.0)

    assert any(line.startswith("[ALERT_ARB_CROSS]") for line in lines)
    assert any(line.startswith("[ALERT_ARB_WITHIN]") for line in lines)
    assert any(line.startswith("[ALERT_DEVIATION]") for line in lines)
    if engine == "multiplex":
        assert len(runner._subscription_manager._exchange_ingestors) == 2  # type: ignore[attr-defined]


def test_multiplex_refresh_reuses_exchange_ingestors(monkeypatch) -> None:
    module = _load_script_module()

    monitoring_payloads = [
        {
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
        },
        {
            "ok": True,
            "pairs": [
                {
                    "pair_id": 2,
                    "mappings": [
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                                {"exchange": "polymarket", "market_id": "PM-2", "outcome_id": "PM2-YES"},
                            ],
                        },
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                                {"exchange": "polymarket", "market_id": "PM-2", "outcome_id": "PM2-NO"},
                            ],
                        },
                    ],
                }
            ],
        },
    ]
    pair_details_payloads = {
        "/api/pairs/1": {
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
        },
        "/api/pairs/2": {
            "ok": True,
            "pair": {
                "pair_set": {"id": 2},
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
                            "market_id": "PM-2",
                            "raw_snapshot": {
                                "yes": {"outcome_id": "PM2-YES"},
                                "no": {"outcome_id": "PM2-NO"},
                            },
                        }
                    ],
                },
            },
        },
    }
    monitoring_index = 0

    def fake_http_get_json(base_url, path, **kwargs):  # noqa: ANN001
        nonlocal monitoring_index
        if path == "/api/monitoring/pairs":
            payload = monitoring_payloads[min(monitoring_index, len(monitoring_payloads) - 1)]
            monitoring_index += 1
            return payload
        if path in pair_details_payloads:
            return pair_details_payloads[path]
        raise AssertionError(f"Unexpected path: {path}")

    ask_by_outcome = {
        "KX-YES": 0.60,
        "KX-NO": 0.38,
        "PM-YES": 0.55,
        "PM-NO": 0.34,
        "PM2-YES": 0.57,
        "PM2-NO": 0.36,
    }

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            ask = ask_by_outcome.get(outcome_id, 0.50)
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": max(0.0, ask - 0.01), "size": 10.0}],
                "asks": [{"price": ask, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(module, "_http_get_json", fake_http_get_json)
    monkeypatch.setattr(
        module,
        "_build_exchange",
        lambda exchange, **kwargs: FakeExchange(),
    )

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        mapping_subscription_debounce_seconds=0.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    runner._subscription_manager._print_line = runner._print_line  # type: ignore[attr-defined]

    manager = runner._subscription_manager
    try:
        runner._refresh_mappings(reason="startup")
        time.sleep(0.05)

        kalshi_ingestor = manager._exchange_ingestors["kalshi"]  # type: ignore[attr-defined]
        polymarket_ingestor = manager._exchange_ingestors["polymarket"]  # type: ignore[attr-defined]
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM-YES"),
            ("polymarket", "PM-NO"),
        }

        runner._refresh_mappings(reason="periodic")
        time.sleep(0.05)

        assert manager._exchange_ingestors["kalshi"] is kalshi_ingestor  # type: ignore[attr-defined]
        assert manager._exchange_ingestors["polymarket"] is polymarket_ingestor  # type: ignore[attr-defined]
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM2-YES"),
            ("polymarket", "PM2-NO"),
        }
        assert any(
            "multiplex subscriptions updated exchange=polymarket add=2 remove=2 keep=0 total=2"
            in line
            for line in lines
        )
    finally:
        runner.stop_event.set()
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_mapping_refresh_debounce_coalesces_periodic_updates(monkeypatch) -> None:
    module = _load_script_module()

    monitoring_payloads = [
        {
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
        },
        {
            "ok": True,
            "pairs": [
                {
                    "pair_id": 2,
                    "mappings": [
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                                {"exchange": "polymarket", "market_id": "PM-2", "outcome_id": "PM2-YES"},
                            ],
                        },
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                                {"exchange": "polymarket", "market_id": "PM-2", "outcome_id": "PM2-NO"},
                            ],
                        },
                    ],
                }
            ],
        },
        {
            "ok": True,
            "pairs": [
                {
                    "pair_id": 3,
                    "mappings": [
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-YES"},
                                {"exchange": "polymarket", "market_id": "PM-3", "outcome_id": "PM3-YES"},
                            ],
                        },
                        {
                            "relation_type": "same_direction",
                            "legs": [
                                {"exchange": "kalshi", "market_id": "KX-1", "outcome_id": "KX-NO"},
                                {"exchange": "polymarket", "market_id": "PM-3", "outcome_id": "PM3-NO"},
                            ],
                        },
                    ],
                }
            ],
        },
    ]
    pair_details_payloads = {
        "/api/pairs/1": {
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
        },
        "/api/pairs/2": {
            "ok": True,
            "pair": {
                "pair_set": {"id": 2},
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
                            "market_id": "PM-2",
                            "raw_snapshot": {
                                "yes": {"outcome_id": "PM2-YES"},
                                "no": {"outcome_id": "PM2-NO"},
                            },
                        }
                    ],
                },
            },
        },
        "/api/pairs/3": {
            "ok": True,
            "pair": {
                "pair_set": {"id": 3},
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
                            "market_id": "PM-3",
                            "raw_snapshot": {
                                "yes": {"outcome_id": "PM3-YES"},
                                "no": {"outcome_id": "PM3-NO"},
                            },
                        }
                    ],
                },
            },
        },
    }

    def fake_http_get_json(base_url, path, **kwargs):  # noqa: ANN001
        if path == "/api/monitoring/pairs":
            assert monitoring_payloads
            return monitoring_payloads.pop(0)
        if path in pair_details_payloads:
            return pair_details_payloads[path]
        raise AssertionError(f"Unexpected path: {path}")

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(module, "_http_get_json", fake_http_get_json)
    monkeypatch.setattr(
        module,
        "_build_exchange",
        lambda exchange, **kwargs: FakeExchange(),
    )

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        mapping_subscription_debounce_seconds=0.2,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    runner._subscription_manager._print_line = runner._print_line  # type: ignore[attr-defined]

    manager = runner._subscription_manager
    try:
        runner._refresh_mappings(reason="startup")
        time.sleep(0.05)

        polymarket_ingestor = manager._exchange_ingestors["polymarket"]  # type: ignore[attr-defined]
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM-YES"),
            ("polymarket", "PM-NO"),
        }

        runner._refresh_mappings(reason="periodic")
        time.sleep(0.05)
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM-YES"),
            ("polymarket", "PM-NO"),
        }

        runner._refresh_mappings(reason="periodic")
        time.sleep(0.05)
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM-YES"),
            ("polymarket", "PM-NO"),
        }
        assert sum("updated stream subscriptions reason=periodic" in line for line in lines) == 0

        deadline = time.time() + 1.0
        while time.time() < deadline:
            runner._flush_pending_stream_subscription_update(time.time())
            if polymarket_ingestor.subscription_keys() == {
                ("polymarket", "PM3-YES"),
                ("polymarket", "PM3-NO"),
            }:
                break
            time.sleep(0.02)

        assert manager._exchange_ingestors["polymarket"] is polymarket_ingestor  # type: ignore[attr-defined]
        assert polymarket_ingestor.subscription_keys() == {
            ("polymarket", "PM3-YES"),
            ("polymarket", "PM3-NO"),
        }
        assert sum("updated stream subscriptions reason=periodic" in line for line in lines) == 1
    finally:
        runner.stop_event.set()
        manager.stop(reason="test", timeout_seconds=1.0)


def test_parse_polymarket_ws_quote_updates_prefers_best_bid_ask_and_book() -> None:
    updates = parse_polymarket_ws_quote_updates(
        json.dumps(
            [
                {
                    "event_type": "best_bid_ask",
                    "asset_id": "PM-YES",
                    "best_bid": "0.41",
                    "best_ask": "0.43",
                    "timestamp": "1736200000123",
                },
                {
                    "event_type": "book",
                    "asset_id": "PM-NO",
                    "bids": [{"price": "0.57"}, {"price": "0.56"}],
                    "asks": [{"price": "0.60"}, {"price": "0.61"}],
                    "timestamp": "1736200000456",
                },
            ]
        )
    )

    assert [update.outcome_id for update in updates] == ["PM-YES", "PM-NO"]
    assert updates[0].best_bid == 0.41
    assert updates[0].best_ask == 0.43
    assert updates[0].book_timestamp_ms == 1736200000123
    assert updates[0].source_label == "direct_ws_best_bid_ask"
    assert updates[1].best_bid == 0.57
    assert updates[1].best_ask == 0.6
    assert updates[1].book_timestamp_ms == 1736200000456
    assert updates[1].source_label == "direct_ws_book"


def test_parse_polymarket_ws_quote_updates_labels_price_changes() -> None:
    updates = parse_polymarket_ws_quote_updates(
        json.dumps(
            {
                "event_type": "price_change",
                "timestamp": "1736200000789",
                "price_changes": [
                    {
                        "asset_id": "PM-YES",
                        "best_bid": "0.51",
                        "best_ask": "0.53",
                    }
                ],
            }
        )
    )

    assert len(updates) == 1
    assert updates[0].outcome_id == "PM-YES"
    assert updates[0].best_bid == 0.51
    assert updates[0].best_ask == 0.53
    assert updates[0].book_timestamp_ms == 1736200000789
    assert updates[0].source_label == "direct_ws_price_change"


def test_multiplex_polymarket_direct_ws_ingests_shared_socket_updates(monkeypatch) -> None:
    import app.streaming.polymarket_ws as polymarket_ws_module

    sent_messages: list[str] = []
    recv_payloads = [
        json.dumps(
            [
                {
                    "event_type": "best_bid_ask",
                    "asset_id": "PM-YES",
                    "best_bid": "0.44",
                    "best_ask": "0.46",
                    "timestamp": str(int(time.time() * 1000)),
                }
            ]
        )
    ]

    class FakeConnection:
        def send(self, payload):  # noqa: ANN001
            sent_messages.append(payload)

        def recv(self, timeout=None, decode=None):  # noqa: ANN001
            if recv_payloads:
                return recv_payloads.pop(0)
            raise TimeoutError()

        def close(self) -> None:
            return None

    monkeypatch.setattr(polymarket_ws_module, "connect", lambda *args, **kwargs: FakeConnection())

    lines: list[str] = []
    seen_stream_keys: list[tuple[str, str]] = []
    stop_event = threading.Event()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            multiplex_polymarket_source_mode="direct",
            multiplex_polymarket_ping_interval_seconds=30.0,
            multiplex_polymarket_receive_timeout_seconds=0.01,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda stream_key: seen_stream_keys.append(stream_key),
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-YES"})
    manager._exchange_ingestors["polymarket"] = ingestor  # type: ignore[attr-defined]

    worker_thread = threading.Thread(target=manager.worker_loop, args=(ingestor,), daemon=True)
    dispatch_thread = threading.Thread(target=manager.dispatch_loop, args=(ingestor,), daemon=True)
    worker_thread.start()
    dispatch_thread.start()

    deadline = time.time() + 1.0
    while time.time() < deadline:
        if seen_stream_keys:
            break
        time.sleep(0.01)

    stop_event.set()
    ingestor.stop()
    worker_thread.join(timeout=1.0)
    dispatch_thread.join(timeout=1.0)

    snapshot = manager._quote_store.snapshot()  # type: ignore[attr-defined]
    assert snapshot["quotes"][("polymarket", "PM-YES")].best_bid == 0.44
    assert snapshot["quotes"][("polymarket", "PM-YES")].best_ask == 0.46
    assert snapshot["quotes"][("polymarket", "PM-YES")].source_label == "direct_ws_best_bid_ask"
    assert seen_stream_keys == [("polymarket", "PM-YES")]
    assert any('"assets_ids": ["PM-YES"]' in payload for payload in sent_messages)
    assert any("multiplex direct ws loop entering exchange=polymarket" in line for line in lines)


def test_multiplex_polymarket_direct_ws_runs_quiet_refresh_only_on_idle(monkeypatch) -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    refresh_calls: list[float] = []

    class FakeWsClient:
        def sync_subscriptions(self, outcome_ids):  # noqa: ANN001
            assert outcome_ids == {"PM-YES"}
            return set(), set()

        def recv_quote_updates(self):  # noqa: ANN001
            stop_event.set()
            raise TimeoutError()

    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            multiplex_polymarket_source_mode="direct",
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-YES"})
    manager._exchange_ingestors["polymarket"] = ingestor  # type: ignore[attr-defined]

    monkeypatch.setattr(manager, "_get_polymarket_ws_client", lambda: FakeWsClient())
    monkeypatch.setattr(manager, "_close_exchange_client", lambda exchange: None)
    monkeypatch.setattr(
        manager,
        "_refresh_polymarket_direct_quiet_books",
        lambda *, ingestor, now: refresh_calls.append(now),
    )

    manager._run_polymarket_direct_loop(ingestor)

    assert len(refresh_calls) == 1
    assert any("multiplex direct ws loop exiting exchange=polymarket" in line for line in lines)


def test_multiplex_polymarket_direct_quiet_refresh_fetches_stale_books() -> None:
    lines: list[str] = []

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            assert outcome_id == "PM-STALE"
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.42, "size": 10.0}],
                "asks": [{"price": 0.47, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            multiplex_polymarket_source_mode="direct",
            multiplex_polymarket_direct_quiet_refresh_seconds=1.0,
            multiplex_polymarket_direct_quiet_refresh_batch_size=2,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: FakeExchange(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-STALE"})
    manager._exchange_ingestors["polymarket"] = ingestor  # type: ignore[attr-defined]

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-STALE")] = now - 5.0

    manager._refresh_polymarket_direct_quiet_books(ingestor=ingestor, now=now)
    quote_update = ingestor.next_quote_update()

    assert quote_update is not None
    assert quote_update.quote.outcome_id == "PM-STALE"
    assert quote_update.quote.best_bid == 0.42
    assert quote_update.quote.best_ask == 0.47
    assert quote_update.quote.source_label == "quiet_refresh"
    assert any(
        "multiplex direct quiet refresh exchange=polymarket outcomes=PM-STALE count=1"
        in line
        for line in lines
    )


def test_multiplex_kalshi_warmup_releases_in_batches(monkeypatch) -> None:
    module = _load_script_module()

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        module,
        "_build_exchange",
        lambda exchange, **kwargs: FakeExchange(),
    )

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_warmup_batch_size=2,
        multiplex_warmup_interval_seconds=0.15,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    runner._subscription_manager._print_line = runner._print_line  # type: ignore[attr-defined]
    runner._subscription_manager._debug = runner._debug  # type: ignore[attr-defined]
    runner._subscription_manager._exchange_builder = lambda exchange, **kwargs: FakeExchange()  # type: ignore[attr-defined]

    manager = runner._subscription_manager
    try:
        manager.replace_streams(
            {
                ("kalshi", "KX-1"),
                ("kalshi", "KX-2"),
                ("kalshi", "KX-3"),
                ("kalshi", "KX-4"),
            }
        )

        kalshi_ingestor = manager._exchange_ingestors["kalshi"]  # type: ignore[attr-defined]
        time.sleep(0.05)
        assert kalshi_ingestor.active_outcome_ids() == {"KX-1", "KX-2"}
        assert kalshi_ingestor.pending_outcome_ids() == {"KX-3", "KX-4"}

        deadline = time.time() + 1.0
        while time.time() < deadline:
            if kalshi_ingestor.active_outcome_ids() == {"KX-1", "KX-2", "KX-3", "KX-4"}:
                break
            time.sleep(0.02)

        assert kalshi_ingestor.active_outcome_ids() == {"KX-1", "KX-2", "KX-3", "KX-4"}
        assert kalshi_ingestor.pending_outcome_ids() == set()
        assert any("multiplex warmup queued exchange=kalshi active=2 pending=2" in line for line in lines)
        assert any(
            "multiplex warmup released exchange=kalshi promoted=2 active=4 pending=0" in line
            for line in lines
        )
    finally:
        runner.stop_event.set()
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_stale_recovery_prioritizes_only_stale_outcome() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3"})

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now
        manager._stream_last_watch_ok_at[("polymarket", "PM-2")] = now
        manager._stream_last_watch_ok_at[("polymarket", "PM-3")] = now - 20.0

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )
    assert ingestor.next_outcome_id() == "PM-3"

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now + 1.0,
    )
    assert ingestor.next_outcome_id() == "PM-1"
    assert any(
        "multiplex stale recovery exchange=polymarket outcomes=PM-3 count=1" in line
        for line in lines
    )


def test_multiplex_aging_refresh_prioritizes_pre_stale_outcomes() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_aging_refresh_seconds=10.0,
        multiplex_aging_refresh_cooldown_seconds=5.0,
        multiplex_aging_refresh_batch_size=2,
        multiplex_stale_recovery_seconds=15.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3", "PM-4"})

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now - 2.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-2")] = now - 14.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-3")] = now - 11.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-4")] = now - 16.0

    manager._promote_aging_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.priority_outcome_ids() == {"PM-2", "PM-3"}
    assert ingestor.next_outcome_id() == "PM-2"
    assert ingestor.next_outcome_id() == "PM-3"
    assert any(
        "multiplex aging refresh exchange=polymarket outcomes=PM-2,PM-3 count=2 eligible=2"
        in line
        for line in lines
    )


def test_multiplex_aging_refresh_respects_existing_priority_budget() -> None:
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_aging_refresh_seconds=10.0,
        multiplex_aging_refresh_cooldown_seconds=5.0,
        multiplex_aging_refresh_batch_size=2,
        multiplex_stale_recovery_seconds=15.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3", "PM-4"})
    ingestor.prioritize_outcome_ids(["PM-4"])

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now - 14.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-2")] = now - 12.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-3")] = now - 11.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-4")] = now - 1.0

    manager._promote_aging_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.priority_outcome_ids() == {"PM-1", "PM-4"}


def test_multiplex_stale_recovery_limits_priority_batch_to_oldest_stale_outcomes() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
        multiplex_stale_recovery_batch_size=2,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3", "PM-4"})

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now
        manager._stream_last_watch_ok_at[("polymarket", "PM-2")] = now - 30.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-3")] = now - 10.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-4")] = now - 25.0

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.priority_outcome_ids() == {"PM-2", "PM-4"}
    assert ingestor.next_outcome_id() == "PM-2"
    assert ingestor.next_outcome_id() == "PM-4"
    assert any(
        "multiplex stale recovery exchange=polymarket outcomes=PM-2,PM-4 count=2 eligible=3"
        in line
        for line in lines
    )


def test_multiplex_stale_recovery_respects_existing_priority_backlog_budget() -> None:
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
        multiplex_stale_recovery_batch_size=2,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3", "PM-4"})
    ingestor.prioritize_outcome_ids(["PM-4"])

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now - 30.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-2")] = now - 25.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-3")] = now - 20.0
        manager._stream_last_watch_ok_at[("polymarket", "PM-4")] = now - 15.0

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.priority_outcome_ids() == {"PM-1", "PM-4"}


def test_multiplex_scheduler_prioritizes_active_outcomes_without_success() -> None:
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2", "PM-3"})

    with manager._state_lock:
        manager._stream_watch_success_count[("polymarket", "PM-1")] = 1

    first = ingestor.next_outcome_id()
    second = ingestor.next_outcome_id()
    assert {first, second} == {"PM-2", "PM-3"}


def test_multiplex_stale_recovery_includes_never_ok_active_outcomes() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1", "PM-2"})

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now
    with ingestor._subscriptions_lock:  # type: ignore[attr-defined]
        ingestor._active_since["PM-2"] = now - 20.0  # type: ignore[attr-defined]

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.next_outcome_id() == "PM-2"
    assert any(
        "multiplex stale recovery exchange=polymarket outcomes=PM-2 count=1" in line
        for line in lines
    )


def test_multiplex_kalshi_watch_stale_recovery_uses_forced_snapshot_refresh() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        kalshi_book_mode="auto",
        multiplex_warmup_batch_size=10,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    ingestor.replace_subscriptions({"KX-1", "KX-2", "KX-3"})

    with manager._state_lock:  # type: ignore[attr-defined]
        manager._exchange_source_mode["kalshi"] = "watch"  # type: ignore[attr-defined]
        manager._exchange_source_preference["kalshi"] = "watch"  # type: ignore[attr-defined]

    ingestor.enqueue_quote_update(
        QueuedQuoteUpdate(
            quote=QuoteSnapshot(
                exchange="kalshi",
                outcome_id="KX-2",
                best_bid=0.49,
                best_ask=0.50,
                book_timestamp_ms=None,
                updated_at=time.time(),
            ),
            watch_latency_ms=10.0,
            received_at=time.time(),
        )
    )
    ingestor.enqueue_quote_update(
        QueuedQuoteUpdate(
            quote=QuoteSnapshot(
                exchange="kalshi",
                outcome_id="KX-3",
                best_bid=0.48,
                best_ask=0.51,
                book_timestamp_ms=None,
                updated_at=time.time(),
            ),
            watch_latency_ms=11.0,
            received_at=time.time(),
        )
    )

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("kalshi", "KX-1")] = now
        manager._stream_last_watch_ok_at[("kalshi", "KX-2")] = now - 20.0
        manager._stream_last_watch_ok_at[("kalshi", "KX-3")] = now

    manager._recover_stale_outcomes(
        exchange="kalshi",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.outcome_ids() == {"KX-1", "KX-2", "KX-3"}
    assert ingestor.dispatch_queue_depth() == 2
    assert ingestor.priority_outcome_ids() == {"KX-2"}
    assert ingestor.forced_fetch_outcome_ids() == {"KX-2"}
    assert any(
        "multiplex forced snapshot refresh exchange=kalshi outcomes=KX-2 count=1"
        for line in lines
    )


def test_multiplex_kalshi_watch_aging_refresh_uses_forced_snapshot_fetch() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        kalshi_book_mode="auto",
        multiplex_warmup_batch_size=10,
        multiplex_aging_refresh_seconds=10.0,
        multiplex_aging_refresh_cooldown_seconds=5.0,
        multiplex_aging_refresh_batch_size=2,
        multiplex_stale_recovery_seconds=15.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    ingestor.replace_subscriptions({"KX-1", "KX-2", "KX-3"})

    with manager._state_lock:  # type: ignore[attr-defined]
        manager._exchange_source_mode["kalshi"] = "watch"  # type: ignore[attr-defined]
        manager._exchange_source_preference["kalshi"] = "watch"  # type: ignore[attr-defined]

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("kalshi", "KX-1")] = now - 2.0
        manager._stream_last_watch_ok_at[("kalshi", "KX-2")] = now - 14.0
        manager._stream_last_watch_ok_at[("kalshi", "KX-3")] = now - 11.0

    manager._promote_aging_outcomes(
        exchange="kalshi",
        ingestor=ingestor,
        now=now,
    )

    assert ingestor.priority_outcome_ids() == {"KX-2", "KX-3"}
    assert ingestor.forced_fetch_outcome_ids() == {"KX-2", "KX-3"}
    assert any(
        "multiplex aging refresh exchange=kalshi outcomes=KX-2,KX-3 count=2 eligible=2"
        in line
        for line in lines
    )


def test_multiplex_kalshi_forced_snapshot_refresh_fetches_while_source_mode_stays_watch() -> None:
    class FakeKalshiExchange:
        def __init__(self) -> None:
            self.watch_calls = 0
            self.fetch_calls = 0

        def watch_order_book(self, outcome_id, limit=None):  # noqa: ANN001
            self.watch_calls += 1
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            self.fetch_calls += 1
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.48, "size": 10.0}],
                "asks": [{"price": 0.51, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    client = FakeKalshiExchange()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_warmup_batch_size=10,
            multiplex_stale_recovery_seconds=5.0,
            multiplex_stale_recovery_cooldown_seconds=10.0,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: client,
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    ingestor.replace_subscriptions({"KX-1"})
    manager._exchange_ingestors["kalshi"] = ingestor  # type: ignore[attr-defined]

    with manager._state_lock:  # type: ignore[attr-defined]
        manager._exchange_source_mode["kalshi"] = "watch"  # type: ignore[attr-defined]
        manager._exchange_source_preference["kalshi"] = "watch"  # type: ignore[attr-defined]

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("kalshi", "KX-1")] = now - 20.0

    manager._recover_stale_outcomes(
        exchange="kalshi",
        ingestor=ingestor,
        now=now,
    )
    book = manager._fetch_multiplex_order_book(
        client=client,
        exchange="kalshi",
        outcome_id="KX-1",
    )

    assert book["bids"][0]["price"] == 0.48
    assert client.fetch_calls == 1
    assert client.watch_calls == 0
    assert ingestor.forced_fetch_outcome_ids() == {"KX-1"}


def test_multiplex_polymarket_worker_count_is_clamped_to_one() -> None:
    active_fetches = 0
    active_fetches_lock = threading.Lock()
    built_clients: list[object] = []
    max_concurrent_fetches = 0

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            nonlocal active_fetches, max_concurrent_fetches
            with active_fetches_lock:
                active_fetches += 1
                max_concurrent_fetches = max(max_concurrent_fetches, active_fetches)
            try:
                time.sleep(0.05)
                return {
                    "timestamp": int(time.time() * 1000),
                    "bids": [{"price": 0.49, "size": 10.0}],
                    "asks": [{"price": 0.50, "size": 10.0}],
                }
            finally:
                with active_fetches_lock:
                    active_fetches -= 1

        def close(self) -> None:
            return None

    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            multiplex_polymarket_worker_count=4,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: built_clients.append(FakeExchange()) or built_clients[-1],
        on_quote_update=lambda _stream_key: None,
    )

    try:
        manager.replace_streams(
            {
                ("polymarket", "PM-1"),
                ("polymarket", "PM-2"),
                ("polymarket", "PM-3"),
                ("polymarket", "PM-4"),
            }
        )

        ingestor = manager._exchange_ingestors["polymarket"]
        deadline = time.time() + 0.3
        while time.time() < deadline and max_concurrent_fetches < 1:
            time.sleep(0.02)

        assert ingestor.producer_thread_count() == 1
        assert max_concurrent_fetches == 1
        assert len(built_clients) == 1
    finally:
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_polymarket_invalid_order_book_outcome_is_disabled() -> None:
    lines: list[str] = []

    class FakeExchange:
        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            if outcome_id == "PM-BAD":
                raise Exception("Failed to fetch order book: Order not found: unknown")
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    quote_store = QuoteStore()
    quote_store.replace_mappings(
        [],
        {("polymarket", "PM-BAD"), ("polymarket", "PM-GOOD")},
    )
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
        ),
        quote_store=quote_store,
        stop_event=threading.Event(),
        print_line=lines.append,
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: FakeExchange(),
        on_quote_update=lambda _stream_key: None,
    )

    try:
        manager.replace_streams(
            {
                ("polymarket", "PM-BAD"),
                ("polymarket", "PM-GOOD"),
            }
        )

        deadline = time.time() + 1.0
        while time.time() < deadline:
            ingestor = manager._exchange_ingestors["polymarket"]
            if "PM-BAD" not in ingestor.outcome_ids():
                break
            time.sleep(0.02)

        ingestor = manager._exchange_ingestors["polymarket"]
        assert ingestor.outcome_ids() == {"PM-GOOD"}
        assert quote_store.snapshot()["active_streams"] == [("polymarket", "PM-GOOD")]
        assert any(
            line
            == "[warn] disabled invalid order book outcome exchange=polymarket "
            "outcome_id=PM-BAD reason=order_book_not_found"
            for line in lines
        )
        assert not any("multiplex stream failure exchange=polymarket outcome_id=PM-BAD" in line for line in lines)
    finally:
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_exchange_mode_transitions() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_degraded_failure_threshold=2,
        multiplex_mode_hysteresis_seconds=0.05,
        multiplex_stale_recovery_seconds=5.0,
        multiplex_stale_recovery_cooldown_seconds=10.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    ingestor.replace_subscriptions({"PM-1"})

    now = time.time()
    with manager._state_lock:
        manager._stream_last_watch_ok_at[("polymarket", "PM-1")] = now - 20.0

    manager._recover_stale_outcomes(
        exchange="polymarket",
        ingestor=ingestor,
        now=now,
    )
    assert manager.health_state(now=now).exchange_modes["polymarket"] == "recovering"

    manager._register_exchange_failure(
        exchange="polymarket",
        reason="stream_failure",
        outcome_id="PM-1",
    )
    manager._register_exchange_failure(
        exchange="polymarket",
        reason="rate_limit",
        outcome_id="PM-1",
    )
    assert manager.health_state(now=now).exchange_modes["polymarket"] == "degraded"

    manager._mark_stream_success("polymarket")
    assert manager.health_state(now=now).exchange_modes["polymarket"] == "degraded"
    time.sleep(0.06)
    manager._mark_stream_success("polymarket")
    assert manager.health_state(now=now).exchange_modes["polymarket"] == "healthy"
    assert any("exchange_mode exchange=polymarket mode=recovering reason=stale_recovery" in line for line in lines)
    assert any("exchange_mode exchange=polymarket mode=degraded reason=rate_limit" in line for line in lines)
    assert any("exchange_mode exchange=polymarket mode=healthy reason=stream_ok" in line for line in lines)


def test_multiplex_exchange_source_mode_transitions() -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    stop_event.set()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager.replace_streams({("polymarket", "PM-1")})
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["polymarket"] == "poll"

    manager._register_exchange_failure(
        exchange="polymarket",
        reason="stream_failure",
        outcome_id="PM-1",
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["polymarket"] == "degraded"

    manager._mark_stream_success("polymarket")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["polymarket"] == "poll"
    assert any(
        "exchange_source_mode exchange=polymarket mode=poll reason=subscription_start"
        in line
        for line in lines
    )
    assert any(
        "exchange_source_mode exchange=polymarket mode=degraded reason=stream_failure outcome_id=PM-1"
        in line
        for line in lines
    )
    assert any(
        "exchange_source_mode exchange=polymarket mode=poll reason=stream_ok"
        in line
        for line in lines
    )


def test_multiplex_kalshi_watch_first_uses_watch_order_book() -> None:
    class FakeKalshiExchange:
        def __init__(self) -> None:
            self.watch_calls = 0
            self.fetch_calls = 0

        def watch_order_book(self, outcome_id, limit=None):  # noqa: ANN001
            self.watch_calls += 1
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            self.fetch_calls += 1
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.48, "size": 10.0}],
                "asks": [{"price": 0.51, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    client = FakeKalshiExchange()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_warmup_batch_size=25,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: client,
        on_quote_update=lambda _stream_key: None,
    )

    try:
        manager.replace_streams({("kalshi", "KX-1")})
        deadline = time.time() + 2.0
        while time.time() < deadline:
            health = manager.health_state(now=time.time())
            if health.quote_count >= 1:
                break
            time.sleep(0.02)

        health = manager.health_state(now=time.time())
        assert health.quote_count >= 1
        assert health.exchange_source_modes["kalshi"] == "watch"
        assert client.watch_calls >= 1
        assert client.fetch_calls == 0
    finally:
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_kalshi_watch_mode_uses_worker_scoped_clients() -> None:
    built_clients: list[object] = []

    class FakeKalshiExchange:
        def close(self) -> None:
            return None

    def build_exchange(exchange, **kwargs):  # noqa: ANN001, ANN003
        assert exchange == "kalshi"
        client = FakeKalshiExchange()
        built_clients.append(client)
        return client

    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_kalshi_worker_count=2,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=build_exchange,
        on_quote_update=lambda _stream_key: None,
    )

    with manager._state_lock:  # type: ignore[attr-defined]
        manager._exchange_source_mode["kalshi"] = "watch"  # type: ignore[attr-defined]
        manager._exchange_source_preference["kalshi"] = "watch"  # type: ignore[attr-defined]

    client0 = manager._get_client_for_worker(exchange="kalshi", worker_index=0)
    client1 = manager._get_client_for_worker(exchange="kalshi", worker_index=1)
    client0_again = manager._get_client_for_worker(exchange="kalshi", worker_index=0)

    assert client0 is client0_again
    assert client0 is not client1
    assert len(built_clients) == 2
    assert manager.health_state(now=time.time()).client_exchanges == ["kalshi"]


def test_multiplex_kalshi_poll_mode_keeps_shared_client_across_workers() -> None:
    built_clients: list[object] = []

    class FakeKalshiExchange:
        def close(self) -> None:
            return None

    def build_exchange(exchange, **kwargs):  # noqa: ANN001, ANN003
        assert exchange == "kalshi"
        client = FakeKalshiExchange()
        built_clients.append(client)
        return client

    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="poll",
            multiplex_kalshi_worker_count=2,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=build_exchange,
        on_quote_update=lambda _stream_key: None,
    )

    client0 = manager._get_client_for_worker(exchange="kalshi", worker_index=0)
    client1 = manager._get_client_for_worker(exchange="kalshi", worker_index=1)

    assert client0 is client1
    assert len(built_clients) == 1


def test_multiplex_kalshi_watch_auto_falls_back_to_poll_on_rate_limit() -> None:
    lines: list[str] = []

    class FakeKalshiExchange:
        def __init__(self) -> None:
            self.watch_calls = 0
            self.fetch_calls = 0

        def watch_order_book(self, outcome_id, limit=None):  # noqa: ANN001
            self.watch_calls += 1
            raise Exception("429 too many requests")

        def fetch_order_book(self, outcome_id):  # noqa: ANN001
            self.fetch_calls += 1
            return {
                "timestamp": int(time.time() * 1000),
                "bids": [{"price": 0.49, "size": 10.0}],
                "asks": [{"price": 0.50, "size": 10.0}],
            }

        def close(self) -> None:
            return None

    client = FakeKalshiExchange()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_backoff_jitter_ratio=0.0,
            multiplex_kalshi_watch_fallback_rate_limit_threshold=1,
        ),
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: client,
        on_quote_update=lambda _stream_key: None,
    )

    try:
        manager.replace_streams({("kalshi", "KX-1")})

        deadline = time.time() + 4.0
        while time.time() < deadline:
            health = manager.health_state(now=time.time())
            if (
                health.quote_count >= 1
                and health.exchange_source_modes.get("kalshi") == "poll"
                and client.fetch_calls >= 1
            ):
                break
            time.sleep(0.05)

        health = manager.health_state(now=time.time())
        assert client.watch_calls >= 1
        assert client.fetch_calls >= 1
        assert health.quote_count >= 1
        assert health.exchange_source_modes["kalshi"] == "poll"
        assert any(
            "exchange_source_mode exchange=kalshi mode=watch reason=subscription_start"
            in line
            for line in lines
        )
        assert any(
            "exchange_source_mode exchange=kalshi mode=poll reason=rate_limit_storm_auto_fallback"
            in line
            for line in lines
        )
        assert any(
            "kalshi_multiplex_source_mode=poll reason=rate_limit_storm_auto_fallback"
            in line
            for line in lines
        )
    finally:
        manager.stop(reason="test", timeout_seconds=1.0)


def test_multiplex_kalshi_single_worker_rate_limit_keeps_watch_mode() -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    stop_event.set()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_backoff_jitter_ratio=0.0,
            multiplex_mode_hysteresis_seconds=0.0,
            multiplex_kalshi_worker_count=2,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager.replace_streams({("kalshi", "KX-1")})
    manager._register_rate_limit(
        exchange="kalshi",
        outcome_id="KX-1",
        exc=RuntimeError("429 too many requests"),
        worker_index=0,
    )

    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "watch"
    assert health.exchange_modes["kalshi"] == "recovering"
    assert health.rate_limit_counts["kalshi"] == 1
    assert any("exchange_mode exchange=kalshi mode=recovering reason=rate_limit" in line for line in lines)
    assert not any(
        "exchange_source_mode exchange=kalshi mode=degraded reason=rate_limit" in line
        for line in lines
    )


def test_multiplex_kalshi_worker_recovery_restores_watch_after_all_workers_blocked() -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    stop_event.set()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_backoff_jitter_ratio=0.0,
            multiplex_mode_hysteresis_seconds=0.0,
            multiplex_kalshi_worker_count=2,
            multiplex_kalshi_successes_to_restore_watch=2,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager.replace_streams({("kalshi", "KX-1")})
    manager._register_rate_limit(
        exchange="kalshi",
        outcome_id="KX-1",
        exc=RuntimeError("429 too many requests"),
        worker_index=0,
    )
    manager._register_rate_limit(
        exchange="kalshi",
        outcome_id="KX-1",
        exc=RuntimeError("429 too many requests"),
        worker_index=1,
    )
    with manager._state_lock:
        manager._worker_exchange_retry_not_before[("kalshi", 0)] = time.time() - 1.0
        manager._worker_exchange_retry_not_before[("kalshi", 1)] = time.time() + 60.0

    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "degraded"

    manager._mark_stream_success("kalshi", worker_index=0)
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "degraded"
    assert health.exchange_modes["kalshi"] == "recovering"
    assert health.rate_limit_counts["kalshi"] == 2

    manager._mark_stream_success("kalshi", worker_index=0)
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "watch"
    assert health.exchange_modes["kalshi"] == "recovering"
    assert health.rate_limit_counts["kalshi"] == 1
    assert any(
        "exchange_source_mode exchange=kalshi mode=degraded reason=rate_limit outcome_id=KX-1"
        in line
        for line in lines
    )
    assert any(
        "exchange_source_mode exchange=kalshi mode=watch reason=stream_ok"
        in line
        for line in lines
    )


def test_multiplex_kalshi_source_mode_hysteresis_delays_return_to_watch() -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    stop_event.set()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_source_mode_hysteresis_seconds=0.05,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager.replace_streams({("kalshi", "KX-1")})
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "watch"

    manager._set_exchange_source_mode(
        exchange="kalshi",
        mode="poll",
        reason="rate_limit_storm_auto_fallback",
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "poll"

    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "poll"

    time.sleep(0.06)
    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "watch"
    assert any(
        "exchange_source_mode exchange=kalshi mode=poll reason=rate_limit_storm_auto_fallback"
        in line
        for line in lines
    )
    assert any(
        "exchange_source_mode exchange=kalshi mode=watch reason=stream_ok"
        in line
        for line in lines
    )


def test_multiplex_rate_limit_backoff_uses_bounded_jitter() -> None:
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=False,
        multiplex_backoff_jitter_ratio=0.2,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    started_at = time.time()
    manager._register_rate_limit(
        exchange="polymarket",
        outcome_id="PM-1",
        exc=RuntimeError("429 too many requests"),
    )
    retry_delay = manager._exchange_retry_not_before["polymarket"] - started_at

    assert 1.55 <= retry_delay <= 2.45
    assert retry_delay != pytest.approx(2.0, rel=1e-6)


def test_multiplex_kalshi_rate_limit_recovery_requires_success_streak() -> None:
    lines: list[str] = []
    stop_event = threading.Event()
    stop_event.set()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            kalshi_book_mode="auto",
            multiplex_degraded_failure_threshold=1,
            multiplex_mode_hysteresis_seconds=0.0,
            multiplex_backoff_jitter_ratio=0.0,
            multiplex_kalshi_successes_to_restore_watch=3,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager.replace_streams({("kalshi", "KX-1")})
    manager._register_rate_limit(
        exchange="kalshi",
        outcome_id="KX-1",
        exc=RuntimeError("429 too many requests"),
    )
    with manager._state_lock:
        manager._exchange_retry_not_before["kalshi"] = time.time() - 1.0

    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "degraded"
    assert health.rate_limit_counts["kalshi"] == 1

    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "degraded"
    assert health.exchange_modes["kalshi"] == "degraded"
    assert health.rate_limit_counts["kalshi"] == 1

    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "degraded"
    assert health.exchange_modes["kalshi"] == "degraded"
    assert health.rate_limit_counts["kalshi"] == 1

    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_source_modes["kalshi"] == "watch"
    assert health.exchange_modes["kalshi"] == "healthy"
    assert health.rate_limit_counts["kalshi"] == 0
    assert any(
        "exchange_source_mode exchange=kalshi mode=watch reason=stream_ok" in line
        for line in lines
    )


def test_multiplex_auth_recovery_policy_is_explicit() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_degraded_failure_threshold=2,
        multiplex_mode_hysteresis_seconds=0.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager._register_auth_error(
        exchange="kalshi",
        outcome_id="KX-YES",
        exc=RuntimeError("unauthorized access token"),
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["kalshi"] == "recovering"
    assert health.exchange_recovery_reasons["kalshi"] == "auth_refresh"

    manager._register_auth_error(
        exchange="kalshi",
        outcome_id="KX-YES",
        exc=RuntimeError("unauthorized access token"),
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["kalshi"] == "degraded"
    assert health.exchange_recovery_reasons["kalshi"] == "auth_refresh"

    manager._mark_stream_success("kalshi")
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["kalshi"] == "healthy"
    assert "kalshi" not in health.exchange_recovery_reasons
    assert any("exchange_recovery exchange=kalshi policy=auth_refresh mode=recovering" in line for line in lines)
    assert any("exchange_recovery exchange=kalshi policy=auth_refresh mode=degraded" in line for line in lines)


def test_multiplex_sidecar_recovery_policy_is_explicit() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_degraded_failure_threshold=2,
        multiplex_mode_hysteresis_seconds=0.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager._register_sidecar_unavailable(
        exchange="polymarket",
        outcome_id="PM-YES",
        exc=RuntimeError("connection refused"),
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["polymarket"] == "recovering"
    assert health.exchange_recovery_reasons["polymarket"] == "sidecar_restart"

    manager._register_sidecar_unavailable(
        exchange="polymarket",
        outcome_id="PM-YES",
        exc=RuntimeError("connection refused"),
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["polymarket"] == "degraded"
    assert health.exchange_recovery_reasons["polymarket"] == "sidecar_restart"
    assert any(
        "exchange_recovery exchange=polymarket policy=sidecar_restart mode=degraded" in line
        for line in lines
    )


def test_multiplex_rate_limit_storm_policy_is_explicit() -> None:
    lines: list[str] = []
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        multiplex_degraded_failure_threshold=2,
        multiplex_mode_hysteresis_seconds=0.0,
        multiplex_backoff_jitter_ratio=0.0,
        multiplex_rate_limit_storm_threshold=2,
        multiplex_rate_limit_storm_min_delay_seconds=4.0,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: lines.append(line),
        debug=lambda line: lines.append(f"[debug] {line}"),
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )

    manager._register_rate_limit(
        exchange="polymarket",
        outcome_id="PM-YES",
        exc=RuntimeError("429 too many requests"),
    )
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["polymarket"] == "recovering"
    assert health.exchange_recovery_reasons["polymarket"] == "rate_limit_backoff"

    started_at = time.time()
    manager._register_rate_limit(
        exchange="polymarket",
        outcome_id="PM-YES",
        exc=RuntimeError("429 too many requests"),
    )
    health = manager.health_state(now=time.time())
    retry_delay = manager._exchange_retry_not_before["polymarket"] - started_at
    assert health.exchange_modes["polymarket"] == "degraded"
    assert health.exchange_recovery_reasons["polymarket"] == "rate_limit_storm_backoff"
    assert manager._exchange_rate_limit_count["polymarket"] == 2
    assert retry_delay >= 4.0
    assert any(
        "exchange_recovery exchange=polymarket policy=rate_limit_storm_backoff mode=degraded"
        in line
        for line in lines
    )
    assert any("exchange_rate_limit_storm exchange=polymarket" in line for line in lines)

    manager._mark_stream_success("polymarket")
    health = manager.health_state(now=time.time())
    assert health.exchange_modes["polymarket"] == "healthy"
    assert "polymarket" not in health.exchange_recovery_reasons
    assert manager._exchange_rate_limit_count["polymarket"] == 0


def test_multiplex_kalshi_recovery_budgets_scale_with_worker_capacity() -> None:
    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=False,
        kalshi_book_mode="auto",
        multiplex_stale_recovery_batch_size=2,
        multiplex_aging_refresh_batch_size=2,
        multiplex_kalshi_worker_count=2,
    )
    manager = MultiplexSubscriptionManager(
        args=args,
        quote_store=QuoteStore(),
        stop_event=threading.Event(),
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    outcome_ids = {f"KX-{idx}" for idx in range(6)}
    ingestor.replace_subscriptions(outcome_ids)

    now = time.time()
    with manager._state_lock:
        for outcome_id in outcome_ids:
            manager._stream_last_watch_ok_at[("kalshi", outcome_id)] = now - 20.0

    manager._recover_stale_outcomes(
        exchange="kalshi",
        ingestor=ingestor,
        now=now,
    )
    assert len(ingestor.forced_fetch_outcome_ids()) == 4

    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    ingestor.replace_subscriptions(outcome_ids)
    with manager._state_lock:
        manager._next_aging_refresh_at.clear()
        for outcome_id in outcome_ids:
            manager._stream_last_watch_ok_at[("kalshi", outcome_id)] = now - 12.0

    manager._promote_aging_outcomes(
        exchange="kalshi",
        ingestor=ingestor,
        now=now,
    )
    assert len(ingestor.forced_fetch_outcome_ids()) == 4


def test_multiplex_dispatch_queue_is_bounded_and_drops_oldest() -> None:
    stop_event = threading.Event()
    manager = MultiplexSubscriptionManager(
        args=argparse.Namespace(
            api_base_url="http://127.0.0.1:8011",
            arb_threshold=0.02,
            deviation_threshold=0.03,
            cooldown_seconds=60.0,
            mapping_refresh_seconds=3600.0,
            book_stale_seconds=15.0,
            depth=None,
            include_expired=False,
            include_inactive=False,
            engine="multiplex",
            debug=True,
            multiplex_dispatch_queue_size=2,
        ),
        quote_store=QuoteStore(),
        stop_event=stop_event,
        print_line=lambda line: None,
        debug=lambda line: None,
        exchange_builder=lambda exchange, **kwargs: object(),
        on_quote_update=lambda _stream_key: None,
    )
    ingestor = MultiplexExchangeIngestor(manager=manager, exchange="polymarket")
    manager._exchange_ingestors["polymarket"] = ingestor  # type: ignore[attr-defined]

    for idx in range(3):
        ingestor.enqueue_quote_update(
            QueuedQuoteUpdate(
                quote=QuoteSnapshot(
                    exchange="polymarket",
                    outcome_id=f"PM-{idx}",
                    best_bid=0.40 + (idx * 0.01),
                    best_ask=0.50 + (idx * 0.01),
                    book_timestamp_ms=None,
                    updated_at=100.0 + idx,
                ),
                watch_latency_ms=5.0 + idx,
                received_at=200.0 + idx,
            )
        )

    health = manager.health_state(now=time.time())
    assert health.exchange_queue_depth_counts["polymarket"] == 2
    assert health.exchange_dispatch_drop_counts["polymarket"] == 1

    stop_event.set()
    manager.dispatch_loop(ingestor)

    snapshot = manager._quote_store.snapshot()  # type: ignore[attr-defined]
    assert snapshot["quotes"].keys() == {
        ("polymarket", "PM-1"),
        ("polymarket", "PM-2"),
    }
    assert snapshot["quotes"][("polymarket", "PM-1")].best_ask == 0.51
    assert snapshot["quotes"][("polymarket", "PM-2")].best_ask == 0.52


def test_debug_heartbeat_includes_exchange_modes(monkeypatch) -> None:
    module = _load_script_module()

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        debug_heartbeat_seconds=10.0,
        compact_alerts=False,
        color="never",
        multiplex_warmup_batch_size=2,
        multiplex_warmup_interval_seconds=60.0,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    manager = runner._subscription_manager
    manager._print_line = runner._print_line  # type: ignore[attr-defined]

    kalshi_ingestor = MultiplexExchangeIngestor(manager=manager, exchange="kalshi")
    kalshi_ingestor.replace_subscriptions({"KX-1", "KX-2", "KX-3"})
    manager._exchange_ingestors["kalshi"] = kalshi_ingestor  # type: ignore[attr-defined]
    runner._store.replace_mappings(
        [],
        {("kalshi", "KX-1"), ("kalshi", "KX-2"), ("kalshi", "KX-3")},
    )
    runner._store.upsert_quote(
        QuoteSnapshot(
            exchange="kalshi",
            outcome_id="KX-1",
            best_bid=0.49,
            best_ask=0.50,
            book_timestamp_ms=None,
            updated_at=time.time() - 30.0,
        )
    )
    runner._store.upsert_quote(
        QuoteSnapshot(
            exchange="kalshi",
            outcome_id="KX-2",
            best_bid=0.48,
            best_ask=0.51,
            book_timestamp_ms=None,
            updated_at=time.time(),
        )
    )
    heartbeat_now = time.time()
    with manager._state_lock:  # type: ignore[attr-defined]
        manager._exchange_source_mode["kalshi"] = "poll"  # type: ignore[attr-defined]
        manager._exchange_mode["kalshi"] = "recovering"  # type: ignore[attr-defined]
        manager._exchange_recovery_reason["kalshi"] = "auth_refresh"  # type: ignore[attr-defined]
        manager._stream_watch_success_count[("kalshi", "KX-2")] = 1  # type: ignore[attr-defined]
        manager._stream_last_watch_latency_ms[("kalshi", "KX-2")] = 11.0  # type: ignore[attr-defined]
        manager._stream_last_watch_ok_at[("kalshi", "KX-2")] = heartbeat_now - 6.0  # type: ignore[attr-defined]
    kalshi_ingestor.prioritize_outcome_ids(["KX-2"])
    runner._runner_started_at = heartbeat_now - 60.0
    runner._quote_update_count = 30
    runner._quote_updates_since_last_eval = 3
    runner._last_quote_update_at = heartbeat_now - 1.5
    runner._last_eval_completed_at = heartbeat_now - 2.0
    runner._last_eval_duration_ms = 12.5
    runner._last_eval_mapping_count = 2
    runner._last_eval_raw_event_count = 5
    runner._last_eval_due_event_count = 4
    runner._last_eval_emitted_count = 2
    runner._eval_count = 10
    runner._record_latency_metrics(
        exchange="kalshi",
        quote_seen_to_alert_ms=12.0,
        book_timestamp_to_alert_ms=450.0,
        exchange_update_to_store_ms=200.0,
        store_to_alert_ms=25.0,
    )

    runner._emit_debug_heartbeat(heartbeat_now)

    assert any("exchange_source_modes={'kalshi': 'poll'}" in line for line in lines)
    assert any("exchange_modes={'kalshi': 'recovering'}" in line for line in lines)
    assert any("exchange_recovery_reasons={'kalshi': 'auth_refresh'}" in line for line in lines)
    assert any("stale_quote_count=2" in line for line in lines)
    assert any("exchange_subscription_counts={'kalshi': 3}" in line for line in lines)
    assert any("exchange_active_counts={'kalshi': 2}" in line for line in lines)
    assert any("exchange_pending_counts={'kalshi': 1}" in line for line in lines)
    assert any("exchange_priority_counts={'kalshi': 1}" in line for line in lines)
    assert any("exchange_queue_depth_counts={'kalshi': 2}" in line for line in lines)
    assert any("exchange_dispatch_drop_counts={'kalshi': 0}" in line for line in lines)
    assert any("last_eval_duration_ms=12.500" in line for line in lines)
    assert any("last_eval_mapping_count=2" in line for line in lines)
    assert any("last_eval_raw_event_count=5" in line for line in lines)
    assert any("last_eval_due_event_count=4" in line for line in lines)
    assert any("last_eval_emitted_count=2" in line for line in lines)
    assert any("eval_count=10" in line for line in lines)
    assert any("evals_per_min=10.000" in line for line in lines)
    assert any("quote_update_count=30" in line for line in lines)
    assert any("quote_updates_since_last_eval=3" in line for line in lines)
    assert any("quote_updates_per_min=30.000" in line for line in lines)
    assert any("exchange_update_counts={'kalshi': 1}" in line for line in lines)
    assert any("exchange_updates_per_min={'kalshi': 1.0}" in line for line in lines)
    assert any("exchange_last_ok_age_p95_s={'kalshi': 6.0}" in line for line in lines)
    assert any("exchange_last_ok_age_max_s={'kalshi': 6.0}" in line for line in lines)
    assert any("exchange_latency_p95_ms={'kalshi': 11.0}" in line for line in lines)
    assert any("exchange_latency_max_ms={'kalshi': 11.0}" in line for line in lines)
    assert any(
        "quote_seen_to_alert_sample_count={'kalshi': 1, 'overall': 1}" in line for line in lines
    )
    assert any(
        "quote_seen_to_alert_p50_ms={'kalshi': 12.0, 'overall': 12.0}" in line
        for line in lines
    )
    assert any(
        "book_timestamp_to_alert_p95_ms={'kalshi': 450.0, 'overall': 450.0}" in line
        for line in lines
    )
    assert any(
        "exchange_update_to_store_p99_ms={'kalshi': 200.0, 'overall': 200.0}" in line
        for line in lines
    )
    assert any(
        "store_to_alert_p50_ms={'kalshi': 25.0, 'overall': 25.0}" in line for line in lines
    )
    assert any("last_quote_update_age_s=" in line for line in lines)
    assert any("eval_age_s=" in line for line in lines)


def test_debug_heartbeat_splits_latency_metrics_by_source() -> None:
    module = _load_script_module()

    args = argparse.Namespace(
        api_base_url="http://127.0.0.1:8011",
        arb_threshold=0.02,
        deviation_threshold=0.03,
        cooldown_seconds=60.0,
        mapping_refresh_seconds=3600.0,
        book_stale_seconds=15.0,
        depth=None,
        include_expired=False,
        include_inactive=False,
        engine="multiplex",
        debug=True,
        debug_heartbeat_seconds=10.0,
        compact_alerts=False,
        color="never",
        multiplex_warmup_batch_size=2,
        multiplex_warmup_interval_seconds=60.0,
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]
    manager = runner._subscription_manager
    manager._print_line = runner._print_line  # type: ignore[attr-defined]

    runner._record_latency_metrics(
        exchange="polymarket",
        source_label="direct_ws_price_change",
        quote_seen_to_alert_ms=4.0,
        book_timestamp_to_alert_ms=40.0,
        exchange_update_to_store_ms=35.0,
        store_to_alert_ms=1.0,
    )
    runner._record_latency_metrics(
        exchange="polymarket",
        source_label="quiet_refresh",
        quote_seen_to_alert_ms=6.0,
        book_timestamp_to_alert_ms=6000.0,
        exchange_update_to_store_ms=5990.0,
        store_to_alert_ms=2.0,
    )

    runner._emit_debug_heartbeat(time.time())

    assert any(
        "quote_seen_to_alert_sample_count={'overall': 2, 'polymarket': 2, 'polymarket:direct_ws_price_change': 1, 'polymarket:quiet_refresh': 1}"
        in line
        for line in lines
    )
    assert any(
        "book_timestamp_to_alert_p50_ms={'overall': 40.0, 'polymarket': 40.0, 'polymarket:direct_ws_price_change': 40.0, 'polymarket:quiet_refresh': 6000.0}"
        in line
        for line in lines
    )
    assert any(
        "exchange_update_to_store_p95_ms={'overall': 5990.0, 'polymarket': 5990.0, 'polymarket:direct_ws_price_change': 35.0, 'polymarket:quiet_refresh': 5990.0}"
        in line
        for line in lines
    )


def test_shadow_run_diagnoses_multiplex_only_alert_as_legacy_missing_quotes() -> None:
    module = _load_named_script_module("ws_arb_shadow_run")

    mapping = SemanticMapping(
        pair_id=2,
        relation_type="same_direction",
        kalshi_market_id="KX-2",
        polymarket_market_id="PM-2",
        kalshi_p="KX-YES",
        kalshi_not_p="KX-NO",
        polymarket_p="PM-YES",
        polymarket_not_p="PM-NO",
    )
    diagnostics = module._build_alert_diagnostics(
        alerts={
            "[ALERT_DEVIATION] pair_id=2 kalshi_market_id=KX-2 polymarket_market_id=PM-2 "
            "relation_type=same_direction metric=same_token_gap_p value=0.1000 threshold=0.0300 "
            "kalshi_p_ask=0.4500 polymarket_p_ask=0.5500": 1
        },
        mapping_lookup={(2, "KX-2", "PM-2"): mapping},
        legacy_stream_states={
            ("kalshi", "KX-YES"): {
                "exchange": "kalshi",
                "outcome_id": "KX-YES",
                "ok": 0,
                "err": 0,
                "last_latency_ms": None,
                "last_ok_age_s": None,
            },
            ("polymarket", "PM-YES"): {
                "exchange": "polymarket",
                "outcome_id": "PM-YES",
                "ok": 1,
                "err": 0,
                "last_latency_ms": 180.0,
                "last_ok_age_s": 1.2,
            },
        },
        multiplex_stream_states={
            ("kalshi", "KX-YES"): {
                "exchange": "kalshi",
                "outcome_id": "KX-YES",
                "ok": 1,
                "err": 0,
                "last_latency_ms": 1800.0,
                "last_ok_age_s": 0.8,
            },
            ("polymarket", "PM-YES"): {
                "exchange": "polymarket",
                "outcome_id": "PM-YES",
                "ok": 1,
                "err": 0,
                "last_latency_ms": 180.0,
                "last_ok_age_s": 0.4,
            },
        },
    )

    assert len(diagnostics) == 1
    assert diagnostics[0]["diagnosis"] == "legacy_missing_or_stale_required_quotes"
    required = diagnostics[0]["required_streams"]
    assert required[0]["exchange"] == "kalshi"
    assert required[0]["legacy_status"] == "never_ok"
    assert required[0]["multiplex_status"] == "fresh"


def test_soak_summary_reports_stream_age_and_mode_events() -> None:
    module = _load_named_script_module("ws_arb_soak")

    summary = module._summarize_log_lines(
        [
            "[info] exchange_source_mode exchange=kalshi mode=watch reason=subscription_start",
            "[info] exchange_mode exchange=kalshi mode=healthy reason=stream_ok",
            "[debug] heartbeat active_streams=4 quote_count=3 stale_quote_count=1 "
            "clients=['kalshi'] exchange_source_modes={'kalshi': 'watch'} "
            "exchange_modes={'kalshi': 'healthy'} exchange_recovery_reasons={} "
            "exchange_subscription_counts={'kalshi': 4} exchange_active_counts={'kalshi': 4} "
            "exchange_pending_counts={'kalshi': 0} exchange_priority_counts={'kalshi': 0} "
            "exchange_queue_depth_counts={'kalshi': 0} exchange_dispatch_drop_counts={'kalshi': 0} "
            "last_eval_duration_ms=0.100 eval_age_s=0.100 last_eval_due_event_count=0 "
            "last_eval_emitted_count=0 eval_count=3 evals_per_min=18.000 quote_update_count=3 "
            "quote_updates_since_last_eval=0 last_quote_update_age_s=0.100 "
            "quote_updates_per_min=18.000 exchange_update_counts={'kalshi': 3} "
            "exchange_updates_per_min={'kalshi': 18.0} "
            "exchange_last_ok_age_p95_s={'kalshi': 16.0} "
            "exchange_last_ok_age_max_s={'kalshi': 16.0} "
            "exchange_latency_p95_ms={'kalshi': 1800.0} "
            "exchange_latency_max_ms={'kalshi': 1800.0} "
            "quote_seen_to_alert_sample_count={'kalshi': 2, 'overall': 2} "
            "quote_seen_to_alert_p50_ms={'kalshi': 12.0, 'overall': 12.0} "
            "quote_seen_to_alert_p95_ms={'kalshi': 20.0, 'overall': 20.0} "
            "quote_seen_to_alert_p99_ms={'kalshi': 20.0, 'overall': 20.0} "
            "book_timestamp_to_alert_sample_count={'kalshi': 2, 'overall': 2} "
            "book_timestamp_to_alert_p50_ms={'kalshi': 450.0, 'overall': 450.0} "
            "book_timestamp_to_alert_p95_ms={'kalshi': 800.0, 'overall': 800.0} "
            "book_timestamp_to_alert_p99_ms={'kalshi': 800.0, 'overall': 800.0} "
            "exchange_update_to_store_sample_count={'kalshi': 2, 'overall': 2} "
            "exchange_update_to_store_p50_ms={'kalshi': 200.0, 'overall': 200.0} "
            "exchange_update_to_store_p95_ms={'kalshi': 350.0, 'overall': 350.0} "
            "exchange_update_to_store_p99_ms={'kalshi': 350.0, 'overall': 350.0} "
            "store_to_alert_sample_count={'kalshi': 2, 'overall': 2} "
            "store_to_alert_p50_ms={'kalshi': 25.0, 'overall': 25.0} "
            "store_to_alert_p95_ms={'kalshi': 40.0, 'overall': 40.0} "
            "store_to_alert_p99_ms={'kalshi': 40.0, 'overall': 40.0} "
            "backoffs={} retry_in={'kalshi': 0.0} "
            "rate_limit_counts={'kalshi': 0} sidecar_error_counts={} auth_error_counts={}",
            "[debug] stream exchange=kalshi outcome_id=KX-1 ok=1 err=0 last_latency_ms=1800.0 last_ok_age_s=1.5",
            "[debug] stream exchange=kalshi outcome_id=KX-2 ok=1 err=0 last_latency_ms=1800.0 last_ok_age_s=3.0",
            "[debug] stream exchange=kalshi outcome_id=KX-3 ok=1 err=0 last_latency_ms=1800.0 last_ok_age_s=16.0",
            "[debug] stream exchange=kalshi outcome_id=KX-4 ok=0 err=0 last_latency_ms=na last_ok_age_s=na",
        ],
        stale_after_seconds=15.0,
    )

    assert summary["heartbeat_count"] == 1
    assert summary["quote_count_min"] == 3
    assert summary["quote_count_max"] == 3
    assert summary["stale_quote_count_max"] == 1
    assert summary["stream_age_p95_s_max"] == 16.0
    assert summary["stale_stream_count_max"] == 1
    assert summary["never_ok_stream_count_max"] == 1
    assert summary["stale_after_success_stream_count_max"] == 1
    assert summary["per_exchange_tail_max"] == {
        "kalshi": {
            "fresh_after_success_count_max": 2,
            "never_ok_count_max": 1,
            "stale_after_success_count_max": 1,
            "stream_count_max": 4,
        }
    }
    assert summary["exchange_updates_per_min_max"] == {"kalshi": 18.0}
    assert summary["exchange_last_ok_age_p95_s_max"] == {"kalshi": 16.0}
    assert summary["exchange_last_ok_age_max_s_max"] == {"kalshi": 16.0}
    assert summary["exchange_latency_p95_ms_max"] == {"kalshi": 1800.0}
    assert summary["exchange_latency_max_ms_max"] == {"kalshi": 1800.0}
    assert summary["competitive_latency"]["quote_seen_to_alert_ms"]["sample_count"] == {
        "kalshi": 2,
        "overall": 2,
    }
    assert summary["competitive_latency"]["quote_seen_to_alert_ms"]["p95_ms"] == {
        "kalshi": 20.0,
        "overall": 20.0,
    }
    assert summary["competitive_latency"]["book_timestamp_to_alert_ms"]["p50_ms"] == {
        "kalshi": 450.0,
        "overall": 450.0,
    }
    assert summary["exchange_mode_events"] == {"kalshi:healthy": 1}
    assert summary["exchange_source_mode_events"] == {"kalshi:watch": 1}


def test_benchmark_summary_reports_competitive_latency() -> None:
    module = _load_named_script_module("ws_arb_benchmark")

    summary = module._summarize(
        [
            (
                0.1,
                "[debug] heartbeat active_streams=2 quote_count=2 stale_quote_count=0 "
                "clients=['kalshi'] exchange_source_modes={'kalshi': 'watch'} "
                "exchange_modes={'kalshi': 'healthy'} exchange_recovery_reasons={} "
                "exchange_subscription_counts={'kalshi': 2} exchange_active_counts={'kalshi': 2} "
                "exchange_pending_counts={'kalshi': 0} exchange_priority_counts={'kalshi': 0} "
                "exchange_queue_depth_counts={'kalshi': 0} exchange_dispatch_drop_counts={'kalshi': 0} "
                "last_eval_duration_ms=0.200 eval_age_s=0.100 last_eval_mapping_count=1 "
                "last_eval_raw_event_count=1 last_eval_due_event_count=1 "
                "last_eval_emitted_count=1 eval_count=1 evals_per_min=10.000 "
                "quote_update_count=1 quote_updates_since_last_eval=0 last_quote_update_age_s=0.100 "
                "quote_updates_per_min=10.000 exchange_update_counts={'kalshi': 1} "
                "exchange_updates_per_min={'kalshi': 10.0} "
                "exchange_last_ok_age_p95_s={'kalshi': 1.0} "
                "exchange_last_ok_age_max_s={'kalshi': 1.0} "
                "exchange_latency_p95_ms={'kalshi': 5.0} exchange_latency_max_ms={'kalshi': 5.0} "
                "quote_seen_to_alert_sample_count={'kalshi': 1, 'kalshi:watch': 1, 'overall': 1} "
                "quote_seen_to_alert_p50_ms={'kalshi': 12.0, 'kalshi:watch': 12.0, 'overall': 12.0} "
                "quote_seen_to_alert_p95_ms={'kalshi': 12.0, 'kalshi:watch': 12.0, 'overall': 12.0} "
                "quote_seen_to_alert_p99_ms={'kalshi': 12.0, 'kalshi:watch': 12.0, 'overall': 12.0} "
                "book_timestamp_to_alert_sample_count={'kalshi': 1, 'overall': 1} "
                "book_timestamp_to_alert_p50_ms={'kalshi': 450.0, 'overall': 450.0} "
                "book_timestamp_to_alert_p95_ms={'kalshi': 450.0, 'overall': 450.0} "
                "book_timestamp_to_alert_p99_ms={'kalshi': 450.0, 'overall': 450.0} "
                "exchange_update_to_store_sample_count={'kalshi': 1, 'overall': 1} "
                "exchange_update_to_store_p50_ms={'kalshi': 200.0, 'overall': 200.0} "
                "exchange_update_to_store_p95_ms={'kalshi': 200.0, 'overall': 200.0} "
                "exchange_update_to_store_p99_ms={'kalshi': 200.0, 'overall': 200.0} "
                "store_to_alert_sample_count={'kalshi': 1, 'overall': 1} "
                "store_to_alert_p50_ms={'kalshi': 25.0, 'overall': 25.0} "
                "store_to_alert_p95_ms={'kalshi': 25.0, 'overall': 25.0} "
                "store_to_alert_p99_ms={'kalshi': 25.0, 'overall': 25.0} "
                "backoffs={} retry_in={'kalshi': 0.0} rate_limit_counts={'kalshi': 0} "
                "sidecar_error_counts={} auth_error_counts={}"
            )
        ]
    )

    assert summary["competitive_latency"]["quote_seen_to_alert_ms"]["p50_ms"] == {
        "kalshi": 12.0,
        "kalshi:watch": 12.0,
        "overall": 12.0,
    }
    assert summary["competitive_latency"]["store_to_alert_ms"]["sample_count"] == {
        "kalshi": 1,
        "overall": 1,
    }
