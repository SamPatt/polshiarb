from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import threading
import time
from types import ModuleType


def _load_script_module() -> ModuleType:
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "ws_arb_alerts.py"
    spec = importlib.util.spec_from_file_location("ws_arb_alerts_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_script_emits_cross_within_and_deviation_alert_tags(monkeypatch) -> None:
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
    )
    runner = module.ArbAlertRunner(args)
    lines: list[str] = []
    runner._print_line = lambda line: lines.append(line)  # type: ignore[method-assign]

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
