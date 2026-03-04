from __future__ import annotations

from pathlib import Path

import app.main as main_module
from app.main import PreviewPairRequest, SavePairRequest
from app.pmxt_adapter import PMXTAdapterError


def _sample_save_payload(active: bool = True) -> dict:
    return {
        "kalshi_url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
        "polymarket_url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner",
        "normalized": {
            "kalshi": {"lookup_value": "kxsenateild-26"},
            "polymarket": {"lookup_value": "illinois-democratic-senate-primary-winner"},
        },
        "preview": {
            "kalshi": {
                "entity_type": "event",
                "event": {
                    "event_id": "KXSENATEILD-26",
                    "markets": [
                        {
                            "market_id": "KXSENATEILD-26-DDUR",
                            "title": "Illinois Democratic Senate nominee?",
                            "url": "https://kalshi.com/events/KXSENATEILD-26",
                            "resolution_rules": "rules",
                            "yes": {"outcome_id": "DDUR", "label": "Dick Durbin", "price": 0.1},
                            "no": {"outcome_id": "NOT-DDUR", "label": "Not Dick Durbin", "price": 0.9},
                            "outcomes": [
                                {"outcome_id": "DDUR", "label": "Dick Durbin", "price": 0.1},
                                {"outcome_id": "NOT-DDUR", "label": "Not Dick Durbin", "price": 0.9},
                            ],
                        }
                    ],
                },
            },
            "polymarket": {
                "entity_type": "event",
                "event": {
                    "event_id": "poly-1",
                    "markets": [
                        {
                            "market_id": "poly-market-1",
                            "title": "Will candidate X be nominee?",
                            "url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner",
                            "resolution_rules": "rules",
                            "yes": {"outcome_id": "token-yes", "label": "Yes", "price": 0.2},
                            "no": {"outcome_id": "token-no", "label": "No", "price": 0.8},
                            "outcomes": [
                                {"outcome_id": "token-yes", "label": "Yes", "price": 0.2},
                                {"outcome_id": "token-no", "label": "No", "price": 0.8},
                            ],
                        }
                    ],
                },
            },
        },
        "matches": [
            {
                "kalshi_market_id": "KXSENATEILD-26-DDUR",
                "polymarket_market_id": "poly-market-1",
                "relation_type": "same_direction",
                "active": active,
            }
        ],
        "recurrence_intent": "weekly",
        "expires_at": "2099-01-01",
    }


def test_api_save_edit_list_delete_flow(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "api_flow.db"
    monkeypatch.setattr(main_module, "DEFAULT_DB_PATH", db_path)

    create_payload = SavePairRequest(**_sample_save_payload(active=True))
    created = main_module.create_pair(create_payload)
    assert created["ok"] is True
    pair_id = int(created["pair_id"])

    listed_all = main_module.list_pairs(status="all").model_dump()
    assert listed_all["ok"] is True
    assert len(listed_all["pairs"]) == 1
    assert listed_all["pairs"][0]["id"] == pair_id

    fetched = main_module.get_pair(pair_id=pair_id)
    assert fetched["ok"] is True
    assert fetched["pair"]["pair_set"]["id"] == pair_id

    update_data = _sample_save_payload(active=False)
    update_data["recurrence_intent"] = "daily"
    update_payload = SavePairRequest(**update_data)
    updated = main_module.put_pair(pair_id=pair_id, payload=update_payload)
    assert updated["ok"] is True
    assert updated["pair_id"] == pair_id

    listed_active = main_module.list_pairs(status="active").model_dump()
    assert listed_active["ok"] is True
    assert len(listed_active["pairs"]) == 0

    deleted = main_module.remove_pair(pair_id=pair_id)
    assert deleted["ok"] is True

    missing = main_module.get_pair(pair_id=pair_id)
    assert missing["ok"] is False
    assert missing["error_code"] == "PAIR_NOT_FOUND"


def test_api_validation_and_filter_errors_have_codes() -> None:
    normalized = main_module.normalize_pair(
        payload=main_module.NormalizePairRequest(
            kalshi_url="https://example.com/bad-kalshi",
            polymarket_url="https://polymarket.com/event/test",
        )
    )
    assert normalized["ok"] is False
    assert normalized["error_code"] == "INVALID_URL"

    preview = main_module.preview_pair(
        payload=PreviewPairRequest(
            kalshi_url="https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
            polymarket_url="https://example.com/bad-polymarket",
        )
    )
    assert preview["ok"] is False
    assert preview["error_code"] == "INVALID_URL"

    invalid_list = main_module.list_pairs(status="oops").model_dump()
    assert invalid_list["ok"] is False
    assert invalid_list["error_code"] == "INVALID_STATUS_FILTER"


def test_preview_pmxt_failure_returns_predictable_error(monkeypatch) -> None:
    class FailingAdapter:
        def preview_from_normalized(self, normalized):  # noqa: ANN001
            raise PMXTAdapterError("network unreachable")

    monkeypatch.setattr(main_module, "get_pmxt_adapter", lambda: FailingAdapter())
    payload = PreviewPairRequest(
        kalshi_url="https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
        polymarket_url="https://polymarket.com/event/illinois-democratic-senate-primary-winner",
    )
    response = main_module.preview_pair(payload=payload)

    assert response["ok"] is False
    assert response["error_code"] == "PMXT_PREVIEW_FAILED"
    assert "Preview fetch failed via PMXT" in response["error"]
