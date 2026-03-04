from __future__ import annotations

from pathlib import Path

import app.main as main_module
from app.db import init_db, save_pair_set


def _sample_payload(expires_at: str, active: bool = True) -> dict:
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
                            "no": {
                                "outcome_id": "NOT-DDUR",
                                "label": "Not Dick Durbin",
                                "price": 0.9,
                            },
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
        "expires_at": expires_at,
    }


def _seed_monitoring_data(db_path: Path) -> None:
    init_db(db_path)
    save_pair_set(db_path, _sample_payload(expires_at="2099-01-01", active=True))
    save_pair_set(db_path, _sample_payload(expires_at="2000-01-01", active=True))
    save_pair_set(db_path, _sample_payload(expires_at="2099-01-01", active=False))


def test_monitoring_pairs_contract_default_filters(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "monitoring_contract.db"
    _seed_monitoring_data(db_path)
    monkeypatch.setattr(main_module, "DEFAULT_DB_PATH", db_path)

    payload = main_module.get_monitoring_pairs(
        active_only=True, include_expired=False
    ).model_dump()
    assert payload["ok"] is True
    assert payload["active_only"] is True
    assert payload["include_expired"] is False
    assert set(payload.keys()) == {
        "ok",
        "pairs",
        "active_only",
        "include_expired",
        "error",
        "error_code",
    }
    assert payload["error"] is None
    assert payload["error_code"] is None

    # One non-expired active pair remains; each match creates YES/NO mappings.
    assert len(payload["pairs"]) == 1
    pair = payload["pairs"][0]
    assert set(pair.keys()) == {"pair_id", "recurrence_intent", "expires_at", "mappings"}
    assert pair["expires_at"] == "2099-01-01"
    assert len(pair["mappings"]) == 2

    mapping = pair["mappings"][0]
    assert set(mapping.keys()) == {"relation_type", "legs"}
    assert len(mapping["legs"]) == 2
    exchanges = {leg["exchange"] for leg in mapping["legs"]}
    assert exchanges == {"kalshi", "polymarket"}
    for leg in mapping["legs"]:
        assert set(leg.keys()) == {"exchange", "market_id", "outcome_id"}
        assert isinstance(leg["market_id"], str)


def test_monitoring_pairs_contract_with_flags(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "monitoring_contract_flags.db"
    _seed_monitoring_data(db_path)
    monkeypatch.setattr(main_module, "DEFAULT_DB_PATH", db_path)

    payload = main_module.get_monitoring_pairs(
        active_only=False, include_expired=True
    ).model_dump()
    assert payload["ok"] is True
    assert payload["active_only"] is False
    assert payload["include_expired"] is True
    # Latest pair wins for duplicated mappings across pair sets.
    assert len(payload["pairs"]) == 1
    assert payload["pairs"][0]["pair_id"] == 3
    assert all(len(pair["mappings"]) == 2 for pair in payload["pairs"])
