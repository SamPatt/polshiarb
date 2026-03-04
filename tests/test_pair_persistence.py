from __future__ import annotations

from pathlib import Path

from app.db import delete_pair_set, init_db, list_pair_sets, load_pair_set, save_pair_set, update_pair_set


def _sample_payload() -> dict:
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
                            "yes": {
                                "outcome_id": "DDUR",
                                "label": "Dick Durbin",
                                "price": 0.1,
                            },
                            "no": {
                                "outcome_id": "NOT-DDUR",
                                "label": "Not Dick Durbin",
                                "price": 0.9,
                            },
                            "outcomes": [
                                {
                                    "outcome_id": "DDUR",
                                    "label": "Dick Durbin",
                                    "price": 0.1,
                                },
                                {
                                    "outcome_id": "NOT-DDUR",
                                    "label": "Not Dick Durbin",
                                    "price": 0.9,
                                },
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
                            "yes": {
                                "outcome_id": "token-yes",
                                "label": "Yes",
                                "price": 0.2,
                            },
                            "no": {
                                "outcome_id": "token-no",
                                "label": "No",
                                "price": 0.8,
                            },
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
            }
        ],
        "recurrence_intent": "weekly",
        "expires_at": "2026-12-31",
    }


def test_save_and_load_pair_set(tmp_path: Path) -> None:
    db_path = tmp_path / "pairs.db"
    init_db(db_path)

    save_result = save_pair_set(db_path, _sample_payload())
    assert save_result["pair_id"] > 0
    # YES->YES and NO->NO rows for same_direction.
    assert save_result["saved_link_rows"] == 2

    loaded = load_pair_set(db_path, save_result["pair_id"])
    assert loaded is not None
    assert loaded["pair_set"]["recurrence_intent"] == "weekly"
    assert loaded["pair_set"]["expires_at"] == "2026-12-31"
    assert len(loaded["markets"]["kalshi"]) == 1
    assert len(loaded["markets"]["polymarket"]) == 1
    assert len(loaded["outcome_links"]) == 2


def test_inverse_relation_creates_cross_outcome_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "pairs_inverse.db"
    payload = _sample_payload()
    payload["matches"][0]["relation_type"] = "inverse"

    save_result = save_pair_set(db_path, payload)
    loaded = load_pair_set(db_path, save_result["pair_id"])
    assert loaded is not None

    pairs = {
        (link["kalshi_outcome_id"], link["polymarket_outcome_id"])
        for link in loaded["outcome_links"]
    }
    assert ("DDUR", "token-no") in pairs
    assert ("NOT-DDUR", "token-yes") in pairs


def test_update_pair_set_rewrites_links_and_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "pairs_update.db"
    initial = save_pair_set(db_path, _sample_payload())

    update_payload = _sample_payload()
    update_payload["recurrence_intent"] = "daily"
    update_payload["expires_at"] = "2026-01-01"
    update_payload["matches"][0]["active"] = False

    update_result = update_pair_set(db_path, initial["pair_id"], update_payload)
    assert update_result is not None
    assert update_result["pair_id"] == initial["pair_id"]
    assert update_result["saved_link_rows"] == 2

    loaded = load_pair_set(db_path, initial["pair_id"])
    assert loaded is not None
    assert loaded["pair_set"]["recurrence_intent"] == "daily"
    assert loaded["pair_set"]["expires_at"] == "2026-01-01"
    assert all(link["active"] is False for link in loaded["outcome_links"])


def test_list_pair_sets_status_filtering(tmp_path: Path) -> None:
    db_path = tmp_path / "pairs_list.db"

    active_payload = _sample_payload()
    active_payload["expires_at"] = "2099-01-01"
    save_pair_set(db_path, active_payload)

    expired_payload = _sample_payload()
    expired_payload["expires_at"] = "2000-01-01"
    save_pair_set(db_path, expired_payload)

    inactive_payload = _sample_payload()
    inactive_payload["expires_at"] = "2099-01-01"
    inactive_payload["matches"][0]["active"] = False
    save_pair_set(db_path, inactive_payload)

    all_rows = list_pair_sets(db_path, "all")
    active_rows = list_pair_sets(db_path, "active")
    expired_rows = list_pair_sets(db_path, "expired")

    assert len(all_rows) == 3
    assert len(active_rows) == 1
    assert active_rows[0]["is_expired"] is False
    assert active_rows[0]["active_link_rows"] == 2
    assert len(expired_rows) == 1
    assert expired_rows[0]["is_expired"] is True


def test_delete_pair_set_removes_pair(tmp_path: Path) -> None:
    db_path = tmp_path / "pairs_delete.db"
    saved = save_pair_set(db_path, _sample_payload())

    assert delete_pair_set(db_path, saved["pair_id"]) is True
    assert delete_pair_set(db_path, saved["pair_id"]) is False
    assert load_pair_set(db_path, saved["pair_id"]) is None
