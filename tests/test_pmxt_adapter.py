from __future__ import annotations

from app.pmxt_adapter import PMXTAdapter


class FakeKalshiClient:
    def fetch_event(self, slug: str) -> dict:
        assert slug == "kxsenateild-26"
        return {
            "id": "kalshi-event-1",
            "slug": slug,
            "title": "Illinois Democratic Senate nominee?",
            "description": "Resolves to the candidate who wins the nomination.",
            "url": "https://kalshi.com/events/KXSENATEILD-26",
            "markets": [
                {
                    "market_id": "KXSENATEILD-26-DDUR",
                    "title": "Illinois Democratic Senate nominee?",
                    "description": "Resolves YES if Dick Durbin wins nomination.",
                    "resolution_date": "2026-03-17",
                    "url": "https://kalshi.com/events/KXSENATEILD-26",
                    "outcomes": [
                        {"outcome_id": "DDUR", "label": "Dick Durbin", "price": 0.01, "market_id": "KXSENATEILD-26-DDUR"},
                        {"outcome_id": "NOT-DDUR", "label": "Not Dick Durbin", "price": 0.99, "market_id": "KXSENATEILD-26-DDUR"},
                    ],
                    "yes": {"outcome_id": "DDUR", "label": "Dick Durbin", "price": 0.01, "market_id": "KXSENATEILD-26-DDUR"},
                    "no": {"outcome_id": "NOT-DDUR", "label": "Not Dick Durbin", "price": 0.99, "market_id": "KXSENATEILD-26-DDUR"},
                },
                {
                    "market_id": "KXSENATEILD-26-RKEL",
                    "title": "Illinois Democratic Senate nominee?",
                    "description": "Resolves YES if Robin Kelly wins nomination.",
                    "resolution_date": "2026-03-17",
                    "url": "https://kalshi.com/events/KXSENATEILD-26",
                    "outcomes": [
                        {"outcome_id": "RKEL", "label": "Robin Kelly", "price": 0.05, "market_id": "KXSENATEILD-26-RKEL"},
                        {"outcome_id": "NOT-RKEL", "label": "Not Robin Kelly", "price": 0.95, "market_id": "KXSENATEILD-26-RKEL"},
                    ],
                    "yes": {"outcome_id": "RKEL", "label": "Robin Kelly", "price": 0.05, "market_id": "KXSENATEILD-26-RKEL"},
                    "no": {"outcome_id": "NOT-RKEL", "label": "Not Robin Kelly", "price": 0.95, "market_id": "KXSENATEILD-26-RKEL"},
                },
            ],
        }

    def fetch_market(self, slug: str) -> dict:
        assert slug == "kxsenateild-26"
        return {
            "market_id": "kalshi-1",
            "title": "Illinois Democratic Senate nominee?",
            "description": "Resolves YES if candidate wins nomination.",
            "resolution_date": "2026-11-03",
            "url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
            "outcomes": [
                {"outcome_id": "YES", "label": "YES", "price": 0.51, "market_id": "kalshi-1"},
                {"outcome_id": "NO", "label": "NO", "price": 0.49, "market_id": "kalshi-1"},
            ],
            "yes": {"outcome_id": "YES", "label": "YES", "price": 0.51, "market_id": "kalshi-1"},
            "no": {"outcome_id": "NO", "label": "NO", "price": 0.49, "market_id": "kalshi-1"},
            "volume_24h": 1000,
            "volume": 25000,
            "liquidity": 5000,
            "open_interest": 12000,
        }


class FakePolymarketClient:
    def fetch_event(self, slug: str) -> dict:
        assert slug == "illinois-democratic-senate-primary-winner"
        return {
            "id": "poly-event-1",
            "slug": slug,
            "title": "Illinois Democratic Senate Primary Winner",
            "description": "Resolves to the official nominee.",
            "url": f"https://polymarket.com/event/{slug}",
            "markets": [
                {
                    "market_id": "poly-market-1",
                    "title": "Will candidate X be nominee?",
                    "description": "Resolves YES if candidate X wins.",
                    "resolution_date": "2026-03-17",
                    "url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner",
                    "outcomes": [
                        {"outcome_id": "token-yes", "label": "Yes", "price": 0.2, "market_id": "poly-market-1"},
                        {"outcome_id": "token-no", "label": "No", "price": 0.8, "market_id": "poly-market-1"},
                    ],
                    "yes": {"outcome_id": "token-yes", "label": "Yes", "price": 0.2, "market_id": "poly-market-1"},
                    "no": {"outcome_id": "token-no", "label": "No", "price": 0.8, "market_id": "poly-market-1"},
                }
            ],
        }


class AnyPolymarketClient:
    def fetch_event(self, slug: str) -> dict:
        return {
            "id": "poly-event-any",
            "slug": slug,
            "title": "Any Event",
            "description": "placeholder",
            "url": f"https://polymarket.com/event/{slug}",
            "markets": [],
        }


class FakeKalshiSportsClient:
    def __init__(self) -> None:
        self.load_markets_called = False
        self.markets = {
            "KXNBAGAME-26MAR03NOPLAL-LAL": {
                "market_id": "KXNBAGAME-26MAR03NOPLAL-LAL",
                "title": "New Orleans at Los Angeles L",
                "description": "Winner market",
                "url": "https://kalshi.com/events/KXNBAGAME-26MAR03NOPLAL",
                "outcomes": [
                    {"outcome_id": "LAL", "label": "Los Angeles L", "price": 0.82},
                    {"outcome_id": "NOP", "label": "New Orleans", "price": 0.18},
                ],
                "yes": {"outcome_id": "LAL", "label": "Los Angeles L", "price": 0.82},
                "no": {"outcome_id": "NOP", "label": "New Orleans", "price": 0.18},
            },
            "KXNBASPREAD-26MAR03NOPLAL-LAL8": {
                "market_id": "KXNBASPREAD-26MAR03NOPLAL-LAL8",
                "title": "New Orleans at Los Angeles L: Spread",
                "description": "Spread market",
                "url": "https://kalshi.com/events/KXNBASPREAD-26MAR03NOPLAL",
                "outcomes": [
                    {"outcome_id": "LAL8", "label": "Los Angeles L wins by over 8.5", "price": 0.31},
                    {"outcome_id": "NOT-LAL8", "label": "No", "price": 0.69},
                ],
                "yes": {"outcome_id": "LAL8", "label": "Los Angeles L wins by over 8.5", "price": 0.31},
                "no": {"outcome_id": "NOT-LAL8", "label": "No", "price": 0.69},
            },
            "KXNBATOTAL-26MAR03NOPLAL-242": {
                "market_id": "KXNBATOTAL-26MAR03NOPLAL-242",
                "title": "New Orleans at Los Angeles L: Total Points",
                "description": "Total market",
                "url": "https://kalshi.com/events/KXNBATOTAL-26MAR03NOPLAL",
                "outcomes": [
                    {"outcome_id": "OVER242", "label": "Over 242.5", "price": 0.44},
                    {"outcome_id": "UNDER242", "label": "Under 242.5", "price": 0.56},
                ],
                "yes": {"outcome_id": "OVER242", "label": "Over 242.5", "price": 0.44},
                "no": {"outcome_id": "UNDER242", "label": "Under 242.5", "price": 0.56},
            },
            "KXNBAPTS-26MAR03NOPLAL-LALLJAMES23-25": {
                "market_id": "KXNBAPTS-26MAR03NOPLAL-LALLJAMES23-25",
                "title": "New Orleans at Los Angeles L: Points",
                "description": "Player props market",
                "url": "https://kalshi.com/events/KXNBAPTS-26MAR03NOPLAL",
                "outcomes": [
                    {"outcome_id": "LEBRON25", "label": "LeBron James over 25.5", "price": 0.51},
                    {"outcome_id": "NOT-LEBRON25", "label": "No", "price": 0.49},
                ],
                "yes": {"outcome_id": "LEBRON25", "label": "LeBron James over 25.5", "price": 0.51},
                "no": {"outcome_id": "NOT-LEBRON25", "label": "No", "price": 0.49},
            },
            "KXNBAGAME-26MAR05PHXSAC-PHX": {
                "market_id": "KXNBAGAME-26MAR05PHXSAC-PHX",
                "title": "Phoenix at Sacramento",
                "description": "Unrelated game",
                "url": "https://kalshi.com/events/KXNBAGAME-26MAR05PHXSAC",
                "outcomes": [
                    {"outcome_id": "PHX", "label": "Phoenix", "price": 0.5},
                    {"outcome_id": "SAC", "label": "Sacramento", "price": 0.5},
                ],
                "yes": {"outcome_id": "PHX", "label": "Phoenix", "price": 0.5},
                "no": {"outcome_id": "SAC", "label": "Sacramento", "price": 0.5},
            },
        }

    def fetch_event(self, slug: str) -> dict:
        assert slug == "kxnbagame-26mar03noplal"
        return {
            "id": "KXNBAGAME-26MAR03NOPLAL",
            "slug": "KXNBAGAME-26MAR03NOPLAL",
            "title": "New Orleans at Los Angeles L",
            "description": "Base event",
            "url": "https://kalshi.com/events/KXNBAGAME-26MAR03NOPLAL",
            "markets": [
                {
                    "market_id": "KXNBAGAME-26MAR03NOPLAL-LAL",
                    "title": "New Orleans at Los Angeles L",
                    "description": "Winner market",
                    "url": "https://kalshi.com/events/KXNBAGAME-26MAR03NOPLAL",
                    "outcomes": [
                        {"outcome_id": "LAL", "label": "Los Angeles L", "price": 0.82},
                        {"outcome_id": "NOP", "label": "New Orleans", "price": 0.18},
                    ],
                    "yes": {"outcome_id": "LAL", "label": "Los Angeles L", "price": 0.82},
                    "no": {"outcome_id": "NOP", "label": "New Orleans", "price": 0.18},
                },
            ],
        }

    def load_markets(self, reload: bool = False) -> None:
        self.load_markets_called = True

    def fetch_market(self, slug: str) -> dict:
        raise AssertionError("fetch_market fallback should not be used in this test")


def test_preview_from_normalized_returns_market_and_event_payloads() -> None:
    adapter = PMXTAdapter(
        polymarket_client=FakePolymarketClient(),
        kalshi_client=FakeKalshiClient(),
    )

    normalized = {
        "kalshi": {
            "exchange": "kalshi",
            "lookup_type": "market_ticker",
            "lookup_value": "kxsenateild-26",
            "canonical_url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
            "original_url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
        },
        "polymarket": {
            "exchange": "polymarket",
            "lookup_type": "event_slug",
            "lookup_value": "illinois-democratic-senate-primary-winner",
            "canonical_url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner",
            "original_url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner",
        },
    }

    preview = adapter.preview_from_normalized(normalized)

    assert preview["kalshi"]["entity_type"] == "event"
    assert preview["kalshi"]["event"]["event_id"] == "kalshi-event-1"
    assert len(preview["kalshi"]["event"]["markets"]) == 2
    assert (
        preview["kalshi"]["event"]["markets"][0]["possible_outcomes"]
        == ["Dick Durbin", "Not Dick Durbin"]
    )

    assert preview["polymarket"]["entity_type"] == "event"
    assert preview["polymarket"]["event"]["event_id"] == "poly-event-1"
    assert len(preview["polymarket"]["event"]["markets"]) == 1


def test_kalshi_sports_preview_expands_related_matchup_markets() -> None:
    kalshi_client = FakeKalshiSportsClient()
    adapter = PMXTAdapter(
        polymarket_client=AnyPolymarketClient(),
        kalshi_client=kalshi_client,
    )

    normalized = {
        "kalshi": {
            "exchange": "kalshi",
            "lookup_type": "market_ticker",
            "lookup_value": "kxnbagame-26mar03noplal",
            "canonical_url": "https://kalshi.com/markets/kxnbagame/professional-basketball-game/kxnbagame-26mar03noplal",
            "original_url": "https://kalshi.com/markets/kxnbagame/professional-basketball-game/kxnbagame-26mar03noplal",
        },
        "polymarket": {
            "exchange": "polymarket",
            "lookup_type": "event_slug",
            "lookup_value": "nba-nop-lal-2026-03-03",
            "canonical_url": "https://polymarket.com/sports/nba/nba-nop-lal-2026-03-03",
            "original_url": "https://polymarket.com/sports/nba/nba-nop-lal-2026-03-03",
        },
    }

    preview = adapter.preview_from_normalized(normalized)
    kalshi_payload = preview["kalshi"]
    assert kalshi_payload["entity_type"] == "event"

    market_ids = {market["market_id"] for market in kalshi_payload["event"]["markets"]}
    assert "KXNBAGAME-26MAR03NOPLAL-LAL" in market_ids
    assert "KXNBASPREAD-26MAR03NOPLAL-LAL8" in market_ids
    assert "KXNBATOTAL-26MAR03NOPLAL-242" in market_ids
    assert "KXNBAPTS-26MAR03NOPLAL-LALLJAMES23-25" in market_ids
    assert "KXNBAGAME-26MAR05PHXSAC-PHX" not in market_ids
    assert kalshi_client.load_markets_called is True
