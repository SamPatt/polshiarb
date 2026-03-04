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
