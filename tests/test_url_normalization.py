from __future__ import annotations

import pytest

from app.url_normalization import (
    URLNormalizationError,
    normalize_pair_urls,
    parse_kalshi_url,
    parse_polymarket_url,
)


def test_parse_polymarket_event_url() -> None:
    url = "https://polymarket.com/event/illinois-democratic-senate-primary-winner"
    result = parse_polymarket_url(url)
    assert result.exchange == "polymarket"
    assert result.lookup_type == "event_slug"
    assert result.lookup_value == "illinois-democratic-senate-primary-winner"


def test_parse_polymarket_sports_url() -> None:
    url = "https://polymarket.com/sports/nba/nba-nop-lal-2026-03-03"
    result = parse_polymarket_url(url)
    assert result.exchange == "polymarket"
    assert result.lookup_type == "event_slug"
    assert result.lookup_value == "nba-nop-lal-2026-03-03"
    assert result.canonical_url == "https://polymarket.com/sports/nba/nba-nop-lal-2026-03-03"


def test_parse_kalshi_market_url() -> None:
    url = "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26"
    result = parse_kalshi_url(url)
    assert result.exchange == "kalshi"
    assert result.lookup_type == "market_ticker"
    assert result.lookup_value == "kxsenateild-26"


def test_normalize_pair_urls() -> None:
    normalized = normalize_pair_urls(
        kalshi_url="https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
        polymarket_url="https://polymarket.com/event/illinois-democratic-senate-primary-winner",
    )
    assert normalized["kalshi"]["lookup_value"] == "kxsenateild-26"
    assert (
        normalized["polymarket"]["lookup_value"]
        == "illinois-democratic-senate-primary-winner"
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/event/something",
        "https://polymarket.com/market/something",
        "https://polymarket.com/sports/nba",
    ],
)
def test_bad_polymarket_url_raises(url: str) -> None:
    with pytest.raises(URLNormalizationError):
        parse_polymarket_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/markets/foo/bar/baz",
        "https://kalshi.com/events/something",
    ],
)
def test_bad_kalshi_url_raises(url: str) -> None:
    with pytest.raises(URLNormalizationError):
        parse_kalshi_url(url)
