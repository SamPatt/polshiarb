from __future__ import annotations

from dataclasses import asdict, dataclass
from urllib.parse import urlparse


class URLNormalizationError(ValueError):
    """Raised when a user-provided exchange URL cannot be normalized."""


@dataclass(frozen=True)
class NormalizedURL:
    exchange: str
    original_url: str
    canonical_url: str
    lookup_type: str
    lookup_value: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _strip_slashes(path: str) -> list[str]:
    return [segment for segment in path.split("/") if segment]


def parse_polymarket_url(url: str) -> NormalizedURL:
    parsed = urlparse(url.strip())
    host = parsed.netloc.lower()
    segments = _strip_slashes(parsed.path)

    if host not in {"polymarket.com", "www.polymarket.com"}:
        raise URLNormalizationError("Polymarket URL must use polymarket.com")

    if len(segments) >= 2 and segments[0] == "event":
        slug = segments[1]
        return NormalizedURL(
            exchange="polymarket",
            original_url=url,
            canonical_url=f"https://polymarket.com/event/{slug}",
            lookup_type="event_slug",
            lookup_value=slug,
        )

    # Sports pages use /sports/{league}/{event-slug} but still map to an event slug.
    if len(segments) >= 3 and segments[0] == "sports":
        slug = segments[-1]
        return NormalizedURL(
            exchange="polymarket",
            original_url=url,
            canonical_url=f"https://polymarket.com/sports/{segments[1]}/{slug}",
            lookup_type="event_slug",
            lookup_value=slug,
        )

    raise URLNormalizationError(
        "Unsupported Polymarket URL format. Expected /event/{slug} or /sports/{league}/{slug}"
    )


def parse_kalshi_url(url: str) -> NormalizedURL:
    parsed = urlparse(url.strip())
    host = parsed.netloc.lower()
    segments = _strip_slashes(parsed.path)

    if host not in {"kalshi.com", "www.kalshi.com"}:
        raise URLNormalizationError("Kalshi URL must use kalshi.com")

    if len(segments) >= 4 and segments[0] == "markets":
        ticker = segments[3]
        return NormalizedURL(
            exchange="kalshi",
            original_url=url,
            canonical_url=f"https://kalshi.com/markets/{segments[1]}/{segments[2]}/{ticker}",
            lookup_type="market_ticker",
            lookup_value=ticker,
        )

    raise URLNormalizationError(
        "Unsupported Kalshi URL format. Expected /markets/{series}/{event}/{ticker}"
    )


def normalize_pair_urls(kalshi_url: str, polymarket_url: str) -> dict[str, dict[str, str]]:
    kalshi = parse_kalshi_url(kalshi_url)
    polymarket = parse_polymarket_url(polymarket_url)
    return {"kalshi": kalshi.to_dict(), "polymarket": polymarket.to_dict()}
