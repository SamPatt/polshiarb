from __future__ import annotations

from dataclasses import asdict, is_dataclass
import re
from typing import Any, Protocol

import pmxt


class PMXTAdapterError(RuntimeError):
    """Raised when market/event fetch or normalization fails."""


class ExchangeAdapter(Protocol):
    def preview_from_normalized(
        self, normalized: dict[str, dict[str, str]]
    ) -> dict[str, Any]:
        """Fetch and normalize exchange payloads from normalized URL lookups."""


def _to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if is_dataclass(value):
        return asdict(value)
    raise PMXTAdapterError(f"Unsupported payload type for normalization: {type(value)}")


def _normalize_outcome(raw_outcome: dict[str, Any] | None) -> dict[str, Any] | None:
    if not raw_outcome:
        return None
    outcome = _to_dict(raw_outcome)
    return {
        "outcome_id": outcome.get("outcome_id"),
        "label": outcome.get("label"),
        "price": outcome.get("price"),
        "price_change_24h": outcome.get("price_change_24h"),
        "metadata": outcome.get("metadata"),
        "market_id": outcome.get("market_id"),
    }


def _normalize_market(raw_market: dict[str, Any] | Any) -> dict[str, Any]:
    market = _to_dict(raw_market)
    outcomes = [
        normalized
        for outcome in market.get("outcomes", [])
        if (normalized := _normalize_outcome(_to_dict(outcome)))
    ]
    yes = _normalize_outcome(market.get("yes"))
    no = _normalize_outcome(market.get("no"))

    return {
        "market_id": market.get("market_id"),
        "title": market.get("title"),
        "url": market.get("url"),
        "description": market.get("description"),
        # PMXT unified model does not expose a separate rules field; use description.
        "resolution_rules": market.get("description"),
        "resolution_date": market.get("resolution_date"),
        "outcomes": outcomes,
        "possible_outcomes": [o["label"] for o in outcomes if o.get("label")],
        "yes": yes,
        "no": no,
        "stats": {
            "volume_24h": market.get("volume_24h"),
            "volume": market.get("volume"),
            "liquidity": market.get("liquidity"),
            "open_interest": market.get("open_interest"),
        },
    }


def _normalize_event(raw_event: dict[str, Any] | Any) -> dict[str, Any]:
    event = _to_dict(raw_event)
    markets = [_normalize_market(market) for market in event.get("markets", [])]
    return {
        "event_id": event.get("id"),
        "slug": event.get("slug"),
        "title": event.get("title"),
        "url": event.get("url"),
        "description": event.get("description"),
        # PMXT unified model does not expose a separate rules field; use description.
        "resolution_rules": event.get("description"),
        "markets": markets,
    }


class PMXTAdapter:
    def __init__(self, polymarket_client: Any | None = None, kalshi_client: Any | None = None) -> None:
        self.polymarket = polymarket_client or pmxt.Polymarket()
        self.kalshi = kalshi_client or pmxt.Kalshi()
        self._kalshi_matchup_cache: dict[str, list[dict[str, Any]]] = {}

    def preview_from_normalized(
        self, normalized: dict[str, dict[str, str]]
    ) -> dict[str, Any]:
        return {
            "kalshi": self._fetch_kalshi(normalized["kalshi"]),
            "polymarket": self._fetch_polymarket(normalized["polymarket"]),
        }

    def _fetch_kalshi(self, lookup: dict[str, str]) -> dict[str, Any]:
        if lookup.get("lookup_type") != "market_ticker":
            raise PMXTAdapterError(
                f"Unsupported Kalshi lookup type: {lookup.get('lookup_type')}"
            )

        ticker = lookup["lookup_value"]
        # Kalshi URLs like /markets/{series}/{event}/{ticker} often represent an event family.
        # Prefer event fetch to get the full candidate/contract stack; fallback to single market.
        try:
            event = self.kalshi.fetch_event(slug=ticker)
            event_dict = _to_dict(event)
            matchup_key = self._extract_matchup_key(event_dict.get("id") or ticker)
            if matchup_key:
                expanded = self._expand_kalshi_matchup_markets(matchup_key)
                if expanded:
                    event_dict["markets"] = expanded
            return {
                "entity_type": "event",
                "lookup": lookup,
                "event": _normalize_event(event_dict),
            }
        except Exception:
            pass

        try:
            market = self.kalshi.fetch_market(slug=ticker)
            return {
                "entity_type": "market",
                "lookup": lookup,
                "market": _normalize_market(market),
            }
        except Exception as exc:  # pragma: no cover - network/runtime path
            raise PMXTAdapterError(
                f"Kalshi fetch failed for ticker/event '{ticker}': {exc}"
            ) from exc

    def _extract_matchup_key(self, value: str | None) -> str | None:
        if not value:
            return None
        parts = value.upper().split("-", maxsplit=1)
        if len(parts) != 2:
            return None

        candidate = parts[1]
        # Sports matchup keys look like 26MAR03NOPLAL.
        if re.fullmatch(r"\d{2}[A-Z]{3}\d{2}[A-Z0-9]+", candidate):
            return candidate
        return None

    def _expand_kalshi_matchup_markets(self, matchup_key: str) -> list[dict[str, Any]]:
        if matchup_key in self._kalshi_matchup_cache:
            return self._kalshi_matchup_cache[matchup_key]

        try:
            self.kalshi.load_markets(reload=False)
        except TypeError:
            self.kalshi.load_markets(False)
        except Exception:
            return []

        all_markets = list(getattr(self.kalshi, "markets", {}).values())
        selected: dict[str, dict[str, Any]] = {}
        marker = f"-{matchup_key}-"
        suffix = f"-{matchup_key}"

        for market in all_markets:
            raw = _to_dict(market)
            market_id = (raw.get("market_id") or "").upper()
            if not market_id:
                continue
            if marker in market_id or market_id.endswith(suffix):
                selected[market_id] = raw

        expanded = list(selected.values())
        expanded.sort(key=lambda m: (m.get("title") or "", m.get("market_id") or ""))
        self._kalshi_matchup_cache[matchup_key] = expanded
        return expanded

    def _fetch_polymarket(self, lookup: dict[str, str]) -> dict[str, Any]:
        if lookup.get("lookup_type") != "event_slug":
            raise PMXTAdapterError(
                f"Unsupported Polymarket lookup type: {lookup.get('lookup_type')}"
            )

        slug = lookup["lookup_value"]
        try:
            event = self.polymarket.fetch_event(slug=slug)
        except Exception as exc:  # pragma: no cover - network/runtime path
            raise PMXTAdapterError(
                f"Polymarket event fetch failed for slug '{slug}': {exc}"
            ) from exc

        return {
            "entity_type": "event",
            "lookup": lookup,
            "event": _normalize_event(event),
        }
