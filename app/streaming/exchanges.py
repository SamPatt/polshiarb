from __future__ import annotations

from typing import Any

import pmxt


def build_exchange(
    exchange: str,
    *,
    use_kalshi_sidecar_default: bool = True,
    kalshi_direct_kwargs: dict[str, str] | None = None,
) -> Any:
    if exchange == "kalshi":
        if use_kalshi_sidecar_default:
            # Use sidecar default exchange so PMXT reuses a single Kalshi WS client.
            return pmxt.Kalshi()
        kwargs = kalshi_direct_kwargs or {}
        return pmxt.Kalshi(**kwargs)
    if exchange == "polymarket":
        return pmxt.Polymarket()
    raise ValueError(f"Unsupported exchange: {exchange}")
