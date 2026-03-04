#!/usr/bin/env python3
"""Minimal PMXT health check for local development."""

from __future__ import annotations

import sys
import traceback

import pmxt
import urllib3


def _check_exchange(name: str, exchange: object) -> bool:
    try:
        markets = exchange.fetch_markets(query="trump", limit=1)
        print(f"[ok] {name}: fetched {len(markets)} market(s)")
        return True
    except Exception as exc:  # pragma: no cover - operational error visibility
        print(f"[error] {name}: {exc}")
        traceback.print_exc()
        return False


def main() -> int:
    print(f"[info] python={sys.version.split()[0]} urllib3={urllib3.__version__}")
    if int(urllib3.__version__.split(".", maxsplit=1)[0]) < 2:
        print("[error] urllib3<2 detected; PMXT fetch may fail with PoolKey key_ca_cert_data.")
        return 1

    checks = [
        _check_exchange("polymarket", pmxt.Polymarket()),
        _check_exchange("kalshi", pmxt.Kalshi()),
    ]
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
