#!/usr/bin/env python3
"""Continuously monitor mapped Kalshi/Polymarket outcomes and print arb alerts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import urllib3

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.streaming.exchanges import build_exchange as _build_exchange
from app.streaming.manager import ArbAlertRunner as _BaseArbAlertRunner


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Watch mapped Kalshi + Polymarket orderbooks and print top-of-book "
            "arb/deviation alerts."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8011",
        help="Pair manager API base URL (default: http://127.0.0.1:8011)",
    )
    parser.add_argument(
        "--arb-threshold",
        type=float,
        default=0.02,
        help="Minimum YES+NO edge to alert (default: 0.02).",
    )
    parser.add_argument(
        "--deviation-threshold",
        type=float,
        default=0.03,
        help="Minimum same-token cross-market ask gap to alert (default: 0.03).",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=30.0,
        help="Per-opportunity cooldown before re-alerting (default: 30).",
    )
    parser.add_argument(
        "--mapping-refresh-seconds",
        type=float,
        default=60.0,
        help="Refresh mapping set from API every N seconds (default: 60).",
    )
    parser.add_argument(
        "--mapping-subscription-debounce-seconds",
        type=float,
        default=0.5,
        help=(
            "Coalesce multiplex mapping subscription diffs for this many seconds "
            "before applying them (default: 0.5)."
        ),
    )
    parser.add_argument(
        "--multiplex-dispatch-queue-size",
        type=int,
        default=100,
        help=(
            "Maximum queued quote updates per multiplex exchange before oldest "
            "updates are dropped (default: 100)."
        ),
    )
    parser.add_argument(
        "--book-stale-seconds",
        type=float,
        default=15.0,
        help="Ignore quotes older than this many seconds (default: 15).",
    )
    parser.add_argument(
        "--multiplex-stale-recovery-batch-size",
        type=int,
        default=5,
        help=(
            "Maximum number of stale multiplex streams to prioritize or resubscribe "
            "per recovery pass (default: 5)."
        ),
    )
    parser.add_argument(
        "--multiplex-aging-refresh-seconds",
        type=float,
        default=None,
        help=(
            "Promote successfully-seen multiplex streams for refresh once their age "
            "reaches this threshold, before they become stale (default: 70%% of stale threshold)."
        ),
    )
    parser.add_argument(
        "--multiplex-aging-refresh-cooldown-seconds",
        type=float,
        default=None,
        help=(
            "Cooldown before the same multiplex stream can be pre-stale refreshed again "
            "(default: half of the aging-refresh threshold)."
        ),
    )
    parser.add_argument(
        "--multiplex-aging-refresh-batch-size",
        type=int,
        default=2,
        help=(
            "Maximum number of aging multiplex streams to prioritize per refresh pass "
            "(default: 2)."
        ),
    )
    parser.add_argument(
        "--multiplex-kalshi-worker-count",
        type=int,
        default=2,
        help=(
            "Number of bounded producer workers to run for Kalshi multiplex ingestion "
            "(default: 2)."
        ),
    )
    parser.add_argument(
        "--multiplex-polymarket-worker-count",
        type=int,
        default=1,
        help=(
            "Number of bounded producer workers to run for Polymarket multiplex ingestion "
            "(default: 1; ignored when direct Polymarket websocket mode is enabled)."
        ),
    )
    parser.add_argument(
        "--multiplex-polymarket-source-mode",
        choices=("direct", "poll"),
        default="direct",
        help=(
            "Primary Polymarket multiplex source mode. 'direct' uses the exchange market "
            "websocket with shared multi-asset subscriptions; 'poll' keeps the legacy "
            "PMXT fetch loop as a fallback path."
        ),
    )
    parser.add_argument(
        "--multiplex-polymarket-market-ws-url",
        default="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        help="Polymarket market websocket URL for direct multiplex mode.",
    )
    parser.add_argument(
        "--multiplex-polymarket-ping-interval-seconds",
        type=float,
        default=5.0,
        help="Application ping interval for direct Polymarket websocket mode.",
    )
    parser.add_argument(
        "--multiplex-polymarket-receive-timeout-seconds",
        type=float,
        default=1.0,
        help="Receive timeout for direct Polymarket websocket mode.",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Optional PMXT watch_order_book depth limit.",
    )
    parser.add_argument(
        "--include-expired",
        action="store_true",
        help="Include expired pair sets in mapping source.",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive outcome links in mapping source.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    parser.add_argument(
        "--debug-heartbeat-seconds",
        type=float,
        default=10.0,
        help="Emit periodic debug heartbeat every N seconds when --debug is enabled.",
    )
    parser.add_argument(
        "--compact-alerts",
        action="store_true",
        help="Emit single-line alerts only (disable pretty multi-line formatting).",
    )
    parser.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default="auto",
        help="Terminal color mode for pretty alerts (default: auto).",
    )
    parser.add_argument(
        "--engine",
        choices=("legacy", "multiplex"),
        default="legacy",
        help=(
            "Streaming engine to run. 'legacy' keeps one worker per stream; "
            "'multiplex' uses exchange-level ingestion with direct Polymarket websocket support."
        ),
    )
    parser.add_argument(
        "--skip-pmxt-sidecar-restart",
        action="store_true",
        help=(
            "Do not restart PMXT sidecar on startup. By default the runner restarts "
            "the sidecar after loading resolved Kalshi credentials for shared WS sessions."
        ),
    )
    parser.add_argument(
        "--kalshi-direct-mode",
        action="store_true",
        help=(
            "Start Kalshi in direct-credential mode immediately (skip sidecar-default mode). "
            "Useful when sidecar WS auth drifts."
        ),
    )
    parser.add_argument(
        "--kalshi-book-mode",
        choices=("auto", "watch", "poll"),
        default="auto",
        help=(
            "Kalshi orderbook source mode. 'watch' forces websocket watch_order_book, "
            "'poll' uses fetch_order_book polling, 'auto' starts with watch and falls back "
            "to poll after repeated 429 rate limits."
        ),
    )
    return parser.parse_args()


def _http_get_json(
    base_url: str,
    path: str,
    *,
    query: dict[str, str] | None = None,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    url = f"{base}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"

    request = Request(url=url, method="GET")
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected non-object JSON from {url}")
        return payload


class ArbAlertRunner(_BaseArbAlertRunner):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(
            args,
            http_get_json=_http_get_json,
            exchange_builder=_build_exchange,
        )


def main() -> int:
    args = _parse_args()
    urllib3_major = int(urllib3.__version__.split(".", maxsplit=1)[0])
    if urllib3_major < 2:
        print(
            f"[error] urllib3={urllib3.__version__} detected; PMXT WebSocket/orderbook calls "
            "may fail with PoolKey key_ca_cert_data. Use urllib3>=2."
        )
        return 1

    runner = ArbAlertRunner(args)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
