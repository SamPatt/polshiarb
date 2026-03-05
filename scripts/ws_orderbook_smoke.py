#!/usr/bin/env python3
"""WebSocket orderbook smoke test using saved pair mappings from the local DB.

This script loads monitoring outcome links from `data/pair_manager.db` (or a
custom path), then starts PMXT WebSocket orderbook watchers for Kalshi and
Polymarket outcome IDs derived from those links.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
import threading
import time
from typing import Any

import pmxt
import urllib3

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.db import DEFAULT_DB_PATH, list_monitoring_links
from app.kalshi_credentials import build_kalshi_client_kwargs


@dataclass
class StreamTarget:
    exchange: str
    outcome_id: str
    market_ids: list[str]
    pair_ids: list[int]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test PMXT watch_order_book for outcomes from saved pair mappings.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to pair manager sqlite db (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--include-expired",
        action="store_true",
        help="Include links from expired pair sets.",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive outcome links.",
    )
    parser.add_argument(
        "--max-streams",
        type=int,
        default=10,
        help="Maximum number of unique streams to start (default: 10).",
    )
    parser.add_argument(
        "--updates-per-stream",
        type=int,
        default=1,
        help=(
            "Number of watch updates to print per stream; 0 means run continuously "
            "until Ctrl+C (default: 1)."
        ),
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Optional orderbook depth hint passed to watch_order_book.",
    )
    return parser.parse_args()


def _level_price(level: Any) -> float | None:
    if isinstance(level, dict):
        value = level.get("price")
    else:
        value = getattr(level, "price", None)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _level_size(level: Any) -> float | None:
    if isinstance(level, dict):
        value = level.get("size")
    else:
        value = getattr(level, "size", None)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _book_levels(book: Any, side: str) -> list[Any]:
    if isinstance(book, dict):
        raw = book.get(side)
    else:
        raw = getattr(book, side, None)
    if isinstance(raw, list):
        return raw
    return []


def _best_level(levels: list[Any], side: str) -> tuple[float | None, float | None]:
    priced_levels: list[tuple[float, float | None]] = []
    for level in levels:
        price = _level_price(level)
        if price is None:
            continue
        priced_levels.append((price, _level_size(level)))

    if not priced_levels:
        return None, None

    if side == "bid":
        best_price, best_size = max(priced_levels, key=lambda item: item[0])
    else:
        best_price, best_size = min(priced_levels, key=lambda item: item[0])
    return best_price, best_size


def _book_timestamp(book: Any) -> str:
    if isinstance(book, dict):
        raw = book.get("timestamp")
    else:
        raw = getattr(book, "timestamp", None)

    if isinstance(raw, (int, float)):
        # PMXT timestamps are expected in ms.
        dt = datetime.fromtimestamp(float(raw) / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    return "unknown"


def _format_stream_update(target: StreamTarget, book: Any) -> str:
    bids = _book_levels(book, "bids")
    asks = _book_levels(book, "asks")
    bid_price, bid_size = _best_level(bids, "bid")
    ask_price, ask_size = _best_level(asks, "ask")

    spread: float | None = None
    if bid_price is not None and ask_price is not None:
        spread = ask_price - bid_price

    return (
        f"[{target.exchange}] outcome={target.outcome_id} "
        f"pairs={target.pair_ids} markets={target.market_ids} "
        f"ts={_book_timestamp(book)} "
        f"best_bid={bid_price} x {bid_size} "
        f"best_ask={ask_price} x {ask_size} "
        f"spread={spread}"
    )


def _collect_stream_targets(
    db_path: Path,
    *,
    active_only: bool,
    include_expired: bool,
    max_streams: int,
) -> list[StreamTarget]:
    links = list_monitoring_links(
        db_path=db_path,
        active_only=active_only,
        include_expired=include_expired,
        dedupe_latest=True,
    )

    aggregated: dict[tuple[str, str], dict[str, Any]] = {}

    for link in links:
        pair_id = int(link["pair_id"])

        for exchange, outcome_key, market_key in (
            ("kalshi", "kalshi_outcome_id", "kalshi_market_id"),
            ("polymarket", "polymarket_outcome_id", "polymarket_market_id"),
        ):
            outcome = link.get(outcome_key)
            if not isinstance(outcome, str) or not outcome:
                continue

            market_id = link.get(market_key)
            market_value = str(market_id) if market_id is not None else "unknown"
            key = (exchange, outcome)
            bucket = aggregated.setdefault(
                key,
                {
                    "exchange": exchange,
                    "outcome_id": outcome,
                    "market_ids": set(),
                    "pair_ids": set(),
                },
            )
            bucket["market_ids"].add(market_value)
            bucket["pair_ids"].add(pair_id)

    targets: list[StreamTarget] = []
    for value in aggregated.values():
        targets.append(
            StreamTarget(
                exchange=value["exchange"],
                outcome_id=value["outcome_id"],
                market_ids=sorted(value["market_ids"]),
                pair_ids=sorted(value["pair_ids"]),
            )
        )

    if max_streams > 0:
        return targets[:max_streams]
    return targets


def _build_exchange(exchange: str) -> Any:
    if exchange == "kalshi":
        return pmxt.Kalshi(**build_kalshi_client_kwargs())
    if exchange == "polymarket":
        return pmxt.Polymarket()
    raise ValueError(f"Unsupported exchange: {exchange}")


def _worker(
    target: StreamTarget,
    *,
    updates_per_stream: int,
    depth: int | None,
    stop_event: threading.Event,
    print_lock: threading.Lock,
) -> None:
    exchange = _build_exchange(target.exchange)
    updates = 0

    try:
        while not stop_event.is_set():
            if updates_per_stream > 0 and updates >= updates_per_stream:
                break

            try:
                book = exchange.watch_order_book(target.outcome_id, limit=depth)
            except Exception as exc:  # pragma: no cover - operational visibility
                with print_lock:
                    print(
                        f"[error] [{target.exchange}] outcome={target.outcome_id} "
                        f"watch_order_book failed: {exc}"
                    )
                break

            updates += 1
            with print_lock:
                print(_format_stream_update(target, book))

            # Avoid hot loops if watch returns immediately.
            if updates_per_stream == 0:
                time.sleep(0.1)
    finally:
        try:
            exchange.close()
        except Exception:
            pass


def main() -> int:
    args = _parse_args()
    urllib3_major = int(urllib3.__version__.split(".", maxsplit=1)[0])
    if urllib3_major < 2:
        print(
            f"[error] urllib3={urllib3.__version__} detected; PMXT WebSocket/orderbook calls "
            "may fail with PoolKey key_ca_cert_data. Use urllib3>=2."
        )
        return 1

    active_only = not args.include_inactive

    targets = _collect_stream_targets(
        db_path=args.db_path,
        active_only=active_only,
        include_expired=args.include_expired,
        max_streams=args.max_streams,
    )

    if not targets:
        print(
            "[info] No stream targets found from pair DB. "
            "Check that active pair outcome links exist."
        )
        return 1

    print(
        f"[info] Starting {len(targets)} stream(s) from {args.db_path} "
        f"(active_only={active_only}, include_expired={args.include_expired}, "
        f"updates_per_stream={args.updates_per_stream}, depth={args.depth})"
    )

    stop_event = threading.Event()
    print_lock = threading.Lock()
    threads: list[threading.Thread] = []

    for target in targets:
        thread = threading.Thread(
            target=_worker,
            kwargs={
                "target": target,
                "updates_per_stream": args.updates_per_stream,
                "depth": args.depth,
                "stop_event": stop_event,
                "print_lock": print_lock,
            },
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\n[info] Ctrl+C received; stopping streams...")
        stop_event.set()
        return 130

    print("[info] Smoke run complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
