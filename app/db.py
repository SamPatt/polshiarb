#!/usr/bin/env python3
"""SQLite bootstrap for the Pair Manager app."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path("data/pair_manager.db")

MIGRATION_V2_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pair_sets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kalshi_url TEXT NOT NULL,
        polymarket_url TEXT NOT NULL,
        kalshi_lookup_value TEXT,
        polymarket_lookup_value TEXT,
        recurrence_intent TEXT,
        expires_at TEXT,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pair_markets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair_set_id INTEGER NOT NULL REFERENCES pair_sets(id) ON DELETE CASCADE,
        exchange TEXT NOT NULL,
        market_id TEXT NOT NULL,
        title TEXT,
        url TEXT,
        resolution_rules TEXT,
        raw_snapshot_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        UNIQUE(pair_set_id, exchange, market_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pair_outcome_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair_set_id INTEGER NOT NULL REFERENCES pair_sets(id) ON DELETE CASCADE,
        kalshi_market_id TEXT NOT NULL,
        polymarket_market_id TEXT NOT NULL,
        kalshi_outcome_id TEXT,
        polymarket_outcome_id TEXT,
        relation_type TEXT NOT NULL DEFAULT 'same_direction',
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        UNIQUE(
            pair_set_id,
            kalshi_market_id,
            polymarket_market_id,
            kalshi_outcome_id,
            polymarket_outcome_id,
            relation_type
        )
    );
    """,
]


def _connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _ensure_schema_migrations(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        """
    )


def _current_schema_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
    return int(row["version"] or 0)


def _apply_migration(
    connection: sqlite3.Connection, version: int, statements: list[str]
) -> None:
    for statement in statements:
        connection.execute(statement)
    connection.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))


def _extract_markets_from_preview(preview_exchange_payload: dict[str, Any]) -> list[dict[str, Any]]:
    event = preview_exchange_payload.get("event")
    if isinstance(event, dict):
        markets = event.get("markets") or []
        return [market for market in markets if isinstance(market, dict)]

    market = preview_exchange_payload.get("market")
    if isinstance(market, dict):
        return [market]

    return []


def _index_markets_by_id(markets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for market in markets:
        market_id = market.get("market_id")
        if isinstance(market_id, str) and market_id:
            indexed[market_id] = market
    return indexed


def _safe_outcome_id(market: dict[str, Any], side: str) -> str | None:
    outcome = market.get(side)
    if isinstance(outcome, dict):
        outcome_id = outcome.get("outcome_id")
        if isinstance(outcome_id, str) and outcome_id:
            return outcome_id
    return None


def _build_outcome_rows(
    match: dict[str, Any],
    kalshi_market: dict[str, Any] | None,
    polymarket_market: dict[str, Any] | None,
) -> list[tuple[str | None, str | None, str, int]]:
    relation_type = str(match.get("relation_type") or "same_direction")
    active = 1 if bool(match.get("active", True)) else 0
    kalshi_yes = _safe_outcome_id(kalshi_market or {}, "yes")
    kalshi_no = _safe_outcome_id(kalshi_market or {}, "no")
    polymarket_yes = _safe_outcome_id(polymarket_market or {}, "yes")
    polymarket_no = _safe_outcome_id(polymarket_market or {}, "no")

    rows: list[tuple[str | None, str | None, str, int]] = []
    if relation_type == "inverse":
        if kalshi_yes and polymarket_no:
            rows.append((kalshi_yes, polymarket_no, relation_type, active))
        if kalshi_no and polymarket_yes:
            rows.append((kalshi_no, polymarket_yes, relation_type, active))
    else:
        if kalshi_yes and polymarket_yes:
            rows.append((kalshi_yes, polymarket_yes, "same_direction", active))
        if kalshi_no and polymarket_no:
            rows.append((kalshi_no, polymarket_no, "same_direction", active))

    if not rows:
        rows.append((None, None, relation_type, active))
    return rows


def _extract_lookup_value(
    normalized: dict[str, Any], exchange: str, existing_value: str | None
) -> str | None:
    exchange_payload = normalized.get(exchange)
    if isinstance(exchange_payload, dict) and "lookup_value" in exchange_payload:
        lookup = exchange_payload.get("lookup_value")
        if isinstance(lookup, str):
            return lookup
        return None
    return existing_value


def _persist_pair_markets(
    connection: sqlite3.Connection, pair_id: int, preview: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kalshi_payload = preview.get("kalshi") or {}
    polymarket_payload = preview.get("polymarket") or {}
    kalshi_markets = _extract_markets_from_preview(kalshi_payload)
    polymarket_markets = _extract_markets_from_preview(polymarket_payload)

    connection.execute("DELETE FROM pair_markets WHERE pair_set_id = ?", (pair_id,))
    for exchange, markets in (("kalshi", kalshi_markets), ("polymarket", polymarket_markets)):
        for market in markets:
            connection.execute(
                """
                INSERT OR IGNORE INTO pair_markets (
                    pair_set_id,
                    exchange,
                    market_id,
                    title,
                    url,
                    resolution_rules,
                    raw_snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_id,
                    exchange,
                    market.get("market_id"),
                    market.get("title"),
                    market.get("url"),
                    market.get("resolution_rules"),
                    json.dumps(market),
                ),
            )
    return kalshi_markets, polymarket_markets


def _persist_outcome_links(
    connection: sqlite3.Connection,
    pair_id: int,
    matches: list[dict[str, Any]],
    kalshi_markets: list[dict[str, Any]],
    polymarket_markets: list[dict[str, Any]],
) -> int:
    kalshi_by_id = _index_markets_by_id(kalshi_markets)
    polymarket_by_id = _index_markets_by_id(polymarket_markets)

    connection.execute("DELETE FROM pair_outcome_links WHERE pair_set_id = ?", (pair_id,))
    saved_link_rows = 0
    for match in matches:
        kalshi_market_id = match.get("kalshi_market_id")
        polymarket_market_id = match.get("polymarket_market_id")
        if not isinstance(kalshi_market_id, str) or not isinstance(polymarket_market_id, str):
            continue

        outcome_rows = _build_outcome_rows(
            match=match,
            kalshi_market=kalshi_by_id.get(kalshi_market_id),
            polymarket_market=polymarket_by_id.get(polymarket_market_id),
        )
        for kalshi_outcome_id, polymarket_outcome_id, relation_type, active in outcome_rows:
            connection.execute(
                """
                INSERT OR IGNORE INTO pair_outcome_links (
                    pair_set_id,
                    kalshi_market_id,
                    polymarket_market_id,
                    kalshi_outcome_id,
                    polymarket_outcome_id,
                    relation_type,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_id,
                    kalshi_market_id,
                    polymarket_market_id,
                    kalshi_outcome_id,
                    polymarket_outcome_id,
                    relation_type,
                    active,
                ),
            )
            saved_link_rows += 1
    return saved_link_rows


def init_db(db_path: Path | str = DEFAULT_DB_PATH) -> Path:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    connection = _connect(path)
    try:
        with connection:
            _ensure_schema_migrations(connection)
            version = _current_schema_version(connection)
            if version == 0:
                connection.execute("INSERT INTO schema_migrations(version) VALUES (1)")
                version = 1
            if version < 2:
                _apply_migration(connection, 2, MIGRATION_V2_STATEMENTS)
    finally:
        connection.close()

    return path


def save_pair_set(db_path: Path | str, payload: dict[str, Any]) -> dict[str, Any]:
    init_db(db_path)
    normalized = payload.get("normalized") or {}
    preview = payload.get("preview") or {}
    matches = payload.get("matches") or []

    connection = _connect(db_path)
    try:
        with connection:
            pair_row = connection.execute(
                """
                INSERT INTO pair_sets (
                    kalshi_url,
                    polymarket_url,
                    kalshi_lookup_value,
                    polymarket_lookup_value,
                    recurrence_intent,
                    expires_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("kalshi_url"),
                    payload.get("polymarket_url"),
                    (normalized.get("kalshi") or {}).get("lookup_value"),
                    (normalized.get("polymarket") or {}).get("lookup_value"),
                    payload.get("recurrence_intent"),
                    payload.get("expires_at"),
                ),
            )
            pair_id = int(pair_row.lastrowid)

            kalshi_markets, polymarket_markets = _persist_pair_markets(
                connection=connection, pair_id=pair_id, preview=preview
            )
            saved_link_rows = _persist_outcome_links(
                connection=connection,
                pair_id=pair_id,
                matches=matches,
                kalshi_markets=kalshi_markets,
                polymarket_markets=polymarket_markets,
            )

        return {"pair_id": pair_id, "saved_link_rows": saved_link_rows}
    finally:
        connection.close()


def update_pair_set(db_path: Path | str, pair_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    init_db(db_path)
    normalized = payload.get("normalized") or {}
    preview = payload.get("preview")
    matches = payload.get("matches")

    if not isinstance(preview, dict):
        raise ValueError("preview is required when updating a pair")
    if not isinstance(matches, list):
        raise ValueError("matches is required when updating a pair")

    connection = _connect(db_path)
    try:
        with connection:
            pair = connection.execute(
                """
                SELECT id, kalshi_lookup_value, polymarket_lookup_value
                FROM pair_sets
                WHERE id = ?
                """,
                (pair_id,),
            ).fetchone()
            if pair is None:
                return None

            connection.execute(
                """
                UPDATE pair_sets
                SET
                    kalshi_url = ?,
                    polymarket_url = ?,
                    kalshi_lookup_value = ?,
                    polymarket_lookup_value = ?,
                    recurrence_intent = ?,
                    expires_at = ?,
                    updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                WHERE id = ?
                """,
                (
                    payload.get("kalshi_url"),
                    payload.get("polymarket_url"),
                    _extract_lookup_value(
                        normalized=normalized,
                        exchange="kalshi",
                        existing_value=pair["kalshi_lookup_value"],
                    ),
                    _extract_lookup_value(
                        normalized=normalized,
                        exchange="polymarket",
                        existing_value=pair["polymarket_lookup_value"],
                    ),
                    payload.get("recurrence_intent"),
                    payload.get("expires_at"),
                    pair_id,
                ),
            )

            kalshi_markets, polymarket_markets = _persist_pair_markets(
                connection=connection, pair_id=pair_id, preview=preview
            )
            saved_link_rows = _persist_outcome_links(
                connection=connection,
                pair_id=pair_id,
                matches=matches,
                kalshi_markets=kalshi_markets,
                polymarket_markets=polymarket_markets,
            )

        return {"pair_id": pair_id, "saved_link_rows": saved_link_rows}
    finally:
        connection.close()


def delete_pair_set(db_path: Path | str, pair_id: int) -> bool:
    init_db(db_path)
    connection = _connect(db_path)
    try:
        with connection:
            row = connection.execute("DELETE FROM pair_sets WHERE id = ?", (pair_id,))
            return row.rowcount > 0
    finally:
        connection.close()


def list_pair_sets(db_path: Path | str, status: str = "all") -> list[dict[str, Any]]:
    init_db(db_path)
    connection = _connect(db_path)
    try:
        rows = connection.execute(
            """
            SELECT
                ps.id,
                ps.kalshi_url,
                ps.polymarket_url,
                ps.recurrence_intent,
                ps.expires_at,
                ps.created_at,
                ps.updated_at,
                (
                    SELECT COUNT(*)
                    FROM pair_markets pm
                    WHERE pm.pair_set_id = ps.id
                ) AS market_rows,
                (
                    SELECT COUNT(*)
                    FROM pair_outcome_links pol
                    WHERE pol.pair_set_id = ps.id
                ) AS link_rows,
                (
                    SELECT COUNT(*)
                    FROM pair_outcome_links pol
                    WHERE pol.pair_set_id = ps.id AND pol.active = 1
                ) AS active_link_rows
            FROM pair_sets ps
            ORDER BY ps.id DESC
            """
        ).fetchall()

        today = date.today()
        pairs: list[dict[str, Any]] = []
        for row in rows:
            expires_at = row["expires_at"]
            is_expired = False
            if isinstance(expires_at, str) and expires_at:
                try:
                    is_expired = date.fromisoformat(expires_at) < today
                except ValueError:
                    is_expired = False

            pair = {
                "id": row["id"],
                "kalshi_url": row["kalshi_url"],
                "polymarket_url": row["polymarket_url"],
                "recurrence_intent": row["recurrence_intent"],
                "expires_at": row["expires_at"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "market_rows": int(row["market_rows"] or 0),
                "link_rows": int(row["link_rows"] or 0),
                "active_link_rows": int(row["active_link_rows"] or 0),
                "is_expired": is_expired,
            }
            pairs.append(pair)

        if status == "active":
            return [pair for pair in pairs if not pair["is_expired"] and pair["active_link_rows"] > 0]
        if status == "expired":
            return [pair for pair in pairs if pair["is_expired"]]
        return pairs
    finally:
        connection.close()


def load_pair_set(db_path: Path | str, pair_id: int) -> dict[str, Any] | None:
    init_db(db_path)
    connection = _connect(db_path)
    try:
        pair = connection.execute(
            """
            SELECT
                id,
                kalshi_url,
                polymarket_url,
                kalshi_lookup_value,
                polymarket_lookup_value,
                recurrence_intent,
                expires_at,
                created_at,
                updated_at
            FROM pair_sets
            WHERE id = ?
            """,
            (pair_id,),
        ).fetchone()
        if pair is None:
            return None

        market_rows = connection.execute(
            """
            SELECT exchange, market_id, title, url, resolution_rules, raw_snapshot_json, created_at
            FROM pair_markets
            WHERE pair_set_id = ?
            ORDER BY exchange, market_id
            """,
            (pair_id,),
        ).fetchall()
        link_rows = connection.execute(
            """
            SELECT
                id,
                kalshi_market_id,
                polymarket_market_id,
                kalshi_outcome_id,
                polymarket_outcome_id,
                relation_type,
                active,
                created_at
            FROM pair_outcome_links
            WHERE pair_set_id = ?
            ORDER BY id
            """,
            (pair_id,),
        ).fetchall()

        markets_by_exchange = {"kalshi": [], "polymarket": []}
        for row in market_rows:
            exchange = row["exchange"]
            market_obj = {
                "market_id": row["market_id"],
                "title": row["title"],
                "url": row["url"],
                "resolution_rules": row["resolution_rules"],
                "created_at": row["created_at"],
                "raw_snapshot": json.loads(row["raw_snapshot_json"]),
            }
            if exchange in markets_by_exchange:
                markets_by_exchange[exchange].append(market_obj)

        outcome_links = [
            {
                "id": row["id"],
                "kalshi_market_id": row["kalshi_market_id"],
                "polymarket_market_id": row["polymarket_market_id"],
                "kalshi_outcome_id": row["kalshi_outcome_id"],
                "polymarket_outcome_id": row["polymarket_outcome_id"],
                "relation_type": row["relation_type"],
                "active": bool(row["active"]),
                "created_at": row["created_at"],
            }
            for row in link_rows
        ]

        return {
            "pair_set": {
                "id": pair["id"],
                "kalshi_url": pair["kalshi_url"],
                "polymarket_url": pair["polymarket_url"],
                "kalshi_lookup_value": pair["kalshi_lookup_value"],
                "polymarket_lookup_value": pair["polymarket_lookup_value"],
                "recurrence_intent": pair["recurrence_intent"],
                "expires_at": pair["expires_at"],
                "created_at": pair["created_at"],
                "updated_at": pair["updated_at"],
            },
            "markets": markets_by_exchange,
            "outcome_links": outcome_links,
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize local SQLite database.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    args = parser.parse_args()

    created_path = init_db(args.db_path)
    print(f"database_initialized={created_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
