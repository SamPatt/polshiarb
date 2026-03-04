#!/usr/bin/env python3
"""SQLite bootstrap for the Pair Manager app."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path("data/pair_manager.db")

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    """,
]


def init_db(db_path: Path | str = DEFAULT_DB_PATH) -> Path:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path)
    try:
        with connection:
            for statement in SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)", (1,)
            )
    finally:
        connection.close()

    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize local SQLite database.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    args = parser.parse_args()

    created_path = init_db(args.db_path)
    print(f"database_initialized={created_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
