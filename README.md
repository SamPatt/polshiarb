# Polshiarb

Repository for a Polymarket + Kalshi cross-exchange trading research and arbitrage workflow.

## Current scope

- Keep work incremental and explain-first.
- Use PMXT as the exchange interface layer for both market data and trading.
- Avoid getting lost in over-complex agent-driven implementations; build one small step at a time.
- Track changes in:
  - [`agent_journal.md`](./agent_journal.md) for high-level agent-facing notes
- User will track their own changes in:
  - [`sam_journal.md`](./sam_journal.md)

## PMXT integration model (confirmed)

PMXT details are tracked in [`TODO.md`](./TODO.md).
The core contract remains:

- PMXT is used as a Python SDK + local sidecar abstraction.
- For outcome-level calls (orderbook/trades/ohlcv), use `outcome_id`.
- For setup/sanity steps (startup path, health checks, caveats), see the TODO task list.

The full Python SDK API reference is in [`docs/PMXT_PYTHON_API_REFERENCE.md`](./docs/PMXT_PYTHON_API_REFERENCE.md).

## Docs

- [`docs/README.md`](./docs/README.md)
- [`docs/orderbooks-kalshi-polymarket.md`](./docs/orderbooks-kalshi-polymarket.md)
- [`docs/auth/README.md`](./docs/auth/README.md)

## Local setup and run

The repo now includes a Phase 1 web skeleton:
- FastAPI backend (`app/main.py`)
- Server-rendered UI template (`templates/index.html`)
- Local SQLite bootstrap (`app/db.py`)

### Prerequisites

- `python3` available on your PATH
- Internet access for first-time dependency install

### First-time setup

```bash
make setup
```

This creates `.venv`, upgrades `pip`, and installs pinned dependencies from `requirements.txt`.

### Initialize local DB

```bash
make init-db
```

This creates `data/pair_manager.db`.

### Run the web app

```bash
make run-web
```

Default URL:
- `http://127.0.0.1:8011/`

Health endpoint:
- `http://127.0.0.1:8011/healthz`

### WebSocket orderbook smoke test (DB-driven)

Use saved pair outcome links from `data/pair_manager.db` and stream orderbooks via PMXT:

```bash
.venv/bin/python scripts/ws_orderbook_smoke.py --max-streams 10 --updates-per-stream 1
```

Notes:
- Uses `active_only=true`, `include_expired=false` by default.
- Add `--include-inactive` and/or `--include-expired` to widen selection.
- Kalshi streaming requires auth; this repo supports `KALSHI_API_KEY` plus `KALSHI_PRIVATE_KEY=./key.pem`.

## API reference

Base URL (local):
- `http://127.0.0.1:8011`

General notes:
- Request/response format is JSON unless noted.
- Most endpoints currently return HTTP `200` with a predictable error payload for validation/runtime failures:
  - `{"ok": false, "error": "...", "error_code": "..."}`.

Common `error_code` values:
- `INVALID_URL`
- `INVALID_STATUS_FILTER`
- `PMXT_PREVIEW_FAILED`
- `PAIR_SAVE_FAILED`
- `PAIR_UPDATE_FAILED`
- `PAIR_NOT_FOUND`
- `MONITORING_QUERY_FAILED`

### `GET /healthz`

Lightweight service health check.

Example response:

```json
{
  "status": "ok",
  "db_path": "data/pair_manager.db"
}
```

### `POST /api/normalize-pair`

Normalize one Kalshi URL and one Polymarket URL into lookup identifiers.

Request:

```json
{
  "kalshi_url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
  "polymarket_url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner"
}
```

Success response:

```json
{
  "ok": true,
  "normalized": {
    "kalshi": {"lookup_value": "kxsenateild-26"},
    "polymarket": {"lookup_value": "illinois-democratic-senate-primary-winner"}
  }
}
```

### `POST /api/preview`

Fetch market/event details from PMXT for both exchanges and return normalized preview payload.

Request:

```json
{
  "kalshi_url": "https://kalshi.com/markets/kxsenateild/ild/kxsenateild-26",
  "polymarket_url": "https://polymarket.com/event/illinois-democratic-senate-primary-winner"
}
```

Success response includes:
- `normalized`
- `preview.kalshi`
- `preview.polymarket`

### `POST /api/pairs`

Create and persist one pair set plus selected mappings.

Request fields:
- `kalshi_url`, `polymarket_url`
- `normalized` (from normalize/preview)
- `preview` (from preview)
- `matches[]` with:
  - `kalshi_market_id`
  - `polymarket_market_id`
  - `relation_type` (`same_direction` or `inverse`)
  - `active` (`true`/`false`)
- `recurrence_intent` (nullable)
- `expires_at` (nullable `YYYY-MM-DD`)

Success response:

```json
{
  "ok": true,
  "pair_id": 12,
  "saved_link_rows": 4
}
```

### `GET /api/pairs?status=all|active|expired`

List saved pair sets for the list/edit UI.

### `GET /api/pairs/{pair_id}`

Load one saved pair set, including stored market snapshots and outcome links.

### `PUT /api/pairs/{pair_id}`

Update an existing pair set using the same request body shape as `POST /api/pairs`.

### `DELETE /api/pairs/{pair_id}`

Delete a pair set and cascaded child rows.

### `GET /api/monitoring/pairs?active_only=true&include_expired=false`

Use this endpoint for downstream monitoring services:

- `GET /api/monitoring/pairs?active_only=true&include_expired=false`

Default behavior:
- Includes only active links (`active_only=true`).
- Excludes expired pairs (`include_expired=false`).

Response shape (stable contract):

```json
{
  "ok": true,
  "active_only": true,
  "include_expired": false,
  "error": null,
  "error_code": null,
  "pairs": [
    {
      "pair_id": 12,
      "recurrence_intent": "weekly",
      "expires_at": "2026-12-31",
      "mappings": [
        {
          "relation_type": "same_direction",
          "legs": [
            {"exchange": "kalshi", "market_id": "KX...", "outcome_id": "KX..."},
            {"exchange": "polymarket", "market_id": "12345", "outcome_id": "67890"}
          ]
        }
      ]
    }
  ]
}
```

Notes:
- `pairs[].mappings[]` is outcome-link level (for `same_direction`, typically YES and NO rows).
- Each mapping has exactly two legs: one Kalshi leg and one Polymarket leg.
- If duplicate mappings exist across multiple pair sets, the monitoring endpoint keeps only the latest pair-set version (highest `pair_id`).

### PMXT health check

```bash
make check-pmxt
```

This validates PMXT + sidecar connectivity with a minimal fetch on Polymarket and Kalshi.

### Optional port override

If you want a different port temporarily:

```bash
PORT=8020 make run-web
```
