# Kalshi Auth with PMXT (Python)

This guide documents how Kalshi authentication works in this repo with PMXT.

## What PMXT expects

From installed `pmxt==2.18.1` in this repo:

- Python constructor:
  - `pmxt.Kalshi(api_key=..., private_key=...)`
- Sidecar env fallback:
  - `KALSHI_API_KEY`
  - `KALSHI_PRIVATE_KEY`

Important:

- PMXT itself expects `private_key` to be RSA PEM contents.
- This repo now adds a resolver that also accepts `KALSHI_PRIVATE_KEY` as a file path and loads the PEM text automatically.

## Repo-supported env format

The current recommended `.env` values are:

```dotenv
KALSHI_API_KEY=your_key_id_here
KALSHI_PRIVATE_KEY=./key.pem
```

These values can be exported environment variables or kept in repo `.env`.

How it is handled:

- If `KALSHI_PRIVATE_KEY` looks like PEM text, it is used directly.
- Otherwise it is treated as a path (`./key.pem`, absolute path, or `~` path) and file contents are loaded.
- If the path cannot be read, raw value is passed through (PMXT then returns a key/auth error).

Wired locations in this repo:

- [pmxt_adapter.py](/home/nondescript/code_repos/sampatt/polshiarb/app/pmxt_adapter.py)
- [ws_orderbook_smoke.py](/home/nondescript/code_repos/sampatt/polshiarb/scripts/ws_orderbook_smoke.py)
- [check_pmxt_health.py](/home/nondescript/code_repos/sampatt/polshiarb/scripts/check_pmxt_health.py)
- Resolver source: [kalshi_credentials.py](/home/nondescript/code_repos/sampatt/polshiarb/app/kalshi_credentials.py)

## Smoke-test auth in this repo

Use the DB-driven websocket smoke script:

```bash
.venv/bin/python scripts/ws_orderbook_smoke.py --max-streams 4 --updates-per-stream 1
```

Expected behavior:

- With valid key id + readable PEM: Kalshi lines print best bid/ask like Polymarket streams.
- With invalid/missing key: Kalshi returns auth or key-decoder errors.

## Error to watch for

If creds are missing/invalid, PMXT returns messages like:

- `Trading operations require authentication. Initialize KalshiExchange with credentials (apiKey and privateKey).`

## Source references

- PMXT Python Kalshi constructor:
  - `.venv/lib/python3.10/site-packages/pmxt/_exchanges.py`
- PMXT Python credential forwarding (`apiKey`, `privateKey`):
  - `.venv/lib/python3.10/site-packages/pmxt/client.py`
- PMXT sidecar Kalshi env fallback (`KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY`):
  - `.venv/lib/python3.10/site-packages/pmxt/_server/server/bundled.js`
- PMXT Kalshi signing behavior (RSA private key string handling):
  - `.venv/lib/python3.10/site-packages/pmxt/_server/server/bundled.js`
