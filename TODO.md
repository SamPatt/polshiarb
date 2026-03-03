# TODO

- [ ] Initialize PMXT to test

## PMXT setup and local startup checks

- [ ] Confirm `pmxt` is installed in Python:
  ```bash
  pip install pmxt
  ```
- [ ] Verify PMXT usage model in repo:
  - PMXT is a Python SDK + local sidecar model (`http://localhost:3847`).
  - Python should initialize the sidecar automatically via `import pmxt` + exchange instantiation.
  - Keep `outcome_id` for outcome-level calls (`fetch_order_book`, `fetch_trades`, `fetch_ohlcv`).
- [ ] Validate sidecar start-up paths:
  - Primary path: run through Python-managed start (preferred).
  - Alternate validation path when needed:
    ```bash
    node /home/nondescript/.local/lib/python3.10/site-packages/pmxt/_server/server/bundled.js
    ```
- [ ] Add health/boot sanity checks for local startup:
  - `curl http://localhost:3847/health` should return `{ "status": "ok" }`.
  - Run a minimal SDK call (e.g., `fetch_markets`) and ensure response shape is valid.
- [ ] Record and track startup notes:
  - `pmxt-server` command availability may vary by install path/version.
  - npm/global `pmxt-core` installs can have ESM/CJS compatibility issues with Polymarket client in this environment.
  - Use the bundled validation path if needed.
