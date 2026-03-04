# PolShiarb Agent Journal

Use this file to keep a running record of what we learn about the repo and exchange behavior.

## Format

- `YYYY-MM-DD HH:MM TZ`
- **Goal:** What you are trying to do
- **What changed:** Files, commands, observations
- **Why:** Decision rationale
- **Open question / risk:** Anything uncertain or unresolved

## Entries

- 2026-03-03 00:00:00 (repo setup)
  - Journal initialized for ongoing repo understanding and build decisions.
  - This file should be appended to as work progresses.
- 2026-03-03 18:02:08 EST (workflow documentation)
  - Added `README.md` to capture current repo state, PMXT setup, and safety constraints.
  - Captured known working PMXT local startup path via bundled server script and journal workflow.
  - Open risk: unify runtime entrypoint (`pmxt-server` binary vs bundled script) and avoid global install pitfalls for Polymarket CJS/ESM.
- 2026-03-03 18:14 EST (repo path correction)
  - Moved the Python API reference integration to the intended project directory: `code_repos/sampatt/polshiarb`.
  - Added `PMXT_PYTHON_API_REFERENCE.md` there, copied from upstream PMXT Python docs.
  - Updated `code_repos/sampatt/polshiarb/README.md` to point to that reference file.
  - Open risk: keep both copies of repo docs in sync if work later continues in `/home/nondescript/sampatt/polshiarb` path (non-existent now).
- 2026-03-03 19:05 EST (phase 0 reproducible env baseline)
  - Added `requirements.txt` with pinned PMXT Python dependency versions (`pmxt`, `urllib3`, `python-dateutil`, `pydantic`, `typing-extensions`).
  - This establishes a reproducible baseline environment file for upcoming Pair Manager work.
  - Open risk: PMXT live fetch is still failing with a urllib3/PoolKey mismatch and will be addressed in the next Phase 0 reliability step.
- 2026-03-03 19:12 EST (phase 0 PMXT reliability check)
  - Root-caused PMXT fetch failure: generated PMXT client passes `ca_cert_data`, which fails under `urllib3` 1.26.x (`PoolKey.__new__() got unexpected keyword 'key_ca_cert_data`).
  - Updated pinned dependency baseline to `urllib3==2.2.3` in `requirements.txt`.
  - Added `scripts/check_pmxt_health.py` and `make check-pmxt` to validate `pmxt` import and one `fetch_markets` call on both Polymarket and Kalshi.
  - Verified locally: `make check-pmxt` passes using `.venv`.
  - Open risk: ensure all future commands run inside `.venv` (or with `PYTHON=.venv/bin/python`) to avoid fallback to distro Python packages.
- 2026-03-03 19:18 EST (make setup bootstrap)
  - Added `setup` target in `Makefile` to create `.venv`, upgrade `pip`, and install `requirements.txt` in one command.
  - Verified onboarding flow: `make setup && make check-pmxt` succeeds locally.
  - Open risk: this assumes `python3` is available on PATH on fresh machines.
- 2026-03-03 19:24 EST (phase plan status update)
  - Marked Phase 0 as completed in `plan/pair-manager-implementation-plan.md` after environment pinning + PMXT health verification.
  - Next planned work is Phase 1 project skeleton (FastAPI + server-rendered UI scaffold).
- 2026-03-03 19:33 EST (phase 1 project skeleton)
  - Added FastAPI app skeleton with server-rendered template UI: `app/main.py`, `templates/index.html`, and package init.
  - Added SQLite bootstrap module `app/db.py` and `make init-db` target.
  - Added `make run-web` target and pinned web dependencies (`fastapi`, `uvicorn`, `jinja2`) in `requirements.txt`.
  - Verified end-to-end: `make setup`, `make init-db`, `/healthz`, `/` page render, and `make check-pmxt` all pass.
  - Open risk: `@app.on_event("startup")` remains valid today but may be migrated to lifespan later if FastAPI deprecates the hook further.
- 2026-03-04 09:55 EST (README runbook update)
  - Updated `README.md` with local setup/run instructions for the Phase 1 skeleton.
  - Added documented command flow: `make setup`, `make init-db`, `make run-web`, `make check-pmxt`.
  - Documented default app URL/health endpoint on port `8011` and a port override example.
- 2026-03-04 10:08 EST (phase 2 URL parsing + normalization)
  - Added URL parsing/normalization module `app/url_normalization.py` for Kalshi and Polymarket URLs.
  - Added API endpoint `POST /api/normalize-pair` in `app/main.py` returning normalized lookup data or validation errors.
  - Added parser unit tests in `tests/test_url_normalization.py` and `make test` target in `Makefile`.
  - Added `pytest` to `requirements.txt` and marked Phase 2 completed in the plan file.
  - Verification: `make test` passed (7 tests). Normalization endpoint returns expected ticker/slug for sample URLs.
  - Open risk: endpoint currently returns `{ok:false,...}` with HTTP 200 on validation failures; Phase 3 can move this to explicit 4xx response modeling.
- 2026-03-04 10:21 EST (recurrence ownership decision)
  - Updated implementation plan to clarify recurrence/date handling ownership.
  - URL normalization stays deterministic (URL -> identifiers only); recurrence instance selection is app-domain logic, not PMXT logic.
  - Added recurrence intent/config notes to Phase 4 (UI) and Phase 5 (persistence).
- 2026-03-04 10:40 EST (phase 3 PMXT adapter + preview endpoint)
  - Added `app/pmxt_adapter.py` with `PMXTAdapter` to fetch/normalize Kalshi market and Polymarket event details from Phase 2 normalized lookups.
  - Added `POST /api/preview` in `app/main.py` to normalize input URLs and fetch PMXT-backed preview payloads.
  - Added adapter unit test file `tests/test_pmxt_adapter.py` with stub clients to validate normalization shape without live network.
  - Marked Phase 3 complete in the implementation plan.
  - Verification: `make test` passed (8 tests) and live `/api/preview` returned `ok=true` with Kalshi market + Polymarket event (32 markets).
  - Open risk: Polymarket event URLs can map to many markets; Phase 4 UI will need explicit market selection/matching UX.
