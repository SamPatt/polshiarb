# Pair Manager Implementation Plan

## 3-Part Plan (Pre-Implementation)
1. **What to change**
- Build a small full-stack "Pair Manager" app in this repo: URL intake, market/outcome fetch, manual outcome matching, persistence, edit/list views, optional expiration.
2. **Why this is the smallest useful step**
- It delivers the exact workflow end-to-end for one tool without adding trading logic yet, and creates clean persisted mappings that monitoring can consume later.
3. **How to verify quickly**
- Enter one Kalshi URL + one Polymarket URL, see normalized details and outcomes, save mappings, reopen and edit them, and confirm expiration filtering works.

## Detailed Implementation Plan

1. **Phase 0: Environment + PMXT reliability (Completed on 2026-03-03)**
- Create a pinned Python environment file for reproducibility.
- Reproduce and fix current PMXT runtime issue (`PoolKey...key_ca_cert_data`).
- Add a short health script that checks `pmxt` import and one fetch call per exchange.
- Deliverable: repeatable `make check-pmxt` (or equivalent) that passes locally.

2. **Phase 1: Project skeleton (Completed on 2026-03-03)**
- Backend: FastAPI service.
- Frontend: server-rendered templates (Jinja + HTMX) for speed and lower complexity.
- DB: SQLite initially (easy local), schema ready to move to Postgres later.
- Deliverable: app boots, DB migration runs, base page loads.

3. **Phase 2: URL parsing + normalization layer (Completed on 2026-03-04)**
- Build parser functions for Kalshi and Polymarket URLs.
- Extract canonical identifiers (`slug`, `market_id`, or event reference).
- Return structured parse results and validation errors for malformed URLs.
- Deliverable: unit tests for URL parser with real URL samples.

4. **Phase 3: Market fetch adapter layer**
- Implement `ExchangeAdapter` interface with `PMXTAdapter` first.
- Add resolution-rule field extraction strategy:
  - primary: PMXT unified market/event fields
  - fallback: direct exchange API fetch if PMXT misses critical rule text
- Normalize into one response model:
  - market title, description/rules, URL, resolution date, outcomes (`yes/no` and full list), IDs for trading.
- Deliverable: `/api/preview` endpoint returning both markets in one normalized payload.

5. **Phase 4: Pairing UI (new pair flow)**
- Build "Add Pair" page:
  - Kalshi URL input
  - PM URL input
  - "Load" action
- Display side-by-side market details and all tradable outcomes.
- Add mapping controls to link outcomes:
  - link type: `same_direction` or `inverse`
  - allow multiple mappings per pair set.
- Deliverable: user can create mappings in the browser without manual IDs.

6. **Phase 5: Persistence model + save flow**
- DB tables:
  - `pair_sets` (pair metadata, URLs, expiration, timestamps)
  - `pair_markets` (normalized market snapshots)
  - `pair_outcome_links` (outcome-to-outcome mappings, relation type, active flag)
- Save action writes all selected mappings atomically in one transaction.
- Deliverable: saved pair survives restart and can be queried by ID.

7. **Phase 6: View + edit existing pairs**
- Build list page with search/filter (`active`, `expired`, `all`).
- Build edit page:
  - update URLs (optional re-fetch)
  - modify mappings
  - toggle active/inactive links
  - update expiration date
- Deliverable: complete CRUD for pair sets and mappings.

8. **Phase 7: Expiration behavior**
- `expires_at` nullable (default `NULL` = no expiration).
- UI date picker with clear option.
- "Active mapping" query excludes expired pairs by default.
- Deliverable: monitoring-read API returns only active/non-expired mappings.

9. **Phase 8: Monitoring-facing API**
- Add endpoint for downstream bots/services:
  - `GET /api/monitoring/pairs?active_only=true`
- Response includes only fields needed for monitoring:
  - exchange, market IDs, outcome IDs, relation type, expiration.
- Deliverable: stable contract for your monitoring process.

10. **Phase 9: Testing + hardening**
- Unit tests: URL parsing, normalization, expiration logic.
- Integration tests: save/edit/list flows with test DB.
- Contract test: monitoring endpoint payload shape.
- Add structured logging and clear error messages for PMXT/network failures.
- Deliverable: baseline test suite and predictable failure behavior.

## Acceptance Criteria Mapped to the 7 Requirements
1. Input Kalshi URL: supported in Add Pair flow.
2. Input PM URL: supported in Add Pair flow.
3. Pull and display details/rules/outcomes: preview returns normalized fields and renders in UI.
4. Match tradeable events/outcomes: interactive mapping UI with same/inverse link types.
5. Save outcomes for monitoring: persisted in `pair_outcome_links`.
6. View/edit existing pairs: list + edit pages.
7. Automatic expiration date: nullable default + date picker + active filtering.

## Implementation Order (small-step)
1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6
8. Phase 7
9. Phase 8
10. Phase 9
