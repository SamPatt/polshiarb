# AGENT.md

This file defines behavior constraints for AI agents working in this repository.

## Core workflow

1. Build **one small thing at a time**.
2. Do not touch, modify, or add unrelated work without explicit instruction.
3. Before doing any implementation, the agent must present a short **3-part plan**:
   - What to change
   - Why this is the smallest useful step
   - How to verify it quickly
4. The agent must wait for the user to confirm understanding and approve the plan before making code changes.

## PMXT dependency and usage

This project depends on the PMXT stack for all exchange data and trading actions.

Assumptions:
- `pmxt-server` (Node sidecar) is the runtime service for all exchange calls.
- Python bots interact through the `pmxt` Python SDK only.
- Cross-platform arbitrage logic is built via PMXT abstractions, not direct exchange APIs.

Usage rules:
- Treat PMXT as a required dependency and explicit infrastructure component.
- Before any market, orderbook, or trade workflow is implemented, confirm:
  - The PMXT server is running.
  - Exchange client initialization uses PMXT credentials.
  - Error handling covers PMXT startup/API failures.
- Use PMXT model fields as the source of truth (`outcome_id` for outcome-level calls like orderbook/ohlcv/trades).

Minimum workflow per step:
1. Ensure PMXT availability (`pmxt-server` + `pmxt` client import works).
2. Fetch and validate minimal live data (`fetch_markets`/`fetch_order_book` on a known market).
3. Add trading logic only after the above is confirmed in logs/output.

## Communication rule (mandatory)

For every task, the agent must:
- Give plain-language explanation of what it thinks it is doing.
- Show exactly what file(s) will change.
- Explicitly ask: **"Can you confirm this before I implement?"**
- Pause until user approval.

## Execution rule

When implementing:
- Make the smallest possible change.
- Avoid broad refactors.
- Avoid adding abstractions unless requested.
- Prefer explicit, readable code over cleverness.
- Keep all modifications scoped and reversible.

## Completion format

After finishing an approved task, summarize:
1) What changed
2) Why it was done
3) Exact next tiny step

Do not start a second task in parallel.

## Quality and safety constraints

- Do not modify files outside the current task’s scope.
- Do not add third-party dependencies unless explicitly requested.
- Never change credentials, secrets, or API keys.
- If uncertain, stop and ask for a design choice instead of guessing.

## Trading bot specific constraint

This repo is for controlled, explain-first development. All trading logic changes must:
- be implemented incrementally
- include explicit assumptions and limitations
- include operational safeguards (dry-run mode, position limits, max exposure, and error handling) before introducing live execution.

## Ongoing project journal

Use `agent_journal.md` as the primary agent-facing repository log.
- The agent should append a high-level summary after each notable change/session.
- Include date/time, what changed, and any open risk.

The `sam_journal.md` is for the user's thoughts and shouldn't be touched unless the user asks.

For each user prompt, the agent must decide whether anything sufficiently new happened to warrant a log entry (not every prompt requires one).
If an entry is added, it must be included as the final section of the agent’s response.
