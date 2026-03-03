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

The full Python SDK API reference is in [`PMXT_PYTHON_API_REFERENCE.md`](./PMXT_PYTHON_API_REFERENCE.md).
