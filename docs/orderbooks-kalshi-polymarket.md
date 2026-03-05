# Kalshi and Polymarket Orderbook Mechanics

This doc defines how to interpret orderbooks for cross-market arb, with PMXT as the primary interface.

## Scope and assumptions

- We use PMXT `fetch_order_book(outcome_id)` as the canonical read path.
- PMXT returns a unified orderbook shape:
  - `bids: List[OrderLevel]`
  - `asks: List[OrderLevel]`
  - `OrderLevel = { price: float, size: float }`
- In PMXT, prices are normalized to the `0.00` to `1.00` probability scale across exchanges.

## Key identity fields

- Kalshi outcome ID in PMXT: market ticker (`outcome_id == ticker`)
- Polymarket outcome ID in PMXT: CLOB token ID (`outcome_id == token_id`)

Always fetch orderbooks by `outcome_id`, not by event ID.

## Kalshi orderbook model

Kalshi binary contracts settle at `$1.00` if true and `$0.00` if false.

- A YES price of `0.63` means paying `63c` per contract.
- A NO price is the complement of YES in payout terms.
- Contract payoff per share at resolution:
  - YES share pays `1` if event resolves YES, else `0`.
  - NO share pays `1` if event resolves NO, else `0`.

### Raw market intuition

Kalshi is naturally a cent-based binary book (`1` to `99` cents), even when PMXT normalizes to `0-1`.

- `best_yes_bid` and `best_yes_ask` are the top levels for buying/selling YES.
- Complement relationships (conceptual parity):
  - `no_bid ~= 1 - yes_ask`
  - `no_ask ~= 1 - yes_bid`
- Small dislocations can exist due to spread, depth differences, timing, and fees.

## Polymarket orderbook model

Polymarket binary markets are represented by outcome tokens (YES/NO token IDs).

- Orders are placed on outcome token orderbooks (CLOB).
- PMXT exposes these as standard `bids`/`asks` on the same `0-1` price scale.
- For binary markets, YES and NO are economic complements at settlement (`1` vs `0`).

### Practical interpretation

- `price` is marginal fill price per share for a level.
- `size` is executable quantity at that level.
- Best prices:
  - Best bid = highest `bids[i].price`
  - Best ask = lowest `asks[i].price`
- Top-of-book spread: `best_ask - best_bid`

## Execution-aware price (required for arb)

Top-of-book is not enough for sizing. Use depth-weighted simulation.

Given desired size `Q` (shares/contracts):

- Buy-side effective price: walk `asks` from lowest price upward until `Q` filled.
- Sell-side effective price: walk `bids` from highest price downward until `Q` filled.

PMXT helper methods:

- `get_execution_price(order_book, side, amount)`
- `get_execution_price_detailed(order_book, side, amount)`

These should be used for both Kalshi and Polymarket before any arb trigger.

## Cross-exchange normalization for arb

Use these invariants in all comparisons:

- Same direction mapping (`YES` vs `YES`): compare direct prices.
- Inverse mapping (`YES` vs `NO`): compare against complement (`1 - p`).
- All logic should run on the same unit system (`0-1` prices, same quantity convention).

## Suggested implementation contract

For each paired outcome mapping:

1. Fetch both books by `outcome_id`.
2. Compute executable buy/sell prices for target size on both sides.
3. Apply relation transform (`same_direction` or `inverse`).
4. Subtract expected fees/slippage buffer.
5. Emit opportunity only if net edge is positive and fully fillable.

This keeps Kalshi and Polymarket behavior consistent under PMXT and avoids top-of-book false positives.
