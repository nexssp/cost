# Cost adapter for Nexss Flow

`Bundle(opts)` follows the standard `core.Bundle` contract. It creates a
local in-process ledger from `@require` options and installs a
`GuardAction` hook on every atom that carries `:cost_estimate=N`. The
ledger enforces the configured limit, reserves per-atom estimates before
execution, and commits the actual cost afterwards — or releases on
failure, cancellation, or panic.

## Configuration

| `@require` key | Default | Effect |
|---|---|---|
| `budget` | `-1` | Limit in micro-units; negative means unlimited |
| `currency` | `USD` | ISO-4217 three-letter code carried on every event |

## DSL

`:cost_estimate=N` — reserve N micro-units before the atom runs. If the
result implements `cost.Reporter`, the reported cost replaces N on
commit; otherwise N is committed. On error, cancellation, or panic the
reservation is released.

`cost.snapshot` — returns `{limit_micros, used_micros, spent_micros,
currency}`.

## Example

```nflow
@require github.com/nexssp/cost/nexssflow { budget: "1000000", currency: "USD" }

@assert: result.spent_micros == 150000

charge @{ amount: 100 }:cost_estimate=200000
-> cost.snapshot
```
