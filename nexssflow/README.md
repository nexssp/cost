# Cost adapter for Nexss Flow

`github.com/nexssp/cost/nexssflow` is an optional Flow adapter. It registers `:cost_estimate=N` as a Flow modifier; `N` may be a fixed integer or a validated path into the action's runtime input. The default backend is the local in-process ledger; PostgreSQL supports durable fixed monthly quotas and provisioned tenant/user prepaid wallets.

The modifier reserves the estimate before its target runs, then commits the reported cost when the target result implements `cost.Reporter` or commits the estimate otherwise. On action failure, cancellation, or panic, it releases the reservation. In PostgreSQL mode, insufficient monthly quota or wallet funds becomes a branchable denial result; database, identity, suspended-account, and settlement failures remain errors.

## `@require` configuration

All options are recognized by the bundle factory. Budget and estimates are integer micro-units: one unit of currency equals 1,000,000 micros.

| Option | Default | Meaning |
| --- | --- | --- |
| `backend` | `memory` | `memory`, `postgres`, or `storage` |
| `budget` | `-1` | Maximum usage in micros. Memory accepts `-1` as unlimited; PostgreSQL monthly mode requires a non-negative limit. Wallet mode uses provisioned database balances. Storage mode rejects this option because each tenant's limit is read from Storage for every request. |
| `currency` | `USD` | Three uppercase currency letters |
| `dsn` | unset | PostgreSQL DSN; environment references such as `$COST_POSTGRES_DSN` are expanded by the adapter |
| `schema_mode` | `external` | `external` uses a pre-provisioned schema; `auto` calls the PostgreSQL adapter's idempotent schema initializer |
| `accounting_mode` | `monthly` | PostgreSQL only: `monthly` for a configured tenant/month limit, or `wallet` for provisioned prepaid balances |
| `scope_prefix` | `support-quota` | Namespace included in durable scope keys |
| `tenant_field` | `customer_id` | Top-level string identity property. Required in both PostgreSQL modes |
| `user_field` | `user_id` | Wallet mode: top-level user identity property |
| `wallet_field` | `wallet_id` | Wallet mode: top-level wallet identity property |
| `reservation_ttl` | `5m` | PostgreSQL reservation lease lifetime; must be at least one second |
| `max_open_conns` | `10` | PostgreSQL connection-pool upper bound |
| `max_idle_conns` | `5` | PostgreSQL idle connection-pool bound, capped at `max_open_conns` |
| `storage_path` | unset | Storage mode: path to the pre-migrated SQLite Storage database; environment references such as `$COST_STORAGE_PATH` are expanded |
| `storage_namespace` | `cost-runtime-v1` | Storage mode: namespace in the existing Storage KV table |

In `.nflow` `@require` blocks, write environment references as `$NAME` (for example, `$COST_POSTGRES_DSN`), not `${NAME}`: Flow currently treats a `}` inside a quoted option value as the end of the options block.

Example durable configuration:

```nflow
@require github.com/nexssp/cost/nexssflow {
  backend: "postgres",
  accounting_mode: "monthly",
  dsn: "$COST_POSTGRES_DSN",
  schema_mode: "external",
  scope_prefix: "support-ticket-monthly",
  tenant_field: "customer_id",
  budget: "300000",
  currency: "USD",
  reservation_ttl: "5m"
}
```

The repository-local sample in [`examples/durable_quota/`](examples/durable_quota/) uses a local-path `@require` so Flow resolves and compiles the in-progress adapter directly from this checkout. The sample is a standalone folder and is intentionally separate from the local-ledger examples.

For a database-backed user wallet rather than a configured per-tenant cap, see the operational [`examples/durable_quota/`](examples/durable_quota/) workflow. It provisions tenant/user/wallet ownership and balances separately, reserves funds transactionally, and debits/audits successful work.

## Durable tenant quotas

In PostgreSQL mode, the modified action must receive an object with a non-empty string at `tenant_field`, and must return an object. The adapter derives a scope from `scope_prefix`, a SHA-256 digest of the tenant identifier, and the current UTC month (`YYYY-MM`). Each customer therefore has an independent quota for each UTC calendar month without storing the raw customer ID in the scope key.

The monthly PostgreSQL schema and transactional reservation implementation are supplied by `github.com/nexssp/cost/adapters/postgres`. In the default `schema_mode: "external"`, the deployment applies the schema before serving traffic and the adapter initializes each tenant-month budget row with an upsert; it does not issue DDL on requests. `schema_mode: "auto"` is available for monthly-mode development and calls `EnsureSchema` to create tables and initialize the scope. Use a database role with the required DML permissions for normal runtime, and keep `reservation_ttl` longer than the maximum expected duration of a protected action. Expired reservations are removed and their quota released on the next reservation attempt for that scope, not by a background sweeper.

The quota denial result contains `quota_reserved: false` and `quota_state: "monthly_quota_exceeded"`; the protected action is skipped. A successful protected action's object result is annotated with `quota_reserved: true` and `quota_state: "reserved"`. Flow can branch explicitly:

```nflow
pipeline.draft_reply:cost_estimate=200000
-> match(.quota_reserved) {
  true  -> noop,
  false -> pipeline.human_review,
}
```

Only quota exhaustion becomes a false branch result. Missing tenant identity, database connectivity, reservation, commit, and audit failures remain Flow errors. This avoids accidentally presenting an infrastructure failure as a human-review quota event.

The estimate is the admission gate. If a reported actual cost exceeds the reserved estimate, committed monthly usage can exceed the configured limit and later reservations will be denied; choose a conservative estimate. Apply a retention or archival policy to old monthly budget and audit rows.

The PostgreSQL implementation persists quota usage and audit entries across process restarts and coordinates concurrent reservations through the database. It does not expose the in-memory `cost.snapshot`, `cost.report`, or `cost.record` actions; use the PostgreSQL tables or the application's reporting layer for durable usage views.

## Tenant/user prepaid wallets

Set `accounting_mode: "wallet"` with `backend: "postgres"`. Each protected action must receive top-level non-empty string fields configured by `tenant_field`, `user_field`, and `wallet_field`. The database must already contain the active tenant, user, and wallet. The adapter checks the complete ownership tuple and currency; it does not create wallets, infer balances from a global `budget`, or alter funding policy on requests. Wallet mode requires `schema_mode: "external"` and the wallet schema in [`examples/durable_quota/schema.sql`](examples/durable_quota/schema.sql).

Each reserve/settle/release operation locks the wallet row and updates the held amount, balance, and immutable financial entries transactionally. `quota_state` is `wallet_insufficient_funds` on a low-balance branch and `wallet_charged` after successful settlement. Successful debits and their Flow action classification are recorded in the same transaction. `cost_credit_wallet` provides an idempotent top-up operation for a trusted billing role; do not grant that function to the request role or accept credit authority from Flow input.

The wallet database enforces that its own balance cannot be overdrawn. It does not make third-party provider spend reversible: when provider-reported actual cost exceeds the reservation and the remaining balance cannot cover the difference, settlement errors after the external action. Bound the provider's maximum spend (and make provider/application retries idempotent) when a strict end-to-end ceiling is required. As in monthly mode, expiration is lazy on the next reservation for that wallet; provision a TTL above the longest protected-action duration.

## Runtime tenant budgets in Storage

Use `backend: "storage"` when the limit must change without redeploying the Flow. Configure only the pre-migrated SQLite `storage_path` and existing KV `storage_namespace`; do not set `budget` in `@require`. The adapter reads the current tenant policy on every reservation and stores the policy, committed usage, and active reservations in one namespaced record in Storage's existing KV table. It adds no Cost-owned SQL table. Seed or change a limit from trusted Go setup/admin code with `nexssflow.SetRuntimeBudget(ctx, kv, tenantID, limitMicros)`; never expose this helper to a Flow request.

The host must attach the authenticated tenant to context with `xctx.WithTenantID` after authentication. Cost ignores tenant IDs in Flow input. Missing or malformed policy fails closed as a Flow error before the protected action runs. Exhausted budget returns `quota_reserved: false` and `quota_state: "runtime_budget_exceeded"`; the protected action is skipped. Successful object results are marked `quota_reserved: true` and `quota_state: "runtime_reserved"`.

Each policy stores `budget_micros` as a non-negative decimal string so SQLite/JSON does not round large integers. `SetRuntimeBudget` updates that field atomically without clearing committed usage or in-flight reservations. After a reduction, new work is denied whenever committed plus reserved usage would exceed the new limit; reservations already admitted are allowed to finish and settle, so an in-flight request can leave usage above the reduced cap. The adapter does not expire in-flight reservations automatically; after a process crash, an operator must reconcile stale holds before that tenant can spend the held amount again.

As in PostgreSQL mode, the estimate is the admission gate. If an action's reported actual cost is greater than the estimate, committed usage can exceed the limit and subsequent requests will be denied; use a conservative estimate. This backend does not provide `cost.snapshot` or `cost.report`; usage lives in the Storage record and should be exposed through an authorized reporting path.

## Local memory backend and DSL

The memory backend remains the default and can be configured as follows:

```nflow
@require github.com/nexssp/cost/nexssflow { budget: "1000000", currency: "USD" }
```

`:cost_estimate=N` reserves `N` micros before the modified action. A successful result implementing `cost.Reporter` supplies the actual cost; otherwise the estimate is committed. For a request-dependent amount, pass a path such as `:cost_estimate=.policy.estimate_micros`. The path is compiled once, but its value is looked up and validated for every invocation. Paths only traverse object fields (no arbitrary expressions or code evaluation). The value must be a non-negative integer number of micros; a missing, fractional, string, or invalid value fails closed before the protected action runs.

This works with the existing Storage KV Flow bundle: a storage lookup can produce a record whose `value` contains `estimate_micros`, and the Cost modifier can read `.value.estimate_micros` from the input it receives. Configure that KV alias with a persistent backend such as `sqlite` or `postgres` when the policy must survive restarts. Storage remains the source of the value; Cost does not duplicate its schema or cache policy data at compile time. Pass `request: .` to `kv.get` to carry the original business payload through the lookup in the result's `request` field; without it, the existing KV result shape is unchanged.

```nflow
@storage kv:sqlite as policies {
  path: "/var/lib/app/cost-policy.db",
  namespace: "cost-policy-v1"
}

# The pre-provisioned row for the incoming model_key stores an integer
# estimate_micros. SQLite migrations must be applied before serving traffic.
policies.kv.get @{ key: .model_key, request: . }
-> runtime.noop:cost_estimate=.value.estimate_micros
```

The lookup result contains `value`, `hit`, and the original input under `request`. A downstream business action can consume that nested request together with the policy record (or use a small projection to map both into its own request shape).

The modifier is registered in Flow's `ModifierTable`, not through `AtomAdvise`, and is supported on pipeline actions and projections in Flow v0.20.3.

In memory mode the adapter also provides:

- `cost.snapshot`: current limit, remaining, used, spent, and currency counters.
- `cost.report`: counters and audit-ring aggregates by `(domain, operation)`. Buckets are first-seen ordered for sequential actions; parallel order is nondeterministic. The ledger retains at most 1,024 events.
- `cost.record`: append a post-hoc audit event. It does not reserve quota or change snapshot counters.

For example, the output after `cost.report` has report fields (including `spent_micros` and `buckets`); it is not the ticket/input object. Keep assertions on the final shape they actually consume:

```nflow
@assert: result.spent_micros == 150000
runtime.const @{ value: "completed" }:cost_estimate=150000
-> cost.report
```

## Lifecycle notes

- Invalid options, currencies, backends, durations, and durable quota settings are rejected during bundle construction.
- Negative or malformed estimates are rejected before execution.
- Memory bundle ledgers and their audit rings are process-local; PostgreSQL is the durable, multi-process option.
- The PostgreSQL bundle owns its SQL connection pool and closes it through Flow's bundle shutdown lifecycle.
- The local `NewBundle(ledger)` API remains available for hosts that directly own a memory ledger. For external durable backends, configure the bundle via `@require` rather than constructing storage in the Flow host.
