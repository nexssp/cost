# Durable prepaid wallets for tenant/user workflows

This is a PostgreSQL-backed NFlow example for a multi-tenant support service where **each user has a wallet inside a tenant**. The Flow file configures the adapter with `@require`; the generated Go host remains a generic Flow runner and does not construct or select the accounting backend.

Unlike the older fixed monthly-quota model, wallet mode does not hand every tenant the same configured budget and does not create or reset accounts on requests. Tenant, user, wallet, currency, balance, status, and opening credits are provisioned in the database. The adapter accepts a wallet only when the request's `(tenant_id, user_id, wallet_id)` tuple matches an active wallet owned by an active user in an active tenant.

## What happens on a request

1. Before `pipeline.draft_reply` runs, `:cost_estimate=200000` (USD 0.20) is reserved from the identified wallet in a PostgreSQL transaction. The wallet row is locked so concurrent requests cannot reserve the same available funds.
2. If available funds are insufficient, the paid step is skipped and Flow routes to `pipeline.human_review` with `quota_reserved: false` and `quota_state: "wallet_insufficient_funds"`.
3. On success, settlement atomically releases the hold, debits the actual reported cost (or the estimate if the action does not implement `cost.Reporter`), and writes an immutable debit entry with the Flow action name. The result has `quota_reserved: true` and `quota_state: "wallet_charged"`.
4. Failed actions release their hold; database, identity, suspended-account, and settlement failures remain errors. They are not mislabeled as low balance.
5. Reservations whose lease expires are reclaimed on the next reservation attempt for that wallet. There is no background sweeper.

The schema also provides an idempotent `cost_credit_wallet(...)` function for a trusted billing/top-up service. The request adapter never creates wallets or credits them. The optional demo seed gives Acme's Alex USD 0.50, Acme's Sam USD 0.15, and Beta's Maria USD 1.00. With a USD 0.20 estimate, Alex can make two calls before the third is routed to review; Sam is denied immediately; Maria has an independent balance.

## Configure and run

1. Set `COST_POSTGRES_DSN` to a PostgreSQL connection string for a database the process can reach. Keep credentials in the environment or deployment secret store, not in the `.nflow` file.
2. Apply [`schema.sql`](schema.sql) once through the database migration process. It creates tenant/user/wallet tables, transactional holds, immutable wallet entries, optional usage events, and the trusted top-up function; it does not create demo accounts. For a local/example database, then load [`seed.sql`](seed.sql), which inserts Acme/Beta sample accounts and opening balances idempotently:

   ```sh
   psql "$COST_POSTGRES_DSN" -v ON_ERROR_STOP=1 -f schema.sql
   psql "$COST_POSTGRES_DSN" -v ON_ERROR_STOP=1 -f seed.sql
   ```

   Do not apply the sample seed to a production database. Provision production identities and wallets through the application's migration/onboarding process.

3. From a Nexss Flow v0.20.3 checkout, build the example:

   ```sh
   go run ./cmd/nflow build /workspace/cost/nexssflow/examples/durable_quota/support_ticket.nflow \
     -o /workspace/cost/nexssflow/examples/durable_quota/support_ticket
   ```

   The repository-local `@require ../../../` resolves to this checkout's `nexssflow` adapter. For a released deployment, use the published adapter path and pin its version.
4. Run the generated program with one JSON object piped on stdin, as shown below, or embed the Flow source in the application's generic Flow harness and pass it the authenticated request object.

The compiled CLI accepts one JSON request object on stdin; the Flow source selects request fields from runtime input rather than hard-coding them. For example:

```sh
printf '%s\n' '{"tenant_id":"tn_acme","user_id":"usr_alex","wallet_id":"wal_acme_alex_usd","ticket_id":"ticket_2026_1042","subject":"Cannot export report","message":"CSV export returns an empty file."}' \
  | /workspace/cost/nexssflow/examples/durable_quota/support_ticket
```

The generated workflow demonstrates the real PostgreSQL wallet reservation/settlement path; its `draft_reply` is deliberately a deterministic stand-in because this repository does not include a support/AI provider action. Replace that step with the application's real chargeable action. **Derive tenant, user, and wallet identity from authenticated server-side context**; do not trust client-provided identity fields.

## Wallet funding and deployment permissions

Use a dedicated migration owner to install the schema (and, only for a demo database, the seed data). This least-privilege grant set is sufficient for the request adapter; replace `app_runtime` with the deployment's database role:

```sql
GRANT USAGE ON SCHEMA public TO app_runtime;
GRANT SELECT ON cost_tenants, cost_users, cost_wallets TO app_runtime;
GRANT UPDATE (balance_micros, reserved_micros, updated_at) ON cost_wallets TO app_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON cost_wallet_reservations TO app_runtime;
GRANT INSERT ON cost_wallet_entries TO app_runtime;
GRANT USAGE, SELECT ON SEQUENCE cost_wallet_entries_id_seq TO app_runtime;
```

Do **not** grant the request role permission to execute `cost_credit_wallet`; the migration explicitly revokes PostgreSQL's default `PUBLIC` execute grant. Grant execution only to the trusted billing/top-up role, which also needs underlying table and sequence permissions because the function runs with invoker privileges:

```sql
GRANT USAGE ON SCHEMA public TO billing_topup;
GRANT SELECT, UPDATE ON cost_wallets TO billing_topup;
GRANT SELECT, INSERT ON cost_wallet_entries TO billing_topup;
GRANT USAGE, SELECT ON SEQUENCE cost_wallet_entries_id_seq TO billing_topup;
GRANT EXECUTE ON FUNCTION cost_credit_wallet(TEXT, BIGINT, TEXT, TEXT) TO billing_topup;
```

Keep application-level authorization in place as well as the database ownership check.

Top up a wallet from that trusted service with a stable key for the credit transaction. Repeating the same `(wallet_id, idempotency_key, amount)` is a no-op; reusing the key with a different amount or entry type fails:

```sql
SELECT cost_credit_wallet(
  'wal_acme_alex_usd',
  500000,
  'invoice:inv_2026_1042',
  'invoice_paid'
);
```

All amounts are integer micros (1 USD = 1,000,000 micros). Wallets are single-currency; the adapter rejects currency mismatches. Suspended or unknown tenants/users/wallets are operational errors; only a valid wallet that cannot cover the reservation takes the low-balance branch.

## Production boundaries to preserve

- Set `reservation_ttl` longer than the maximum protected-action duration. Expired holds are reclaimed lazily on the next request for that wallet.
- The database guarantees that **wallet debits cannot overdraw the wallet**. It cannot make a third-party provider's spend reversible: if actual provider cost exceeds the estimate and the remaining wallet funds cannot cover it, settlement fails after the external action has already run. Use a conservative estimate and enforce the provider's own max-token/price limit if a strict end-to-end spend ceiling is required.
- The sample reports actual spend only when the protected action returns a `cost.Reporter`; otherwise the estimate is charged. Connect provider usage reporting before treating estimates as actual billing.
- The accounting transaction is durable and multi-process safe, but it is not a distributed transaction with the provider. Make the provider action and the application request/retry path idempotent using the provider's/request system's own idempotency keys.
- Retain/archive wallet entry and usage-event data according to the service's financial and audit requirements. Never edit balances directly; use an audited top-up/refund workflow.

## Adapter configuration

The relevant settings in `support_ticket.nflow` are:

```nflow
@require ../../../ {
  backend: "postgres",
  dsn: "$COST_POSTGRES_DSN",
  schema_mode: "external",
  accounting_mode: "wallet",
  currency: "USD",
  tenant_field: "tenant_id",
  user_field: "user_id",
  wallet_field: "wallet_id",
  reservation_ttl: "10m"
}
```

Use the unbraced `$COST_POSTGRES_DSN` form in this Flow block; its current `@require` parser treats `}` as the end of the option block.

PostgreSQL `accounting_mode: "monthly"` remains available for fixed tenant/month quotas. `accounting_mode: "wallet"` uses provisioned balances and requires the external wallet schema; wallet mode deliberately does not accept a global `budget` as a per-tenant funding policy.
