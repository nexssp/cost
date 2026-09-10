# Nexss Cost

`github.com/nexssp/cost` is a small, dependency-free Go library for fixed-point budget reservation and cost recording. The core module contains the local ledger and backend-neutral contracts only.

## Modules

| Module | Use | Dependencies |
|---|---|---|
| `github.com/nexssp/cost` | Local in-process budgets and contracts | Standard library only |
| `github.com/nexssp/cost/adapters/redis` | Distributed Redis budgets | `go-redis/v9` |
| `github.com/nexssp/cost/adapters/postgres` | Transactional PostgreSQL budgets | `database/sql` only |
| `github.com/nexssp/cost/adapters/kernel` | Nexss Kernel action integration | Nexss Kernel |
| `github.com/nexssp/cost/testkit` | Adapter contract helper | Core module |

Examples are a separate module under [`examples/`](examples/), so applications do not inherit example dependencies.

## Core usage

```go
ledger := cost.NewLedger(10_000_000, cost.USD)

reservation, err := ledger.Reserve(ctx, 1_000_000)
if err != nil {
	return err
}

if err := reservation.Commit(ctx, 800_000); err != nil {
	return err
}

err = ledger.Record(ctx, cost.Event{
	ID:         "request-123",
	Domain:     "ai",
	Operation:  "completion",
	CostMicros: 800_000,
	Currency:   cost.USD,
})
if err != nil {
	return err
}
```

`Commit` and `Release` return errors. A reservation is idempotent after its first successful terminal operation. Always handle the returned error; distributed backends can fail after the business operation completes.

**Accounting distinction:** on the local ledger, `Commit` updates `SpentMicros` and `Record` does **not** update it. `Record` is an audit operation only: it appends the event to the bounded audit ring. Record the event associated with a commit for traceability, but do not use `Record` as the spend counter.

## Optional adapters

Install only the adapter you use:

```bash
go get github.com/nexssp/cost/adapters/redis
go get github.com/nexssp/cost/adapters/postgres
go get github.com/nexssp/cost/adapters/kernel
```

### Redis

```go
ledger := redisadapter.New(redisadapter.Config{
    Client: client,
    Prefix: "{cost}:checkout", // one Redis Cluster hash slot
    LimitMicros: 10_000_000,
    Currency: cost.USD,
})
```

Redis uses atomic Lua scripts, expiring leases, and lazy reclamation. Set a TTL longer than the longest expected operation. Redis is a coordination backend; keep durable audit records separately when required.

### PostgreSQL

The PostgreSQL adapter accepts `*sql.DB`, not a concrete driver:

```go
ledger := postgresadapter.New(postgresadapter.Config{
    DB: db,
    Scope: "checkout",
    LimitMicros: 10_000_000,
    Currency: cost.USD,
})
if err := ledger.EnsureSchema(ctx); err != nil { return err }
```

Applications choose pgx, lib/pq, pooling, tracing, and migrations. In production, run the exported `postgres.Schema` through your migration system rather than creating schema during request handling.

## Implementing another backend

A custom backend implements only these contracts:

```go
type Reserver interface {
    Reserve(context.Context, int64) (Reservation, error)
    Record(context.Context, Event) error
}

type Reservation interface {
    Commit(context.Context, int64) error
    Release(context.Context) error
}
```

Required guarantees:

1. `Reserve` is atomic against the configured limit.
2. Failed reservations consume no budget.
3. `Commit` and `Release` are idempotent after a successful terminal operation.
4. Context cancellation is respected before and during I/O.
5. Expired or abandoned reservations cannot permanently consume budget.
6. Backend failures are returned, never silently reported as success.
7. Currency and scope are validated.
8. Concurrent callers cannot exceed the limit.
9. Recording is explicit and retry behavior is documented.

Use [`github.com/nexssp/cost/testkit`](testkit/) as the starting point for adapter contract tests. Backend-specific behavior—leases, transactions, retries, consistency, and durability—must be documented by the adapter.

## Documentation map

Start with [`docs/architecture.md`](docs/architecture.md) for module boundaries, [`docs/semantics.md`](docs/semantics.md) for accounting and failure semantics, and [`docs/extending.md`](docs/extending.md) for custom adapters. Runnable examples and infrastructure commands are in [`examples/README.md`](examples/README.md).

## Semantics and performance

Amounts are integer micro-units: one unit equals 1,000,000 micros. The local ledger uses atomics for reservation accounting and a fixed 1024-entry ring for bounded audit memory. Its `Entries` snapshot allocates because it returns ownership-safe data; reservation hot paths do not use locks.

Network and database adapters necessarily include I/O, serialization, backend transactions, and failure handling. They are not zero-allocation paths. Measure local, Redis, and PostgreSQL paths separately under your workload rather than applying one performance claim to all backends.

## Validation

```bash
go test ./...
go test -race ./...
go vet ./...
(cd adapters/redis && go test -race ./...)
(cd adapters/postgres && go test ./...)
(cd adapters/kernel && go test ./...)
(cd testkit && go test ./...)
(cd examples && go test ./... && go build ./...)
```

## License

Apache License 2.0
