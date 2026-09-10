# Extending with a custom backend

Implement `cost.Reserver` and `cost.Reservation` in a separate module under your own repository or under `adapters/` when it is maintained with this project.

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

## Required behavior

A backend must reserve atomically against its configured limit. A failed reservation must not consume budget. `Commit` and `Release` must be safe to retry after a successful terminal operation and must return transport or persistence errors. Context cancellation must be honored before and during backend operations.

The adapter must define how abandoned reservations expire, how retries are identified, and whether events are durable. If it cannot provide a guarantee, it must state that limitation in its README rather than silently weakening the common contract.

## Testing

Use `testkit.RunContract` from an adapter test package and add backend-specific tests for failure injection, expiry, concurrency, reconnects, and restart behavior. Use real service containers or test servers for integration tests; do not add production branches controlled by a test flag.

## Package design

Keep the adapter dependency-free from unrelated providers. Accept interfaces or standard abstractions where the backend client supports them. Keep schema and migration files with the adapter. Do not add provider imports to the core module.
