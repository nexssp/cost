# Semantics

All amounts are integer micro-units. A positive reservation increases current usage; release removes the reservation; commit replaces the reservation with actual usage. A limit below zero means unlimited.

A reservation is terminal. After a successful commit or release, later calls are no-ops. If a terminal call returns an error, the caller must treat the outcome as unknown and use the backend's idempotency and reconciliation mechanism before retrying a business operation.

`Commit` is the operation that updates the local `SpentMicros` aggregate. `Record` is audit-only: it appends an event to the local ring and does not change `SpentMicros`. This separation prevents recording the event associated with a commit from double-counting spend. Durable adapters persist the event and its ID; event IDs should be stable across application retries. The core local ledger stores a bounded in-memory audit ring; it is not a durable audit log.

The Kernel adapter uses a fresh `context.Background()` with a five-second timeout for terminal `Commit` and `Release` operations. This intentionally isolates accounting cleanup from a canceled request, so those terminal operations cannot be canceled by request shutdown. Use the core `Reserver` contract directly when the caller must retain terminal operation control and errors.

The local implementation is optimized for low latency and bounded memory. Redis and PostgreSQL include network, serialization, locking, and persistence costs. No zero-allocation claim applies to those adapters.
