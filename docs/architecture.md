# Architecture

The repository is a small family of Go modules rather than one dependency-heavy module.

```text
github.com/nexssp/cost
├── core contracts and value types
└── local atomic ledger

adapters/redis       optional Redis implementation
adapters/postgres    optional database/sql implementation
adapters/kernel      optional Nexss Kernel hook
testkit              optional adapter contract helper
examples             separate runnable module
```

The core module has no third-party imports. An application that needs only local budgeting installs only the core module. Backend adapters depend on the core contract and their own infrastructure client. The examples module is intentionally excluded from the production dependency graph.

The `Reserver` interface is the integration boundary. It contains reservation and event recording but no provider-specific concepts. Terminal reservation methods return errors because a distributed commit or release can fail after the business operation has completed.

The local ledger uses atomic counters for the reservation hot path and a fixed-size ring for bounded audit memory. Redis uses Lua scripts to keep budget mutation and lease mutation atomic. PostgreSQL locks one budget row per scope and performs reservation changes in short transactions.
