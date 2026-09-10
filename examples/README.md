# Examples

The examples are a separate Go module. They are not dependencies of the core package.

- `01_local`: core local ledger plus optional Kernel adapter.
- `02_redis`: optional Redis adapter; run with `REDIS_ADDR=localhost:6379 go run ./02_redis`.
- `03_postgres`: optional PostgreSQL adapter; run with `DATABASE_URL=... go run ./03_postgres`.

The root module remains usable with only `github.com/nexssp/cost`. Install an adapter only when needed.
