module github.com/nexssp/cost/examples

go 1.25.14

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/nexssp/cost v0.0.0
	github.com/nexssp/cost/adapters/kernel v0.0.0
	github.com/nexssp/cost/adapters/postgres v0.0.0
	github.com/nexssp/cost/adapters/redis v0.0.0
	github.com/nexssp/kernel v0.6.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/nexssp/cost => ..

replace github.com/nexssp/cost/adapters/kernel => ../adapters/kernel

replace github.com/nexssp/cost/adapters/redis => ../adapters/redis

replace github.com/nexssp/cost/adapters/postgres => ../adapters/postgres
