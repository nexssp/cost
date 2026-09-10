package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nexssp/cost"
	postgresadapter "github.com/nexssp/cost/adapters/postgres"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		panic(fmt.Errorf("postgres is unavailable: %w", err))
	}
	ledger := postgresadapter.New(postgresadapter.Config{DB: db, Scope: "checkout", LimitMicros: int64(cost.ToMicro(10)), Currency: cost.USD})

	if err := ledger.EnsureSchema(ctx); err != nil {
		panic(err)
	}

	reservation, err := ledger.Reserve(ctx, int64(cost.ToMicro(0.25)))
	if err != nil {
		panic(err)
	}

	if err := reservation.Commit(ctx, int64(cost.ToMicro(0.19))); err != nil {
		panic(err)
	}

	if err := ledger.Record(ctx, cost.Event{ID: "quote-1", Domain: "checkout", Operation: "quote", CostMicros: int64(cost.ToMicro(0.19)), Currency: cost.USD}); err != nil {
		panic(err)
	}

	fmt.Println("distributed PostgreSQL reservation committed")
}
