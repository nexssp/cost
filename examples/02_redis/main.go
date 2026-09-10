package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nexssp/cost"
	redisadapter "github.com/nexssp/cost/adapters/redis"
	redisclient "github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redisclient.NewClient(&redisclient.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		panic(fmt.Errorf("redis is unavailable at %s: %w", addr, err))
	}

	ledger := redisadapter.New(redisadapter.Config{Client: client, Prefix: "{cost-example}:checkout", LimitMicros: int64(cost.ToMicro(10)), Currency: cost.USD})

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

	fmt.Println("distributed Redis reservation committed")
}
