package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nexssp/cost"
	postgresadapter "github.com/nexssp/cost/adapters/postgres"
	"github.com/nexssp/cost/testkit"
)

func getDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("COST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set COST_POSTGRES_DSN to run PostgreSQL integration tests")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestLedgerContract(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	testkit.RunContract(t, func(t *testing.T) cost.Reserver {
		scope := "contract-" + time.Now().UTC().Format("20060102150405.000000000")
		l := postgresadapter.New(postgresadapter.Config{
			DB:          db,
			Scope:       scope,
			LimitMicros: 100,
			Currency:    cost.USD,
		})
		if err := l.EnsureSchema(context.Background()); err != nil {
			t.Fatal(err)
		}
		return l
	})
}

func TestLedgerIntegration(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	ctx := context.Background()
	scope := "adapter-" + time.Now().UTC().Format("20060102150405.000000000")

	ledger := postgresadapter.New(postgresadapter.Config{
		DB:             db,
		Scope:          scope,
		LimitMicros:    1_000_000,
		Currency:       cost.USD,
		ReservationTTL: time.Second,
	})

	if err := ledger.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	reservation, err := ledger.Reserve(ctx, 400_000)
	if err != nil {
		t.Fatal(err)
	}

	if err := reservation.Commit(ctx, 250_000); err != nil {
		t.Fatal(err)
	}

	second, err := ledger.Reserve(ctx, 500_000)
	if err != nil {
		t.Fatal(err)
	}

	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := ledger.Reserve(ctx, 800_001); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestLedgerReclaimOnFullBudget(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	ctx := context.Background()
	scope := "reclaim-" + time.Now().UTC().Format("20060102150405.000000000")

	ledger := postgresadapter.New(postgresadapter.Config{
		DB:             db,
		Scope:          scope,
		LimitMicros:    1_000_000,
		Currency:       cost.USD,
		ReservationTTL: time.Second,
	})
	if err := ledger.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// Reserve 100% of budget
	_, err := ledger.Reserve(ctx, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}

	// Wait 2s for reservation to expire in PostgreSQL
	time.Sleep(2 * time.Second)

	// Attempting another reservation must reclaim expired row and succeed
	r2, err := ledger.Reserve(ctx, 600_000)
	if err != nil {
		t.Fatalf("expected expired reservation reclamation on full budget, got: %v", err)
	}

	if err := r2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerIdempotentRecord(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	ctx := context.Background()
	scope := "record-" + time.Now().UTC().Format("20060102150405.000000000")

	ledger := postgresadapter.New(postgresadapter.Config{
		DB:          db,
		Scope:       scope,
		LimitMicros: 1_000_000,
		Currency:    cost.USD,
	})
	if err := ledger.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	event := cost.Event{
		ID:         "stable-order-id-123",
		Domain:     "checkout",
		Operation:  "pay",
		CostMicros: 50_000,
		Currency:   cost.USD,
	}

	// First insert
	if err := ledger.Record(ctx, event); err != nil {
		t.Fatal(err)
	}

	// Retry insert with same event.ID; must be a no-op and not fail with unique violation
	if err := ledger.Record(ctx, event); err != nil {
		t.Fatalf("expected idempotent Record with same event ID, got: %v", err)
	}

	var count int
	err := db.QueryRowContext(ctx, "SELECT count(*) FROM cost_entries WHERE scope = $1 AND event_id = $2", scope, event.ID).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 entry in cost_entries, got %d", count)
	}
}

func TestLedgerCurrencyMismatchEnsureSchema(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	ctx := context.Background()
	scope := "cur-mismatch-" + time.Now().UTC().Format("20060102150405.000000000")

	l1 := postgresadapter.New(postgresadapter.Config{
		DB:          db,
		Scope:       scope,
		LimitMicros: 100_000,
		Currency:    cost.EUR,
	})
	if err := l1.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// Attempting EnsureSchema on existing scope with USD must fail
	l2 := postgresadapter.New(postgresadapter.Config{
		DB:          db,
		Scope:       scope,
		LimitMicros: 100_000,
		Currency:    cost.USD,
	})
	if err := l2.EnsureSchema(ctx); err == nil {
		t.Fatal("expected currency mismatch error when re-initializing scope")
	}
}

func TestLedgerConcurrentLocking(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	ctx := context.Background()
	scope := "concurrent-" + time.Now().UTC().Format("20060102150405.000000000")

	ledger := postgresadapter.New(postgresadapter.Config{
		DB:          db,
		Scope:       scope,
		LimitMicros: 10_000,
		Currency:    cost.USD,
	})
	if err := ledger.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	reservations := make([]cost.Reservation, 0, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := ledger.Reserve(context.Background(), 500)
			if err == nil {
				mu.Lock()
				reservations = append(reservations, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(reservations) != 20 {
		t.Fatalf("expected exactly 20 successful reservations (10_000 / 500), got %d", len(reservations))
	}

	for _, r := range reservations {
		if err := r.Commit(context.Background(), 500); err != nil {
			t.Fatal(err)
		}
	}
}
