package cost_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/cost"
)

func TestLocalLifecycleAndIdempotency(t *testing.T) {
	ledger := cost.NewLedger(1_000, cost.USD)

	reservation, err := ledger.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	if err = reservation.Commit(context.Background(), 40); err != nil {
		t.Fatal(err)
	}

	// Terminal call must be idempotent
	if err = reservation.Commit(context.Background(), 80); err != nil {
		t.Fatal(err)
	}

	if ledger.UsedMicros() != 40 || ledger.SpentMicros() != 40 {
		t.Fatalf("used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}

	err = ledger.Record(context.Background(), cost.Event{
		ID:         "e1",
		Domain:     "api",
		Operation:  "call",
		CostMicros: 40,
		Currency:   cost.USD,
	})
	if err != nil {
		t.Fatal(err)
	}

	if ledger.SpentMicros() != 40 {
		t.Fatalf("spent=%d", ledger.SpentMicros())
	}
}

func TestLocalCrossTerminalIdempotency(t *testing.T) {
	ledger := cost.NewLedger(1_000, cost.USD)

	// Commit then Release
	r1, err := ledger.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	err = r1.Commit(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}

	err = r1.Release(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if ledger.UsedMicros() != 50 || ledger.SpentMicros() != 50 {
		t.Fatalf("used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}

	// Release then Commit
	r2, err := ledger.Reserve(context.Background(), 200)
	if err != nil {
		t.Fatal(err)
	}

	err = r2.Release(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = r2.Commit(context.Background(), 150)
	if err != nil {
		t.Fatal(err)
	}

	if ledger.UsedMicros() != 50 || ledger.SpentMicros() != 50 {
		t.Fatalf("used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestLocalZeroAndNegativeCommit(t *testing.T) {
	ledger := cost.NewLedger(1_000, cost.USD)

	r, err := ledger.Reserve(context.Background(), 200)
	if err != nil {
		t.Fatal(err)
	}

	// Negative commit cost is clamped to zero
	err = r.Commit(context.Background(), -50)
	if err != nil {
		t.Fatal(err)
	}

	// Used budget should revert to 0, Spent should remain 0
	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
		t.Fatalf("expected used=0 spent=0, got used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestLocalCommitExceedingEstimate(t *testing.T) {
	ledger := cost.NewLedger(1_000, cost.USD)

	r, err := ledger.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	// Actual spend higher than estimate
	err = r.Commit(context.Background(), 350)
	if err != nil {
		t.Fatal(err)
	}

	if ledger.UsedMicros() != 350 || ledger.SpentMicros() != 350 {
		t.Fatalf("expected used=350 spent=350, got used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestLocalValidationAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ledger := cost.NewLedger(100, cost.USD)
	if _, err := ledger.Reserve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}

	var nilCtx context.Context
	if _, err := ledger.Reserve(nilCtx, 1); !errors.Is(err, cost.ErrNilContext) {
		t.Fatalf("got %v", err)
	}

	if _, err := ledger.Reserve(context.Background(), -1); !errors.Is(err, cost.ErrInvalidEstimate) {
		t.Fatalf("got %v", err)
	}

	if _, err := ledger.Reserve(context.Background(), (math.MaxInt64/2)+1); !errors.Is(err, cost.ErrInvalidEstimate) {
		t.Fatalf("got %v", err)
	}

	if _, err := ledger.Reserve(context.Background(), 101); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestLocalRecordValidation(t *testing.T) {
	ledger := cost.NewLedger(1_000, cost.USD)

	var nilCtx context.Context
	if err := ledger.Record(nilCtx, cost.Event{CostMicros: 100}); !errors.Is(err, cost.ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := ledger.Record(ctx, cost.Event{CostMicros: 100}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if err := ledger.Record(context.Background(), cost.Event{CostMicros: 0}); !errors.Is(err, cost.ErrInvalidCost) {
		t.Fatalf("expected ErrInvalidCost, got %v", err)
	}

	if err := ledger.Record(context.Background(), cost.Event{CostMicros: -10}); !errors.Is(err, cost.ErrInvalidCost) {
		t.Fatalf("expected ErrInvalidCost, got %v", err)
	}

	// Currency mismatch
	err := ledger.Record(context.Background(), cost.Event{
		CostMicros: 100,
		Currency:   cost.EUR,
	})
	if err == nil {
		t.Fatal("expected currency mismatch error")
	}

	// Default timestamp assignment
	now := time.Now().UTC()
	err = ledger.Record(context.Background(), cost.Event{
		CostMicros: 100,
		Currency:   cost.USD,
	})
	if err != nil {
		t.Fatal(err)
	}

	entries := ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Timestamp.Before(now.Add(-time.Second)) {
		t.Fatal("expected automatically assigned recent timestamp")
	}
}

func TestLocalAuditRingWrapping(t *testing.T) {
	ledger := cost.NewLedger(100_000_000, cost.USD)

	// Ring capacity is 1024; insert 2500 entries to verify circular wrap-around
	for i := 1; i <= 2500; i++ {
		err := ledger.Record(context.Background(), cost.Event{
			ID:         fmt.Sprintf("e-%d", i),
			Domain:     "test",
			Operation:  "op",
			CostMicros: 10,
			Currency:   cost.USD,
		})
		if err != nil {
			t.Fatalf("failed recording event %d: %v", i, err)
		}
	}

	entries := ledger.Entries()
	if len(entries) != 1024 {
		t.Fatalf("expected capped capacity 1024, got %d", len(entries))
	}

	// Oldest retained entry should be e-1477, newest should be e-2500
	if entries[0].ID != "e-1477" {
		t.Fatalf("expected oldest entry e-1477, got %s", entries[0].ID)
	}

	if entries[1023].ID != "e-2500" {
		t.Fatalf("expected newest entry e-2500, got %s", entries[1023].ID)
	}
}

func TestLocalOverflowPrevention(t *testing.T) {
	ledger := cost.NewLedger(-1, cost.USD)
	maxAllowed := int64(math.MaxInt64 / 2)

	r1, err := ledger.Reserve(context.Background(), maxAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Release(context.Background())

	_, err = ledger.Reserve(context.Background(), maxAllowed)
	if err != nil {
		t.Fatal(err)
	}

	// Pushing past math.MaxInt64 must safely fail
	if _, err = ledger.Reserve(context.Background(), 10); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded on integer overflow, got %v", err)
	}
}

func TestLocalConcurrencyNeverExceedsLimit(t *testing.T) {
	ledger := cost.NewLedger(50_000, cost.TOK)

	var wg sync.WaitGroup
	var mu sync.Mutex

	reservations := make([]cost.Reservation, 0, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			r, err := ledger.Reserve(context.Background(), 1_000)
			if err == nil {
				mu.Lock()
				reservations = append(reservations, r)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(reservations) != 50 {
		t.Fatalf("expected exactly 50 successful reservations against 50,000 limit, got %d", len(reservations))
	}

	for _, r := range reservations {
		if err := r.Commit(context.Background(), 1_000); err != nil {
			t.Fatal(err)
		}
	}

	if ledger.UsedMicros() > ledger.LimitMicros() {
		t.Fatalf("used=%d exceeded limit=%d", ledger.UsedMicros(), ledger.LimitMicros())
	}
}

func TestValidationErrorUnwrap(t *testing.T) {
	e1 := &cost.ValidationError{Field: "estimate_micros", Value: -1}
	if !errors.Is(e1, cost.ErrInvalidEstimate) {
		t.Fatal("expected ErrInvalidEstimate unwrap")
	}

	e2 := &cost.ValidationError{Field: "cost_micros", Value: 0}
	if !errors.Is(e2, cost.ErrInvalidCost) {
		t.Fatal("expected ErrInvalidCost unwrap")
	}

	e3 := &cost.ValidationError{Field: "other", Value: nil}
	if !errors.Is(e3, cost.ErrValidation) {
		t.Fatal("expected ErrValidation unwrap")
	}

	if e1.Error() != "cost: invalid estimate_micros: -1" {
		t.Fatalf("unexpected error message: %s", e1.Error())
	}
}
