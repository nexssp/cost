package kernel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/cost"
	kernelcost "github.com/nexssp/cost/adapters/kernel"
	"github.com/nexssp/kernel/action"
)

type reportedResult struct {
	cost int64
}

func (r reportedResult) CostMicros() int64 {
	return r.cost
}

func TestGuardActionSuccessWithReporter(t *testing.T) {
	ledger := cost.NewLedger(100_000, cost.USD)
	act := action.New("orders.checkout", func(ctx context.Context, in string) (reportedResult, error) {
		return reportedResult{cost: 30_000}, nil
	}).
		AnyHook(kernelcost.GuardAction(ledger, 50_000)).
		Build()

	res, err := act.Do(context.Background(), "input")
	if err != nil {
		t.Fatalf("unexpected action failure: %v", err)
	}

	if res.cost != 30_000 {
		t.Fatalf("unexpected result cost: %d", res.cost)
	}

	if ledger.UsedMicros() != 30_000 || ledger.SpentMicros() != 30_000 {
		t.Fatalf("used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}

	entries := ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(entries))
	}
	if entries[0].Domain != "orders" || entries[0].Operation != "checkout" {
		t.Fatalf("unexpected event routing: domain=%s op=%s", entries[0].Domain, entries[0].Operation)
	}
}

func TestGuardActionFailureReleasesReservation(t *testing.T) {
	ledger := cost.NewLedger(100_000, cost.USD)
	act := action.New("orders.fail", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("database down")
	}).
		AnyHook(kernelcost.GuardAction(ledger, 50_000)).
		Build()

	_, err := act.Do(context.Background(), "input")
	if err == nil {
		t.Fatal("expected action error")
	}

	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
		t.Fatalf("expected reservation release, used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestGuardActionPanicReleasesReservation(t *testing.T) {
	ledger := cost.NewLedger(100_000, cost.USD)
	act := action.New("orders.crash", func(ctx context.Context, in string) (string, error) {
		panic("fatal unhandled condition")
	}).
		AnyHook(kernelcost.GuardAction(ledger, 50_000)).
		Build()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
		if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
			t.Fatalf("expected reservation release on panic, used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
		}
	}()

	_, _ = act.Do(context.Background(), "input")
}

func TestGuardActionCancelReleasesReservation(t *testing.T) {
	ledger := cost.NewLedger(100_000, cost.USD)
	ctx, cancel := context.WithCancel(context.Background())

	act := action.New("orders.cancel", func(ctx context.Context, in string) (string, error) {
		cancel() // Cancel context mid-flight
		return "", ctx.Err()
	}).
		AnyHook(kernelcost.GuardAction(ledger, 50_000)).
		Build()

	_, _ = act.Do(ctx, "input")

	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
		t.Fatalf("expected reservation release on cancel, used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestGuardActionNilMetaSafety(t *testing.T) {
	ledger := cost.NewLedger(100_000, cost.USD)
	hook := kernelcost.GuardAction(ledger, 20_000)

	ctx, err := hook.Before(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Invoke After with nil meta; must not panic
	hook.After(ctx, nil, reportedResult{cost: 10_000}, nil, nil)

	if ledger.UsedMicros() != 10_000 {
		t.Fatalf("expected used=10000, got %d", ledger.UsedMicros())
	}

	entries := ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Domain != "action" || entries[0].Operation != "action" {
		t.Fatalf("expected default domain/op 'action', got %s.%s", entries[0].Domain, entries[0].Operation)
	}
}
