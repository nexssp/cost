package testkit

import (
	"context"
	"testing"

	"github.com/nexssp/cost"
)

// RunContract validates the guarantees every adapter must provide. Adapters
// should call this from their own _test.go with a backend factory configured
// with a limit of 100 micro-units and cost.USD.
func RunContract(t *testing.T, newBackend func(t *testing.T) cost.Reserver) {
	t.Helper()

	ctx := context.Background()
	backend := newBackend(t)

	reservation, err := backend.Reserve(ctx, 100)
	if err != nil {
		t.Fatalf("unexpected reserve error: %v", err)
	}

	err = reservation.Commit(ctx, 40)
	if err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	// Idempotent terminal call
	err = reservation.Commit(ctx, 90)
	if err != nil {
		t.Fatalf("expected commit idempotency, got: %v", err)
	}

	// Budget is now 40 / 100 used; 61 should fail
	_, err = backend.Reserve(ctx, 61)
	if err == nil {
		t.Fatal("expected budget rejection when limit exceeded")
	}

	// 20 should succeed (40 + 20 = 60 <= 100)
	toRelease, err := backend.Reserve(ctx, 20)
	if err != nil {
		t.Fatalf("unexpected reserve error: %v", err)
	}

	err = toRelease.Release(ctx)
	if err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}

	// Idempotent terminal call
	err = toRelease.Release(ctx)
	if err != nil {
		t.Fatalf("expected release idempotency, got: %v", err)
	}
}
