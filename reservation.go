package cost

import (
	"context"
	"sync/atomic"
)

type localReservation struct {
	ledger   *Ledger
	reserved int64
	done     atomic.Bool
}

func (r *localReservation) Commit(ctx context.Context, actualMicros int64) error {
	if ctx == nil {
		return ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if actualMicros < 0 {
		actualMicros = 0
	}

	if !r.done.CompareAndSwap(false, true) {
		return nil
	}

	r.ledger.usedMicros.Add(actualMicros - r.reserved)

	if actualMicros > 0 {
		r.ledger.spentMicros.Add(actualMicros)
	}

	return nil
}

func (r *localReservation) Release(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if !r.done.CompareAndSwap(false, true) {
		return nil
	}

	r.ledger.usedMicros.Add(-r.reserved)

	return nil
}
