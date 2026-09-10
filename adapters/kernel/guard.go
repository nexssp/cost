package kernel

import (
	"context"
	"time"

	"github.com/nexssp/cost"
	"github.com/nexssp/kernel/action"
)

type reservationKey struct{}

// GuardAction integrates any cost.Reserver with a Kernel action. The adapter
// reserves before execution, releases on failure/cancel/panic, and records a
// successful reported cost. Terminal accounting is deliberately isolated from
// the request context: Commit and Release use a fresh background context with
// a five-second timeout, so a canceled request cannot strand a distributed
// reservation. Applications that need terminal errors returned to their caller
// should use cost.Reserver directly rather than a void hook.
func GuardAction(ledger cost.Reserver, estimateMicros int64) action.AnyHook {
	return action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			if ledger == nil {
				return ctx, cost.ErrNilContext
			}
			reservation, err := ledger.Reserve(ctx, estimateMicros)
			if err != nil {
				return ctx, err
			}
			return context.WithValue(ctx, reservationKey{}, reservation), nil
		},

		After: func(ctx context.Context, _, result any, actionErr error, meta *action.Meta) {
			reservation, ok := ctx.Value(reservationKey{}).(cost.Reservation)
			if !ok {
				return
			}

			if actionErr != nil {
				_ = finishContext(reservation, false, 0)
				return
			}

			actual := estimateMicros
			if reporter, ok := result.(cost.CostReporter); ok {
				actual = reporter.CostMicros()
				if actual < 0 {
					actual = 0
				}
			}

			if err := finishContext(reservation, true, actual); err == nil && actual > 0 {
				metaName := ""
				if meta != nil {
					metaName = meta.Name
				}
				_ = ledger.Record(context.Background(), cost.Event{
					Domain:     domain(metaName),
					Operation:  operation(metaName),
					CostMicros: actual,
				})
			}
		},

		OnPanic: func(ctx context.Context, _, _ any, _ *action.Meta) {
			release(ctx)
		},

		OnCancel: func(ctx context.Context, _ any, _ *action.Meta) {
			release(ctx)
		},
	}
}

func finishContext(reservation cost.Reservation, commit bool, actual int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if commit {
		return reservation.Commit(ctx, actual)
	}
	return reservation.Release(ctx)
}

func release(ctx context.Context) {
	if reservation, ok := ctx.Value(reservationKey{}).(cost.Reservation); ok {
		_ = finishContext(reservation, false, 0)
	}
}

func domain(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[:i]
		}
	}
	return "action"
}

func operation(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[i+1:]
		}
	}
	if name == "" {
		return "action"
	}
	return name
}
