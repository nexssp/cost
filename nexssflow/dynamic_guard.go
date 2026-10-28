package nexssflow

import (
	"context"
	"log/slog"

	"github.com/nexssp/kernel/action"

	"github.com/nexssp/cost"
)

type dynamicReservationKey struct{}

type dynamicReservation struct {
	reservation cost.Reservation
	estimate    int64
}

// dynamicEstimateGuard resolves a compiled input path per invocation. The
// request value is never captured by the compiled Flow action.
func dynamicEstimateGuard(ledger cost.Reserver, spec estimateSpec) action.AnyHook {
	return action.AnyHook{
		Before: func(ctx context.Context, input any, _ *action.Meta) (context.Context, error) {
			if ctx == nil {
				return nil, cost.ErrNilContext
			}
			if ledger == nil {
				return ctx, cost.ErrNilContext
			}
			estimate, err := spec.resolve(input)
			if err != nil {
				return ctx, err
			}
			reservation, err := ledger.Reserve(ctx, estimate)
			if err != nil {
				return ctx, err
			}
			return context.WithValue(ctx, dynamicReservationKey{}, dynamicReservation{
				reservation: reservation,
				estimate:    estimate,
			}), nil
		},
		After: func(ctx context.Context, _, result any, actionErr error, meta *action.Meta) {
			state, ok := ctx.Value(dynamicReservationKey{}).(dynamicReservation)
			if !ok {
				return
			}
			if actionErr != nil {
				if err := finishReservation(ctx, state.reservation, false, 0); err != nil {
					slog.Error("failed to release dynamic cost reservation after action error", "error", err)
				}
				return
			}
			actual := state.estimate
			if reporter, ok := result.(cost.Reporter); ok {
				actual = max(reporter.CostMicros(), 0)
			}
			if err := finishReservation(ctx, state.reservation, true, actual); err != nil {
				slog.Error("failed to commit dynamic cost reservation", "error", err)
				return
			}
			if actual <= 0 {
				return
			}
			name := ""
			if meta != nil {
				name = meta.Name
			}
			domain, operation := actionDomainOperation(name)
			recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
			defer cancel()
			if err := ledger.Record(recordCtx, cost.Event{
				Domain: domain, Operation: operation, CostMicros: actual,
			}); err != nil {
				slog.Error("failed to record dynamic cost event", "error", err)
			}
		},
		OnPanic: func(ctx context.Context, _, _ any, _ *action.Meta) {
			releaseDynamicReservation(ctx)
		},
		OnCancel: func(ctx context.Context, _ any, _ *action.Meta) {
			releaseDynamicReservation(ctx)
		},
	}
}

func releaseDynamicReservation(ctx context.Context) {
	if ctx == nil {
		return
	}
	state, ok := ctx.Value(dynamicReservationKey{}).(dynamicReservation)
	if !ok {
		return
	}
	if err := finishReservation(ctx, state.reservation, false, 0); err != nil {
		slog.Error("failed to release dynamic cost reservation", "error", err)
	}
}
