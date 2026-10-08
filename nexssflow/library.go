// Package nexssflow adapts Nexss Cost to Nexss Flow. The bundle creates
// a local in-process ledger from @require options and registers the
// :cost_estimate modifier, which installs a GuardAction hook that
// reserves the estimate before the target runs and commits or releases
// the reservation afterwards.
package nexssflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nexssp/cost"
	kernelcost "github.com/nexssp/cost/adapters/kernel"
	"github.com/nexssp/flow/core"
	"github.com/nexssp/kernel/action"
)

const ID = "cost"

func init() {
	core.Register(ID, Bundle)
}

// Config controls the local ledger. Budget is in micro-units: one unit
// of the currency equals 1,000,000 micros. A negative budget means
// unlimited.
type Config struct {
	Budget   int64  `flow:"budget" default:"-1"`
	Currency string `flow:"currency" default:"USD"`
}

// Bundle is the standard Flow bundle contract.
func Bundle(opts map[string]string) core.Bundle {
	cfg, err := core.Decode[Config](opts)
	if err != nil {
		panic("cost: " + err.Error())
	}

	ledger := cost.NewLedger(cfg.Budget, parseCurrency(cfg.Currency))

	return core.Bundle{
		ID: ID,
		Libraries: []action.Library{
			{Name: ID, Actions: []action.AnyAction{
				snapshotAction(ledger),
				reportAction(ledger),
			}},
		},

		Modifiers: []core.Modifier{costEstimateModifier(ledger)},
	}
}

// costEstimateModifier registers :cost_estimate=N as a real modifier so
// it applies to every primary — atoms, projections, loops, matches. The
// modifier table pre-validates that N parses as int64; the modifier
// further rejects negative values.
func costEstimateModifier(ledger cost.Reserver) core.Modifier {
	return core.Modifier{
		Name:      "cost_estimate",
		Owner:     core.OwnerBundle,
		ValueKind: core.ModifierKindInt64,
		Apply: func(b *action.Builder[any, any], raw string) error {
			estimate, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("parse int64: %w", err)
			}
			if estimate < 0 {
				return errors.New("estimate must be non-negative")
			}
			b.AnyHook(kernelcost.GuardAction(ledger, estimate))
			return nil
		},
	}
}

// Snapshot is the shape returned by cost.snapshot.
type Snapshot struct {
	LimitMicros int64  `json:"limit_micros"`
	UsedMicros  int64  `json:"used_micros"`
	SpentMicros int64  `json:"spent_micros"`
	Currency    string `json:"currency"`
}

func snapshotAction(ledger *cost.Ledger) action.AnyAction {
	return action.New(ID+".snapshot", func(_ context.Context, _ struct{}) (Snapshot, error) {
		return Snapshot{
			LimitMicros: ledger.LimitMicros(),
			UsedMicros:  ledger.UsedMicros(),
			SpentMicros: ledger.SpentMicros(),
			Currency:    ledger.Currency().String(),
		}, nil
	}).Description("Return the current local ledger snapshot").Build()
}

func parseCurrency(code string) cost.Currency {
	var c cost.Currency
	if err := c.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(code)))); err != nil {
		panic("cost: " + err.Error())
	}
	return c
}
