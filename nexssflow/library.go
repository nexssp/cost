// Package nexssflow adapts Nexss Cost to Nexss Flow. The bundle configures its
// storage backend through @require and registers :cost_estimate as a Flow
// modifier that reserves before execution and settles afterward.
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

// Config controls the cost adapter. Budget and estimates are integer
// micro-units: one unit of the currency equals 1,000,000 micros. Backend
// defaults to the bounded in-memory ledger; PostgreSQL requires a DSN and
// supports fixed tenant/month quotas or provisioned tenant/user wallets.
type Config struct {
	Backend        string `flow:"backend" default:"memory"`
	Budget         int64  `flow:"budget" default:"-1"`
	Currency       string `flow:"currency" default:"USD"`
	DSN            string `flow:"dsn"`
	SchemaMode     string `flow:"schema_mode" default:"external"`
	AccountingMode string `flow:"accounting_mode" default:"monthly"`
	ScopePrefix    string `flow:"scope_prefix" default:"support-quota"`
	TenantField    string `flow:"tenant_field" default:"customer_id"`
	UserField      string `flow:"user_field" default:"user_id"`
	WalletField    string `flow:"wallet_field" default:"wallet_id"`
	ReservationTTL string `flow:"reservation_ttl" default:"5m"`
	MaxOpenConns   int    `flow:"max_open_conns" default:"10"`
	MaxIdleConns   int    `flow:"max_idle_conns" default:"5"`
}

var acceptedOptions = []string{
	"backend", "budget", "currency", "dsn", "schema_mode", "accounting_mode", "scope_prefix",
	"tenant_field", "user_field", "wallet_field", "reservation_ttl", "max_open_conns", "max_idle_conns",
}

var postgresOnlyOptions = []string{
	"dsn", "schema_mode", "accounting_mode", "scope_prefix", "tenant_field", "user_field",
	"wallet_field", "reservation_ttl", "max_open_conns", "max_idle_conns",
}

// Bundle is the standard Flow bundle contract. Every backend choice and
// setting is supplied by the importing .nflow file's @require options.
func Bundle(opts map[string]string) core.Bundle {
	cfg, err := core.Decode[Config](opts)
	if err != nil {
		panic("cost: " + err.Error())
	}
	currency := parseCurrency(cfg.Currency)

	switch strings.ToLower(strings.TrimSpace(cfg.Backend)) {
	case "memory":
		for _, key := range postgresOnlyOptions {
			if _, ok := opts[key]; ok {
				panic(fmt.Sprintf("cost: option %q requires backend=postgres", key))
			}
		}
		bundle := NewBundle(cost.NewLedger(cfg.Budget, currency))
		bundle.AcceptedOptions = acceptedOptions
		return bundle
	case "postgres":
		if strings.EqualFold(strings.TrimSpace(cfg.AccountingMode), accountingModeWallet) {
			if _, ok := opts["budget"]; ok {
				panic("cost: budget is not accepted with accounting_mode=wallet; provision wallet balances in PostgreSQL")
			}
		}
		store, err := newPostgresStore(cfg, currency)
		if err != nil {
			panic("cost: " + err.Error())
		}
		return postgresBundle(store)
	default:
		panic(fmt.Sprintf("cost: unsupported backend %q (want memory or postgres)", cfg.Backend))
	}
}

// NewBundle mounts a host-owned local ledger into Flow. It is useful when a
// host needs direct access to local ledger counters or audit entries.
func NewBundle(ledger *cost.Ledger) core.Bundle {
	if ledger == nil {
		panic("cost: nil ledger")
	}
	return core.Bundle{
		ID: ID,
		Libraries: []action.Library{{Name: ID, Actions: []action.AnyAction{
			snapshotAction(ledger),
			reportAction(ledger),
			recordAction(ledger),
		}}},
		Modifiers: []core.Modifier{costEstimateModifier(ledger)},
	}
}

func costEstimateModifier(ledger cost.Reserver) core.Modifier {
	return costEstimateModifierWithApply(func(b *action.Builder[any, any], estimate int64) {
		b.AnyHook(kernelcost.GuardAction(ledger, estimate))
	})
}

func costEstimateModifierWithApply(apply func(*action.Builder[any, any], int64)) core.Modifier {
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
			apply(b, estimate)
			return nil
		},
	}
}

// Snapshot is the shape returned by cost.snapshot in memory mode.
type Snapshot struct {
	LimitMicros     int64  `json:"limit_micros"`
	RemainingMicros int64  `json:"remaining_micros"`
	UsedMicros      int64  `json:"used_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	Currency        string `json:"currency"`
}

// RecordResult is returned by cost.record after Ledger.Record accepts an event.
// Recording is audit-only and does not change ledger usage or spent counters.
type RecordResult struct {
	Recorded bool `json:"recorded"`
}

func snapshotAction(ledger *cost.Ledger) action.AnyAction {
	return action.New(ID+".snapshot", func(_ context.Context, _ any) (Snapshot, error) {
		limit := ledger.LimitMicros()
		remaining := int64(-1)
		if limit >= 0 {
			remaining = limit - ledger.UsedMicros()
		}
		return Snapshot{
			LimitMicros:     limit,
			RemainingMicros: remaining,
			UsedMicros:      ledger.UsedMicros(),
			SpentMicros:     ledger.SpentMicros(),
			Currency:        ledger.Currency().String(),
		}, nil
	}).Description("Return the current local ledger snapshot").Build()
}

func recordAction(ledger *cost.Ledger) action.AnyAction {
	return action.New(ID+".record", func(ctx context.Context, event cost.Event) (RecordResult, error) {
		if err := ledger.Record(ctx, event); err != nil {
			return RecordResult{}, err
		}
		return RecordResult{Recorded: true}, nil
	}).Description("Append a post-hoc cost event to the local audit ring").Build()
}

func parseCurrency(code string) cost.Currency {
	var currency cost.Currency
	if err := currency.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(code)))); err != nil {
		panic("cost: " + err.Error())
	}
	return currency
}
