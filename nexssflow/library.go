// Package nexssflow wires github.com/nexssp/cost into Nexss Flow.
//
// Two entry points, one per owner:
//
//	Bundle(opts)        Flow owns the ledger. Used by @require. The ledger
//	                    is constructed from the options block and lives for
//	                    the compiled program's lifetime. Dispatches to the
//	                    memory, postgres, or Storage policy backend.
//
//	NewBundle(ledger)   The Go host owns the ledger. Used by applications
//	                    that already hold a *cost.Ledger and want Flow to
//	                    share it. Cannot be reached via @require, because
//	                    a live ledger is not expressible in map[string]string.
//
// Both are registered under the same ID, "cost". Flow's compiler picks up
// the registration from init(); no manual wiring is needed.
package nexssflow

import (
	"context"
	"fmt"
	"strings"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/cost"
	kernelcost "github.com/nexssp/cost/adapters/kernel"
)

const ID = "cost"

// Config is the @require options block. Every field maps to one key.
type Config struct {
	Backend          string `nflow:"backend" default:"memory"`
	Budget           int64  `nflow:"budget" default:"-1"`
	Currency         string `nflow:"currency" default:"USD"`
	DSN              string `nflow:"dsn"`
	SchemaMode       string `nflow:"schema_mode" default:"external"`
	AccountingMode   string `nflow:"accounting_mode" default:"monthly"`
	ScopePrefix      string `nflow:"scope_prefix" default:"support-quota"`
	TenantField      string `nflow:"tenant_field" default:"customer_id"`
	UserField        string `nflow:"user_field" default:"user_id"`
	WalletField      string `nflow:"wallet_field" default:"wallet_id"`
	ReservationTTL   string `nflow:"reservation_ttl" default:"5m"`
	MaxOpenConns     int    `nflow:"max_open_conns" default:"10"`
	MaxIdleConns     int    `nflow:"max_idle_conns" default:"5"`
	StoragePath      string `nflow:"storage_path"`
	StorageNamespace string `nflow:"storage_namespace" default:"cost-runtime-v1"`
}

var acceptedOptions = []string{
	"backend", "budget", "currency", "dsn", "schema_mode", "accounting_mode",
	"scope_prefix", "tenant_field", "user_field", "wallet_field",
	"reservation_ttl", "max_open_conns", "max_idle_conns", "storage_path", "storage_namespace",
}

var postgresOnlyOptions = []string{
	"dsn", "schema_mode", "accounting_mode", "scope_prefix",
	"tenant_field", "user_field", "wallet_field", "reservation_ttl",
	"max_open_conns", "max_idle_conns",
}

var storageOnlyOptions = []string{"storage_path", "storage_namespace"}

func init() {
	compiler.Register(ID, Bundle)
}

// Bundle is the factory Flow calls for
// `@require github.com/nexssp/cost/nexssflow { ... }`.
//
// It constructs the ledger from the options block and returns a bundle
// whose actions and modifiers are backed by that ledger.
func Bundle(opts map[string]string) compiler.Bundle {
	cfg, err := compiler.Decode[Config](opts)
	if err != nil {
		panic("cost: " + err.Error())
	}
	currency := parseCurrency(cfg.Currency)

	switch strings.ToLower(strings.TrimSpace(cfg.Backend)) {
	case "memory":
		return memoryBundle(opts, cfg, currency)

	case "postgres":
		return configuredPostgresBundle(opts, cfg, currency)

	case "storage":
		return configuredStorageBundle(opts, cfg)

	default:
		panic(fmt.Sprintf("cost: unsupported backend %q (want memory, postgres, or storage)", cfg.Backend))
	}
}

func memoryBundle(opts map[string]string, cfg Config, currency cost.Currency) compiler.Bundle {
	rejectOptionsForBackend(opts, postgresOnlyOptions, "postgres")
	rejectOptionsForBackend(opts, storageOnlyOptions, "storage")
	return NewBundle(cost.NewLedger(cfg.Budget, currency))
}

func configuredPostgresBundle(opts map[string]string, cfg Config, currency cost.Currency) compiler.Bundle {
	rejectOptionsForBackend(opts, storageOnlyOptions, "storage")
	if strings.EqualFold(strings.TrimSpace(cfg.AccountingMode), accountingModeWallet) {
		if _, ok := opts["budget"]; ok {
			panic("cost: budget is not accepted with accounting_mode=wallet; " +
				"provision wallet balances in PostgreSQL")
		}
	}
	store, err := newPostgresStore(cfg, currency)
	if err != nil {
		panic("cost: " + err.Error())
	}
	return postgresBundle(store)
}

func configuredStorageBundle(opts map[string]string, cfg Config) compiler.Bundle {
	if _, ok := opts["budget"]; ok {
		panic("cost: budget is stored per tenant in Storage; do not set budget in @require")
	}
	rejectOptionsForBackend(opts, postgresOnlyOptions, "postgres")
	store, err := newStorageQuotaStore(cfg)
	if err != nil {
		panic("cost: " + err.Error())
	}
	return storageBundle(store)
}

func rejectOptionsForBackend(opts map[string]string, keys []string, backend string) {
	for _, key := range keys {
		if _, ok := opts[key]; ok {
			panic(fmt.Sprintf("cost: option %q requires backend=%s", key, backend))
		}
	}
}

// NewBundle wraps a caller-owned *cost.Ledger in a Flow bundle.
//
// Reach for this only when a Go host already owns a ledger and wants the
// compiled Flow program to share it. For everything reachable from a
// .nflow file, use Bundle(opts) via @require instead — the ledger is
// then constructed and owned by the bundle.
func NewBundle(ledger *cost.Ledger) compiler.Bundle {
	if ledger == nil {
		panic("cost: nil ledger")
	}
	return compiler.Bundle{
		ID: ID,
		Libraries: []action.Library{{Name: ID, Actions: []action.AnyAction{
			snapshotAction(ledger),
			reportAction(ledger),
			recordAction(ledger),
		}}},
		Modifiers:       []compiler.Modifier{costEstimateModifier(ledger)},
		AcceptedOptions: acceptedOptions,
	}
}

// ── snapshot / record ────────────────────────────────────────────────

type Snapshot struct {
	LimitMicros     int64  `json:"limit_micros"`
	RemainingMicros int64  `json:"remaining_micros"`
	UsedMicros      int64  `json:"used_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	Currency        string `json:"currency"`
}

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

// ── :cost_estimate modifier ──────────────────────────────────────────

func costEstimateModifier(ledger cost.Reserver) compiler.Modifier {
	return costEstimateModifierWithApply(func(b *action.Builder[any, any], estimate estimateSpec) {
		if estimate.dynamic() {
			b.AnyHook(dynamicEstimateGuard(ledger, estimate))
			return
		}
		b.AnyHook(kernelcost.GuardAction(ledger, estimate.literal))
	})
}

func costEstimateModifierWithApply(apply func(*action.Builder[any, any], estimateSpec)) compiler.Modifier {
	return compiler.Modifier{
		Name:  "cost_estimate",
		Owner: compiler.OwnerBundle,
		// Accept either a validated literal or a runtime request-field path.
		ValueKind: compiler.ModifierKindAny,
		Apply: func(b *action.Builder[any, any], raw string) error {
			estimate, err := parseEstimateSpec(raw)
			if err != nil {
				return err
			}
			apply(b, estimate)
			return nil
		},
	}
}

// ── helpers ──────────────────────────────────────────────────────────

func parseCurrency(code string) cost.Currency {
	var currency cost.Currency
	if err := currency.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(code)))); err != nil {
		panic("cost: " + err.Error())
	}
	return currency
}
