package nexssflow

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nexssp/cost"
	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/storage"
	"github.com/nexssp/storage/sqlite"
)

const runtimeQuotaKeyPrefix = "cost/runtime-budget/v1/"

var (
	errRuntimePolicyMissing = errors.New("cost: runtime tenant budget policy is missing")
	errRuntimePolicyInvalid = errors.New("cost: runtime tenant budget policy is invalid")
)

type storageQuotaStore struct {
	registry *storage.Registry
	kv       storage.AtomicKV
}

func newStorageQuotaStore(cfg Config) (*storageQuotaStore, error) {
	path := strings.TrimSpace(os.ExpandEnv(cfg.StoragePath))
	if path == "" {
		return nil, errors.New("backend=storage requires storage_path")
	}
	namespace := strings.TrimSpace(cfg.StorageNamespace)
	if namespace == "" || len(namespace) > 128 || strings.ContainsAny(namespace, "\x00\r\n") {
		return nil, errors.New("storage_namespace must be a non-empty string of at most 128 characters")
	}

	registry := storage.NewRegistry()
	if err := sqlite.Register(registry); err != nil {
		return nil, fmt.Errorf("register SQLite storage backend: %w", err)
	}
	inst, err := (storage.Declaration{
		Name:    "cost-runtime-budget",
		Backend: "sqlite",
		Kind:    storage.KindKV,
		Options: map[string]string{"path": path, "namespace": namespace},
	}).Open(registry)
	if err != nil {
		_ = registry.Close(context.Background())
		return nil, fmt.Errorf("open runtime budget storage: %w", err)
	}
	kv, err := storage.AssertAtomicKV(inst)
	if err != nil {
		_ = registry.Close(context.Background())
		return nil, fmt.Errorf("runtime budget storage needs atomic KV support: %w", err)
	}
	return &storageQuotaStore{registry: registry, kv: kv}, nil
}

func (s *storageQuotaStore) close(ctx context.Context) error {
	if s == nil || s.registry == nil {
		return nil
	}
	return s.registry.Close(ctx)
}

func storageBundle(store *storageQuotaStore) compiler.Bundle {
	modifier := costEstimateModifierWithApply(func(builder *action.Builder[any, any], estimate estimateSpec) {
		name := ""
		if meta := builder.Describe(); meta != nil {
			name = meta.Name
		}
		builder.UseFirst(store.quotaMiddleware(estimate, name))
	})
	return compiler.Bundle{
		ID:              ID,
		Modifiers:       []compiler.Modifier{modifier},
		AcceptedOptions: acceptedOptions,
		Shutdowns: []compiler.ShutdownFunc{func(ctx context.Context) error {
			return store.close(ctx)
		}},
	}
}

func (s *storageQuotaStore) quotaMiddleware(estimateSpec estimateSpec, actionName string) action.Middleware[any, any] {
	return func(next action.Fn[any, any]) action.Fn[any, any] {
		return func(ctx context.Context, input any) (output any, resultErr error) {
			estimate, err := estimateSpec.resolve(input)
			if err != nil {
				return nil, err
			}
			tenantID := strings.TrimSpace(xctx.TenantIDFrom(ctx))
			if tenantID == "" {
				return nil, errors.New("cost: trusted tenant identity is required in context (set by the authenticated host, not the Flow input)")
			}
			ledger := &runtimeQuotaLedger{kv: s.kv, tenantID: tenantID}
			reservation, err := ledger.Reserve(ctx, estimate)
			if err != nil {
				if errors.Is(err, cost.ErrBudgetExceeded) {
					return setQuotaState(input, false, "runtime_budget_exceeded")
				}
				return nil, fmt.Errorf("cost: load or reserve runtime tenant budget: %w", err)
			}

			settled := false
			defer func() {
				recovered := recover()
				if !settled {
					if cleanupErr := finishReservation(ctx, reservation, false, 0); cleanupErr != nil {
						wrappedErr := fmt.Errorf("cost: release runtime tenant reservation: %w", cleanupErr)
						if recovered == nil {
							resultErr = errors.Join(resultErr, wrappedErr)
						} else {
							logRuntimeSettlementFailure("release runtime tenant reservation after panic", wrappedErr)
						}
					}
				}
				if recovered != nil {
					panic(recovered)
				}
			}()

			output, resultErr = next(ctx, input)
			if resultErr != nil {
				return output, resultErr
			}
			actual := estimate
			if reporter, ok := output.(cost.Reporter); ok {
				actual = max(reporter.CostMicros(), 0)
			}
			if err := finishReservation(ctx, reservation, true, actual); err != nil {
				return nil, fmt.Errorf("cost: settle runtime tenant reservation: %w", err)
			}
			settled = true
			if actual > 0 {
				domain, operation := actionDomainOperation(actionName)
				recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
				defer cancel()
				if err := ledger.Record(recordCtx, cost.Event{
					Domain: domain, Operation: operation, CostMicros: actual,
				}); err != nil {
					return nil, fmt.Errorf("cost: record runtime tenant usage: %w", err)
				}
			}
			return setQuotaState(output, true, "runtime_reserved")
		}
	}
}

// SetRuntimeBudget atomically creates or updates a tenant's budget policy in
// the same Storage KV record used for its committed usage and in-flight
// reservations. Call it from a trusted setup/admin path; never expose it as a
// Flow action or pass a client-provided tenant ID to it.
func SetRuntimeBudget(ctx context.Context, kv storage.AtomicKV, tenantID string, limitMicros int64) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if kv == nil {
		return errors.New("cost: runtime budget requires an atomic Storage KV")
	}
	if limitMicros < 0 {
		return fmt.Errorf("cost: runtime budget must be non-negative, got %d", limitMicros)
	}
	key, err := runtimeQuotaKey(tenantID)
	if err != nil {
		return err
	}
	err = kv.Update(ctx, key, func(current any, hit bool) (any, time.Duration, error) {
		record := newRuntimeQuotaRecord()
		if hit {
			var decodeErr error
			record, decodeErr = decodeRuntimeQuotaRecord(current)
			if decodeErr != nil {
				return nil, 0, decodeErr
			}
		}
		record.BudgetMicros = strconv.FormatInt(limitMicros, 10)
		return record, 0, nil
	})
	if err != nil {
		return fmt.Errorf("cost: update runtime budget for trusted tenant: %w", err)
	}
	return nil
}

type runtimeQuotaRecord struct {
	Version      int               `json:"version"`
	BudgetMicros string            `json:"budget_micros"`
	SpentMicros  string            `json:"spent_micros"`
	Reservations map[string]string `json:"reservations,omitempty"`
}

func newRuntimeQuotaRecord() runtimeQuotaRecord {
	return runtimeQuotaRecord{
		Version:      1,
		BudgetMicros: "",
		SpentMicros:  "0",
		Reservations: make(map[string]string),
	}
}

func decodeRuntimeQuotaRecord(value any) (runtimeQuotaRecord, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return runtimeQuotaRecord{}, fmt.Errorf("%w: encode stored record: %w", errRuntimePolicyInvalid, err)
	}
	var record runtimeQuotaRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return runtimeQuotaRecord{}, fmt.Errorf("%w: decode stored record: %w", errRuntimePolicyInvalid, err)
	}
	if record.Version != 0 && record.Version != 1 {
		return runtimeQuotaRecord{}, fmt.Errorf("%w: unsupported record version %d", errRuntimePolicyInvalid, record.Version)
	}
	if record.Version == 0 {
		record.Version = 1
	}
	if record.SpentMicros == "" {
		record.SpentMicros = "0"
	}
	if _, err := parseNonNegativeMicros(record.SpentMicros, "spent_micros"); err != nil {
		return runtimeQuotaRecord{}, err
	}
	if record.Reservations == nil {
		record.Reservations = make(map[string]string)
	}
	for id, amount := range record.Reservations {
		if id == "" {
			return runtimeQuotaRecord{}, fmt.Errorf("%w: empty reservation ID", errRuntimePolicyInvalid)
		}
		if _, err := parseNonNegativeMicros(amount, "reservation amount"); err != nil {
			return runtimeQuotaRecord{}, err
		}
	}
	return record, nil
}

func parseNonNegativeMicros(raw, field string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%w: %s must be a non-negative decimal integer in micros", errRuntimePolicyInvalid, field)
	}
	return value, nil
}

func runtimeQuotaKey(tenantID string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return "", errors.New("cost: trusted tenant identity is required")
	}
	digest := sha256.Sum256([]byte(tenantID))
	return runtimeQuotaKeyPrefix + hex.EncodeToString(digest[:]), nil
}

type runtimeQuotaLedger struct {
	kv       storage.AtomicKV
	tenantID string
}

//nolint:gocyclo // Validation and reservation must use one atomic policy snapshot.
func (l *runtimeQuotaLedger) Reserve(ctx context.Context, estimate int64) (cost.Reservation, error) {
	if ctx == nil {
		return nil, cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if estimate < 0 || estimate > math.MaxInt64/2 {
		return nil, &cost.ValidationError{Field: "estimate_micros", Value: estimate}
	}
	if l == nil || l.kv == nil {
		return nil, errors.New("cost: runtime budget requires an atomic Storage KV")
	}
	key, err := runtimeQuotaKey(l.tenantID)
	if err != nil {
		return nil, err
	}
	id, err := newRuntimeReservationID()
	if err != nil {
		return nil, fmt.Errorf("cost: generate runtime reservation ID: %w", err)
	}
	err = l.kv.Update(ctx, key, func(current any, hit bool) (any, time.Duration, error) {
		if !hit {
			return nil, 0, errRuntimePolicyMissing
		}
		record, decodeErr := decodeRuntimeQuotaRecord(current)
		if decodeErr != nil {
			return nil, 0, decodeErr
		}
		limit, limitErr := parseNonNegativeMicros(record.BudgetMicros, "budget_micros")
		if limitErr != nil {
			return nil, 0, limitErr
		}
		spent, spentErr := parseNonNegativeMicros(record.SpentMicros, "spent_micros")
		if spentErr != nil {
			return nil, 0, spentErr
		}
		reserved := int64(0)
		for _, raw := range record.Reservations {
			amount, parseErr := parseNonNegativeMicros(raw, "reservation amount")
			if parseErr != nil {
				return nil, 0, parseErr
			}
			if amount > math.MaxInt64-reserved {
				return nil, 0, fmt.Errorf("%w: reserved amount overflows int64", errRuntimePolicyInvalid)
			}
			reserved += amount
		}
		if spent > math.MaxInt64-reserved || estimate > math.MaxInt64-(spent+reserved) {
			return nil, 0, fmt.Errorf("%w: usage amount overflows int64", errRuntimePolicyInvalid)
		}
		used := spent + reserved
		if estimate > limit-used {
			return nil, 0, fmt.Errorf("%w: spent=%d reserved=%d estimate=%d limit=%d", cost.ErrBudgetExceeded, spent, reserved, estimate, limit)
		}
		record.Reservations[id] = strconv.FormatInt(estimate, 10)
		return record, 0, nil
	})
	if err != nil {
		return nil, err
	}
	return &runtimeQuotaReservation{ledger: l, key: key, id: id}, nil
}

func (l *runtimeQuotaLedger) Record(ctx context.Context, _ cost.Event) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The reservation commit is the durable usage record for this backend.
	// Do not write a second, unbounded copy of every event into the KV value.
	return nil
}

type runtimeQuotaReservation struct {
	ledger *runtimeQuotaLedger
	key    string
	id     string
	mu     sync.Mutex
	done   bool
}

func (r *runtimeQuotaReservation) Commit(ctx context.Context, actual int64) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}
	if actual < 0 {
		actual = 0
	}
	err := r.ledger.kv.Update(ctx, r.key, func(current any, hit bool) (any, time.Duration, error) {
		record, err := recordForSettlement(current, hit)
		if err != nil {
			return nil, 0, err
		}
		reserved, ok := record.Reservations[r.id]
		if !ok {
			return nil, 0, fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
		}
		delete(record.Reservations, r.id)
		spent, err := parseNonNegativeMicros(record.SpentMicros, "spent_micros")
		if err != nil {
			return nil, 0, err
		}
		if actual > math.MaxInt64-spent {
			return nil, 0, fmt.Errorf("%w: committed usage overflows int64", errRuntimePolicyInvalid)
		}
		if _, err := parseNonNegativeMicros(reserved, "reservation amount"); err != nil {
			return nil, 0, err
		}
		record.SpentMicros = strconv.FormatInt(spent+actual, 10)
		return record, 0, nil
	})
	if err != nil {
		return err
	}
	r.done = true
	return nil
}

func (r *runtimeQuotaReservation) Release(ctx context.Context) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}
	err := r.ledger.kv.Update(ctx, r.key, func(current any, hit bool) (any, time.Duration, error) {
		record, err := recordForSettlement(current, hit)
		if err != nil {
			return nil, 0, err
		}
		if _, ok := record.Reservations[r.id]; !ok {
			return nil, 0, fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
		}
		delete(record.Reservations, r.id)
		return record, 0, nil
	})
	if err != nil {
		return err
	}
	r.done = true
	return nil
}

func recordForSettlement(current any, hit bool) (runtimeQuotaRecord, error) {
	if !hit {
		return runtimeQuotaRecord{}, fmt.Errorf("%w: record disappeared while a reservation was in flight", cost.ErrReservationNotFound)
	}
	return decodeRuntimeQuotaRecord(current)
}

func newRuntimeReservationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func logRuntimeSettlementFailure(message string, err error) {
	// Keep the durable quota path's cleanup failures visible without exposing
	// tenant identifiers or policy contents in logs.
	slog.Error(message, "error", err)
}
