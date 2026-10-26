package nexssflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/cost"
	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/flow/native"
	"github.com/nexssp/flow/runner"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/storage/sqlite"
)

func newStorageQuotaTestStore(t *testing.T) *storageQuotaStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage.db")
	applyCostStorageMigration(t, path)
	store, err := newStorageQuotaStore(Config{
		StoragePath:      path,
		StorageNamespace: "cost-runtime-test-v1",
	})
	if err != nil {
		t.Fatalf("open runtime quota store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close runtime quota store: %v", err)
		}
	})
	return store
}

func applyCostStorageMigration(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite migration connection: %v", err)
	}
	defer db.Close()

	entries, err := fs.ReadDir(sqlite.Migrations(), ".")
	if err != nil {
		t.Fatalf("read embedded Storage migrations: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "00001_create_storage_tables.sql" {
		t.Fatalf("unexpected Storage migration files: %v", entries)
	}
	migration, err := fs.ReadFile(sqlite.Migrations(), entries[0].Name())
	if err != nil {
		t.Fatalf("read Storage migration: %v", err)
	}
	var up strings.Builder
	inUp := false
	for line := range strings.SplitSeq(string(migration), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-- +goose Up") {
			inUp = true
			continue
		}
		if strings.HasPrefix(trimmed, "-- +goose ") {
			inUp = false
			continue
		}
		if inUp && !strings.HasPrefix(trimmed, "--") {
			up.WriteString(line)
			up.WriteByte('\n')
		}
	}
	for statement := range strings.SplitSeq(up.String(), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("apply embedded Storage migration: %v\nSQL: %s", err, statement)
		}
	}
}

func storageBudgetRunnerConfig(t *testing.T, store *storageQuotaStore, extra ...compiler.Bundle) runner.Config {
	t.Helper()
	bundles := append(native.Bundles(), storageBundle(store))
	bundles = append(bundles, extra...)
	cfg, err := runner.BuildConfig(bundles)
	if err != nil {
		t.Fatalf("build Storage budget Flow config: %v", err)
	}
	return cfg
}

func protectedSupportAction(calls *atomic.Int32) compiler.Bundle {
	protected := action.New("support.draft_reply", func(_ context.Context, input map[string]any) (map[string]any, error) {
		calls.Add(1)
		return map[string]any{
			"ticket": input["ticket"],
			"draft":  "Your refund request is being reviewed.",
		}, nil
	}).Description("Draft a reply for an authenticated support ticket").Build()
	return compiler.Bundle{
		ID:        "test-support-reply",
		Libraries: []action.Library{{Name: "support", Actions: []action.AnyAction{protected}}},
	}
}

const storageBudgetFlow = `support.draft_reply:cost_estimate=60 @{ ticket: .ticket }`

func TestStorageBudgetFlowReadsPolicyAgainOnNextRequestAndIgnoresClientTenant(t *testing.T) {
	store := newStorageQuotaTestStore(t)
	ctx := xctx.WithTenantID(context.Background(), "tenant-maks")
	if err := SetRuntimeBudget(ctx, store.kv, "tenant-maks", 100); err != nil {
		t.Fatalf("seed tenant budget: %v", err)
	}
	if err := SetRuntimeBudget(ctx, store.kv, "tenant-julia", 0); err != nil {
		t.Fatalf("seed unrelated tenant budget: %v", err)
	}

	var calls atomic.Int32
	cfg := storageBudgetRunnerConfig(t, store, protectedSupportAction(&calls))
	payload := map[string]any{
		"ticket":    "refund-1042",
		"tenant_id": "tenant-julia", // Untrusted request data must not select the quota scope.
	}
	first, err := runner.Execute(ctx, cfg, storageBudgetFlow, "support_reply.nflow", payload)
	if err != nil {
		t.Fatalf("first request with 100-micro tenant budget: %v", err)
	}
	firstOutput, ok := first.Output.(map[string]any)
	if !ok || firstOutput["draft"] != "Your refund request is being reviewed." || firstOutput["quota_reserved"] != true {
		t.Fatalf("first request output = %#v, want successful support reply with reservation metadata", first.Output)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("protected action calls after first request = %d, want 1", got)
	}

	if updateErr := SetRuntimeBudget(ctx, store.kv, "tenant-maks", 50); updateErr != nil {
		t.Fatalf("reduce tenant budget before next request: %v", updateErr)
	}
	second, err := runner.Execute(ctx, cfg, storageBudgetFlow, "support_reply.nflow", payload)
	if err != nil {
		t.Fatalf("second request should return a quota denial state: %v", err)
	}
	secondOutput, ok := second.Output.(map[string]any)
	if !ok || secondOutput["quota_reserved"] != false || secondOutput["quota_state"] != "runtime_budget_exceeded" {
		t.Fatalf("second request output = %#v, want runtime budget denial", second.Output)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("protected action calls after budget reduction = %d, want 1; denied request must not run it", got)
	}
}

func TestStorageBudgetMissingAndInvalidPoliciesFailClosedBeforeAction(t *testing.T) {
	store := newStorageQuotaTestStore(t)
	var calls atomic.Int32
	cfg := storageBudgetRunnerConfig(t, store, protectedSupportAction(&calls))

	for _, test := range []struct {
		name    string
		tenant  string
		prepare func(context.Context) error
	}{
		{name: "missing", tenant: "tenant-missing"},
		{
			name:   "invalid",
			tenant: "tenant-invalid",
			prepare: func(ctx context.Context) error {
				key, err := runtimeQuotaKey("tenant-invalid")
				if err != nil {
					return err
				}
				return store.kv.Update(ctx, key, func(any, bool) (any, time.Duration, error) {
					return map[string]any{"version": 1, "budget_micros": "not-a-number"}, 0, nil
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := xctx.WithTenantID(context.Background(), test.tenant)
			if test.prepare != nil {
				if err := test.prepare(ctx); err != nil {
					t.Fatalf("prepare invalid policy: %v", err)
				}
			}
			_, err := runner.Execute(ctx, cfg, storageBudgetFlow, "support_reply.nflow", map[string]any{"ticket": "refund-1042"})
			if err == nil {
				t.Fatal("Flow succeeded despite missing or invalid runtime policy")
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("protected action ran %d times with missing/invalid policy, want 0", got)
			}
		})
	}
}

func TestStorageBudgetRequiresTrustedTenantContext(t *testing.T) {
	store := newStorageQuotaTestStore(t)
	if err := SetRuntimeBudget(context.Background(), store.kv, "tenant-from-input", 1_000); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	cfg := storageBudgetRunnerConfig(t, store, protectedSupportAction(&calls))
	_, err := runner.Execute(context.Background(), cfg, storageBudgetFlow, "support_reply.nflow", map[string]any{
		"ticket":    "refund-1042",
		"tenant_id": "tenant-from-input",
	})
	if err == nil || !strings.Contains(err.Error(), "trusted tenant identity") {
		t.Fatalf("run without trusted tenant context error = %v, want trusted identity failure", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("protected action calls without trusted tenant context = %d, want 0", got)
	}
}

func TestStorageBudgetReductionPreservesInFlightReservationButBlocksNewWork(t *testing.T) {
	store := newStorageQuotaTestStore(t)
	ctx := xctx.WithTenantID(context.Background(), "tenant-in-flight")
	if err := SetRuntimeBudget(ctx, store.kv, "tenant-in-flight", 100); err != nil {
		t.Fatal(err)
	}
	ledger := &runtimeQuotaLedger{kv: store.kv, tenantID: "tenant-in-flight"}
	inFlight, err := ledger.Reserve(ctx, 80)
	if err != nil {
		t.Fatalf("reserve before policy reduction: %v", err)
	}
	if err := SetRuntimeBudget(ctx, store.kv, "tenant-in-flight", 50); err != nil {
		t.Fatalf("reduce policy during in-flight action: %v", err)
	}
	if _, err := ledger.Reserve(ctx, 1); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("new reservation after reduction error = %v, want ErrBudgetExceeded", err)
	}
	if err := inFlight.Commit(ctx, 80); err != nil {
		t.Fatalf("pre-existing reservation must be allowed to settle after reduction: %v", err)
	}
	if _, err := ledger.Reserve(ctx, 1); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("reservation after in-flight settlement error = %v, want ErrBudgetExceeded", err)
	}
}

func TestStorageBudgetConcurrentReservationsAreAtomic(t *testing.T) {
	store := newStorageQuotaTestStore(t)
	ctx := xctx.WithTenantID(context.Background(), "tenant-concurrent")
	const limit = 24
	if err := SetRuntimeBudget(ctx, store.kv, "tenant-concurrent", limit); err != nil {
		t.Fatal(err)
	}
	ledger := &runtimeQuotaLedger{kv: store.kv, tenantID: "tenant-concurrent"}
	const attempts = 120
	reservations := make(chan cost.Reservation, attempts)
	errorsSeen := make(chan error, attempts)
	var group sync.WaitGroup
	for range attempts {
		group.Go(func() {
			reservation, err := ledger.Reserve(ctx, 1)
			if err == nil {
				reservations <- reservation
				return
			}
			if !errors.Is(err, cost.ErrBudgetExceeded) {
				errorsSeen <- err
			}
		})
	}
	group.Wait()
	close(reservations)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("unexpected concurrent reservation error: %v", err)
	}

	accepted := make([]cost.Reservation, 0, limit)
	for reservation := range reservations {
		accepted = append(accepted, reservation)
	}
	if len(accepted) != limit {
		t.Fatalf("concurrent reservations accepted = %d, want exactly %d", len(accepted), limit)
	}
	for _, reservation := range accepted {
		if err := reservation.Commit(ctx, 1); err != nil {
			t.Errorf("commit concurrent reservation: %v", err)
		}
	}
	if _, err := ledger.Reserve(ctx, 1); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("reservation after concurrent usage error = %v, want ErrBudgetExceeded", err)
	}
}

func TestStorageBackendRejectsBudgetInRequireOptions(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(fmt.Sprint(recovered), "budget is stored per tenant in Storage") {
			t.Fatalf("Bundle panic = %v, want static-budget guidance", recovered)
		}
	}()
	Bundle(map[string]string{
		"backend":      "storage",
		"storage_path": filepath.Join(t.TempDir(), "storage.db"),
		"budget":       "1000000",
	})
}
