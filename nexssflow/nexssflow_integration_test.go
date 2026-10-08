package nexssflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/cost"
	"github.com/nexssp/cost/nexssflow"
	"github.com/nexssp/flow/core"
	"github.com/nexssp/flow/extensions/modifiers_core"
	"github.com/nexssp/flow/extensions/modifiers_meta"
	"github.com/nexssp/flow/extensions/pipeline"
	"github.com/nexssp/flow/extensions/projection"
	"github.com/nexssp/flow/extensions/runtime"
	"github.com/nexssp/flow/extensions/scope"
	"github.com/nexssp/flow/extensions/syntax"
	"github.com/nexssp/flow/runner"
	"github.com/nexssp/kernel/action"
)

func costRunnerConfig(t *testing.T, ledger *cost.Ledger, extra ...core.Bundle) runner.Config {
	t.Helper()
	bundles := []core.Bundle{
		syntax.Bundle(nil),
		runtime.Bundle(nil),
		projection.Bundle(nil),
		modifiers_core.Bundle(nil),
		modifiers_meta.Bundle(nil),
		pipeline.Bundle(nil),
		scope.Bundle(nil),
		nexssflow.NewBundle(ledger),
	}
	bundles = append(bundles, extra...)
	cfg, err := runner.BuildConfig(bundles)
	if err != nil {
		t.Fatalf("build Flow runner config: %v", err)
	}
	return cfg
}

func TestFlowAdapter_ReserveCommitOnSuccess(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	cfg := costRunnerConfig(t, ledger)

	execution, err := runner.Execute(
		context.Background(), cfg,
		`runtime.const @{ value: "done" }:cost_estimate=1000 -> cost.snapshot`,
		"cost-success.nflow", nil,
	)
	if err != nil {
		t.Fatalf("execute Flow source: %v", err)
	}

	snapshot, ok := execution.Output.(nexssflow.Snapshot)
	if !ok {
		t.Fatalf("output type = %T, want nexssflow.Snapshot", execution.Output)
	}
	if snapshot.UsedMicros != 1_000 || snapshot.SpentMicros != 1_000 || snapshot.RemainingMicros != 9_000 {
		t.Fatalf("snapshot remaining=%d used=%d spent=%d, want 9000, 1000, 1000", snapshot.RemainingMicros, snapshot.UsedMicros, snapshot.SpentMicros)
	}
	if got := ledger.Entries(); len(got) != 1 || got[0].Operation != "const" || got[0].CostMicros != 1_000 {
		t.Fatalf("ledger entries = %#v, want one runtime.const event for 1000 micros", got)
	}
}

func TestFlowAdapter_ReleaseOnActionError(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	cfg := costRunnerConfig(t, ledger)

	_, err := runner.Execute(
		context.Background(), cfg,
		`runtime.fail:cost_estimate=1000 @{ message: "expected failure" }`,
		"cost-error.nflow", nil,
	)
	if err == nil {
		t.Fatal("expected action error")
	}
	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 || len(ledger.Entries()) != 0 {
		t.Fatalf("failed action did not release cleanly: used=%d spent=%d entries=%d", ledger.UsedMicros(), ledger.SpentMicros(), len(ledger.Entries()))
	}
}

func TestFlowAdapter_ReleaseOnCancellation(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	started := make(chan struct{})
	waitForCancel := action.New("test.wait_for_cancel", func(ctx context.Context, _ any) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	extra := core.Bundle{
		ID:        "test-cancellation",
		Libraries: []action.Library{{Name: "test", Actions: []action.AnyAction{waitForCancel}}},
	}
	cfg := costRunnerConfig(t, ledger, extra)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type outcome struct {
		execution runner.Execution
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		execution, err := runner.Execute(ctx, cfg, `test.wait_for_cancel:cost_estimate=1000`, "cost-cancel.nflow", nil)
		done <- outcome{execution: execution, err: err}
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellable action did not start")
	}
	cancel()

	select {
	case result := <-done:
		if result.err == nil || !errors.Is(result.err, context.Canceled) {
			t.Fatalf("runner error = %v, want context.Canceled", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flow runner did not return after cancellation")
	}
	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 || len(ledger.Entries()) != 0 {
		t.Fatalf("canceled action did not release cleanly: used=%d spent=%d entries=%d", ledger.UsedMicros(), ledger.SpentMicros(), len(ledger.Entries()))
	}
}

func TestFlowAdapter_BudgetExceededIsCatchableWithFallback(t *testing.T) {
	ledger := cost.NewLedger(999, cost.USD)
	cfg := costRunnerConfig(t, ledger)

	execution, err := runner.Execute(
		context.Background(), cfg,
		`runtime.noop:cost_estimate=1000 || runtime.const @{ value: "recovered" }`,
		"cost-fallback.nflow", nil,
	)
	if err != nil {
		t.Fatalf("execute fallback: %v", err)
	}
	if execution.Output != "recovered" {
		t.Fatalf("fallback output = %#v, want %q", execution.Output, "recovered")
	}
	if ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
		t.Fatalf("budget failure changed counters: used=%d spent=%d", ledger.UsedMicros(), ledger.SpentMicros())
	}
}

func TestFlowAdapter_ReportPreservesSequentialPipelineBucketOrder(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	cfg := costRunnerConfig(t, ledger)
	source := `
@pipeline first
  runtime.noop
@end
@pipeline second
  runtime.noop
@end
@pipeline third
  runtime.noop
@end
runtime.const @{ value: "seed" }
-> pipeline.first:cost_estimate=1000
-> pipeline.second:cost_estimate=2000
-> pipeline.third:cost_estimate=3000
-> cost.report
`

	execution, err := runner.Execute(context.Background(), cfg, source, "cost-report-order.nflow", nil)
	if err != nil {
		t.Fatalf("execute report source: %v", err)
	}
	report, ok := execution.Output.(nexssflow.Report)
	if !ok {
		t.Fatalf("output type = %T, want nexssflow.Report", execution.Output)
	}
	if len(report.Buckets) != 3 {
		t.Fatalf("report buckets = %d, want 3: %#v", len(report.Buckets), report.Buckets)
	}
	want := []struct {
		operation string
		spent     int64
	}{{"first", 1_000}, {"second", 2_000}, {"third", 3_000}}
	for i, expected := range want {
		got := report.Buckets[i]
		if got.Operation != expected.operation || got.SpentMicros != expected.spent {
			t.Errorf("bucket[%d] = (%q, %d), want (%q, %d)", i, got.Operation, got.SpentMicros, expected.operation, expected.spent)
		}
	}
}

func TestFlowAdapter_UnlimitedSnapshotUsesNegativeOneRemainingSentinel(t *testing.T) {
	ledger := cost.NewLedger(-1, cost.USD)
	cfg := costRunnerConfig(t, ledger)

	execution, err := runner.Execute(context.Background(), cfg, `cost.snapshot`, "cost-unlimited-snapshot.nflow", nil)
	if err != nil {
		t.Fatalf("execute snapshot: %v", err)
	}
	snapshot, ok := execution.Output.(nexssflow.Snapshot)
	if !ok {
		t.Fatalf("output type = %T, want nexssflow.Snapshot", execution.Output)
	}
	if snapshot.LimitMicros != -1 || snapshot.RemainingMicros != -1 {
		t.Fatalf("unlimited snapshot limit=%d remaining=%d, want -1 for both", snapshot.LimitMicros, snapshot.RemainingMicros)
	}
}

func TestFlowAdapter_RecordAddsAuditEventWithoutChangingCounters(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	cfg := costRunnerConfig(t, ledger)
	source := `cost.record @{ id: "resp_123", domain: "openai", operation: "responses", cost_micros: 1234, currency: "USD" } -> cost.report`

	execution, err := runner.Execute(context.Background(), cfg, source, "cost-record.nflow", nil)
	if err != nil {
		t.Fatalf("execute record source: %v", err)
	}
	report, ok := execution.Output.(nexssflow.Report)
	if !ok {
		t.Fatalf("output type = %T, want nexssflow.Report", execution.Output)
	}
	if len(report.Buckets) != 1 {
		t.Fatalf("report buckets = %d, want one audit bucket: %#v", len(report.Buckets), report.Buckets)
	}
	bucket := report.Buckets[0]
	if bucket.Domain != "openai" || bucket.Operation != "responses" || bucket.Count != 1 || bucket.SpentMicros != 1_234 {
		t.Fatalf("record bucket = %#v, want openai.responses count=1 spent=1234", bucket)
	}
	if report.UsedMicros != 0 || report.SpentMicros != 0 || ledger.UsedMicros() != 0 || ledger.SpentMicros() != 0 {
		t.Fatalf("audit-only record changed counters: report used=%d spent=%d; ledger used=%d spent=%d", report.UsedMicros, report.SpentMicros, ledger.UsedMicros(), ledger.SpentMicros())
	}
	entries := ledger.Entries()
	if len(entries) != 1 || entries[0].ID != "resp_123" {
		t.Fatalf("audit entries = %#v, want recorded event ID resp_123", entries)
	}
}

func TestFlowAdapter_DecoratedProjectionModifierReservesAndCommits(t *testing.T) {
	ledger := cost.NewLedger(10_000, cost.USD)
	cfg := costRunnerConfig(t, ledger)
	source := `{ value: "done" }:cost_estimate=1000 -> cost.snapshot`

	execution, err := runner.Execute(context.Background(), cfg, source, "cost-decorated-projection.nflow", nil)
	if err != nil {
		t.Fatalf("execute decorated projection: %v", err)
	}
	snapshot, ok := execution.Output.(nexssflow.Snapshot)
	if !ok {
		t.Fatalf("output type = %T, want nexssflow.Snapshot", execution.Output)
	}
	if snapshot.RemainingMicros != 9_000 || snapshot.UsedMicros != 1_000 || snapshot.SpentMicros != 1_000 {
		t.Fatalf("decorated projection snapshot remaining=%d used=%d spent=%d, want 9000, 1000, 1000", snapshot.RemainingMicros, snapshot.UsedMicros, snapshot.SpentMicros)
	}
	entries := ledger.Entries()
	if len(entries) != 1 || entries[0].CostMicros != 1_000 {
		t.Fatalf("decorated projection audit entries = %#v, want one 1000-micro guard event", entries)
	}
}
