package nexssflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/cost"
)

func TestMonthlyScopeSeparatesTenantAndUTCMonth(t *testing.T) {
	january := time.Date(2026, time.January, 31, 23, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	february := january.Add(4 * time.Hour)

	first, err := monthlyScope("support", "customer-1", january)
	if err != nil {
		t.Fatal(err)
	}
	otherTenant, err := monthlyScope("support", "customer-2", january)
	if err != nil {
		t.Fatal(err)
	}
	newMonth, err := monthlyScope("support", "customer-1", february)
	if err != nil {
		t.Fatal(err)
	}
	if first == otherTenant {
		t.Fatal("different customers received the same quota scope")
	}
	if first == newMonth {
		t.Fatal("the UTC month boundary did not create a new quota scope")
	}
	if got, want := first[len(first)-7:], "2026-01"; got != want {
		t.Fatalf("scope month suffix = %q, want %q", got, want)
	}
}

func TestMonthlyScopeRejectsEmptyTenant(t *testing.T) {
	if _, err := monthlyScope("support", " \t ", time.Now()); err == nil {
		t.Fatal("expected empty tenant to be rejected")
	}
}

func TestTenantValueRequiresTopLevelNonEmptyString(t *testing.T) {
	got, err := tenantValue(map[string]any{"customer_id": " cus_42 "}, "customer_id")
	if err != nil {
		t.Fatal(err)
	}
	if got != "cus_42" {
		t.Fatalf("tenant = %q, want trimmed customer ID", got)
	}
	for _, input := range []any{
		map[string]any{"customer_id": 42},
		map[string]any{},
		"not-an-object",
	} {
		if _, err := tenantValue(input, "customer_id"); err == nil {
			t.Errorf("tenantValue(%T) unexpectedly succeeded", input)
		}
	}
}

func TestSetQuotaStatePreservesTicketFields(t *testing.T) {
	got, err := setQuotaState(map[string]any{"ticket_id": "ticket-1", "route": "drafted"}, false, "monthly_quota_exceeded")
	if err != nil {
		t.Fatal(err)
	}
	result, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("quota result = %T, want map[string]any", got)
	}
	if result["ticket_id"] != "ticket-1" || result["route"] != "drafted" {
		t.Fatalf("ticket fields were not preserved: %#v", result)
	}
	if result["quota_reserved"] != false || result["quota_state"] != "monthly_quota_exceeded" {
		t.Fatalf("quota state = %#v", result)
	}
}

func TestQuotaMiddlewareRoutesBudgetDenialWithoutRunningTarget(t *testing.T) {
	backend := &fakeReserver{reserveErr: cost.ErrBudgetExceeded}
	store := testPostgresStore(backend)
	targetCalled := false
	next := func(context.Context, any) (any, error) {
		targetCalled = true
		return map[string]any{"route": "ai_draft"}, nil
	}

	got, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(next)(
		context.Background(), map[string]any{"customer_id": "cus_42", "ticket_id": "ticket_42"},
	)
	if err != nil {
		t.Fatalf("quota denial returned error: %v", err)
	}
	result, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("quota result = %T, want map[string]any", got)
	}
	if targetCalled {
		t.Fatal("draft target ran after quota reservation was denied")
	}
	if result["quota_reserved"] != false || result["quota_state"] != "monthly_quota_exceeded" {
		t.Fatalf("quota denial result = %#v", result)
	}
	if result["ticket_id"] != "ticket_42" {
		t.Fatalf("quota denial lost ticket input: %#v", result)
	}
}

func TestQuotaMiddlewareDoesNotMaskInfrastructureErrors(t *testing.T) {
	backend := &fakeReserver{reserveErr: errors.New("database unavailable")}
	store := testPostgresStore(backend)
	targetCalled := false
	_, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(context.Context, any) (any, error) {
			targetCalled = true
			return map[string]any{}, nil
		},
	)(context.Background(), map[string]any{"customer_id": "cus_42"})
	if err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("database failure = %v, want propagated error", err)
	}
	if targetCalled {
		t.Fatal("draft target ran after reservation infrastructure failure")
	}
}

func TestQuotaMiddlewareCommitsAndRecordsSuccessfulDraft(t *testing.T) {
	backend := &fakeReserver{reservation: &fakeReservation{}}
	store := testPostgresStore(backend)
	got, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(_ context.Context, input any) (any, error) {
			inputMap, ok := input.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("pipeline input = %T, want map[string]any", input)
			}
			return map[string]any{"ticket_id": inputMap["ticket_id"], "route": "ai_draft"}, nil
		},
	)(context.Background(), map[string]any{"customer_id": "cus_42", "ticket_id": "ticket_42"})
	if err != nil {
		t.Fatalf("successful draft returned error: %v", err)
	}
	result, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("quota result = %T, want map[string]any", got)
	}
	if result["quota_reserved"] != true || result["quota_state"] != "reserved" {
		t.Fatalf("successful result quota state = %#v", result)
	}
	if !backend.reservation.commitCalled || backend.reservation.committed != 200_000 {
		t.Fatalf("reservation commit = %#v, want 200000 micros", backend.reservation)
	}
	if len(backend.events) != 1 || backend.events[0].Domain != "pipeline" || backend.events[0].Operation != "draft_reply" || backend.events[0].CostMicros != 200_000 {
		t.Fatalf("recorded usage events = %#v", backend.events)
	}
}

func TestQuotaMiddlewareReleasesWhenTargetFails(t *testing.T) {
	backend := &fakeReserver{reservation: &fakeReservation{}}
	store := testPostgresStore(backend)
	wantErr := errors.New("draft failed")
	_, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(context.Context, any) (any, error) { return nil, wantErr },
	)(context.Background(), map[string]any{"customer_id": "cus_42"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("target error = %v, want %v", err, wantErr)
	}
	if !backend.reservation.releaseCalled || backend.reservation.commitCalled {
		t.Fatalf("failed target reservation state = %#v", backend.reservation)
	}
	if len(backend.events) != 0 {
		t.Fatalf("failed target recorded usage: %#v", backend.events)
	}
}

func TestNewPostgresStoreRejectsInvalidSettingsBeforeConnecting(t *testing.T) {
	base := Config{
		Backend: "postgres", Budget: 300_000, Currency: "USD", DSN: "postgres://localhost/cost",
		SchemaMode: "external", ScopePrefix: "support", TenantField: "customer_id", ReservationTTL: "5m",
		MaxOpenConns: 10, MaxIdleConns: 5,
	}
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "negative budget", edit: func(cfg *Config) { cfg.Budget = -2 }, want: "non-negative"},
		{name: "unlimited budget", edit: func(cfg *Config) { cfg.Budget = -1 }, want: "non-negative"},
		{name: "unsupported schema mode", edit: func(cfg *Config) { cfg.SchemaMode = "sometimes" }, want: "schema_mode"},
		{name: "unsupported accounting mode", edit: func(cfg *Config) { cfg.AccountingMode = "sometimes" }, want: "accounting_mode"},
		{name: "bad tenant field", edit: func(cfg *Config) { cfg.TenantField = "customer.id" }, want: "tenant_field"},
		{name: "short lease", edit: func(cfg *Config) { cfg.ReservationTTL = "500ms" }, want: "reservation_ttl"},
		{name: "zero pool", edit: func(cfg *Config) { cfg.MaxOpenConns = 0 }, want: "max_open_conns"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			test.edit(&cfg)
			if _, err := newPostgresStore(cfg, cost.USD); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newPostgresStore error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestBundleRejectsPostgresOnlyOptionInMemoryMode(t *testing.T) {
	defer func() {
		recovered := recover()
		message, ok := recovered.(string)
		if !ok || !strings.Contains(message, "requires backend=postgres") {
			t.Fatalf("expected PostgreSQL-only option error, got %v", recovered)
		}
	}()
	Bundle(map[string]string{"backend": "memory", "dsn": "postgres://localhost/cost"})
}

func TestBundleRejectsGlobalBudgetInWalletMode(t *testing.T) {
	defer func() {
		recovered := recover()
		message, ok := recovered.(string)
		if !ok || !strings.Contains(message, "budget is not accepted with accounting_mode=wallet") {
			t.Fatalf("expected wallet budget configuration error, got %v", recovered)
		}
	}()
	Bundle(map[string]string{
		"backend": "postgres", "accounting_mode": "wallet", "budget": "300000",
	})
}

func testPostgresStore(backend *fakeReserver) *postgresStore {
	return &postgresStore{
		budget:         300_000,
		currency:       cost.USD,
		scopePrefix:    "test-quota",
		tenantField:    "customer_id",
		reservationTTL: 5 * time.Minute,
		ledgerFactory: func(context.Context, string) (cost.Reserver, error) {
			return backend, nil
		},
	}
}

type fakeReserver struct {
	reserveErr  error
	reservation *fakeReservation
	events      []cost.Event
}

func (f *fakeReserver) Reserve(context.Context, int64) (cost.Reservation, error) {
	if f.reserveErr != nil {
		return nil, f.reserveErr
	}
	return f.reservation, nil
}

func (f *fakeReserver) Record(_ context.Context, event cost.Event) error {
	f.events = append(f.events, event)
	return nil
}

type fakeReservation struct {
	committed     int64
	commitErr     error
	commitCalled  bool
	releaseCalled bool
}

func (f *fakeReservation) Commit(_ context.Context, actual int64) error {
	f.commitCalled = true
	f.committed = actual
	return f.commitErr
}

func (f *fakeReservation) Release(context.Context) error {
	f.releaseCalled = true
	return nil
}
