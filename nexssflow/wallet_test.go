package nexssflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/cost"
)

func TestWalletIdentityFromInputRequiresAllThreeTrustedIDs(t *testing.T) {
	input := map[string]any{
		"tenant_id": " tn_acme ",
		"user_id":   "usr_alex",
		"wallet_id": "wal_acme_alex_usd",
	}
	got, err := walletIdentityFromInput(input, "tenant_id", "user_id", "wallet_id")
	if err != nil {
		t.Fatal(err)
	}
	want := walletIdentity{tenantID: "tn_acme", userID: "usr_alex", walletID: "wal_acme_alex_usd"}
	if got != want {
		t.Fatalf("identity = %#v, want %#v", got, want)
	}

	for _, bad := range []map[string]any{
		{"tenant_id": "tn_acme", "user_id": "usr_alex"},
		{"tenant_id": "tn_acme", "user_id": 7, "wallet_id": "wal_acme_alex_usd"},
		{"tenant_id": "tn_acme", "user_id": "usr_alex", "wallet_id": " "},
	} {
		if _, err := walletIdentityFromInput(bad, "tenant_id", "user_id", "wallet_id"); err == nil {
			t.Errorf("walletIdentityFromInput(%#v) unexpectedly succeeded", bad)
		}
	}
}

func TestWalletMiddlewareRoutesInsufficientFundsWithoutRunningTarget(t *testing.T) {
	backend := &fakeReserver{reserveErr: cost.ErrBudgetExceeded}
	store := testWalletStore(backend)
	targetCalled := false
	got, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(context.Context, any) (any, error) {
			targetCalled = true
			return map[string]any{"route": "ai_draft"}, nil
		},
	)(context.Background(), walletRequest())
	if err != nil {
		t.Fatalf("low-balance result returned error: %v", err)
	}
	result, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", got)
	}
	if targetCalled {
		t.Fatal("paid action ran after wallet reservation was denied")
	}
	if result["quota_reserved"] != false || result["quota_state"] != "wallet_insufficient_funds" {
		t.Fatalf("wallet state = %#v", result)
	}
	if result["ticket_id"] != "ticket_42" {
		t.Fatalf("denial lost request fields: %#v", result)
	}
}

func TestWalletMiddlewareChargesEstimateAfterSuccess(t *testing.T) {
	backend := &fakeReserver{reservation: &fakeReservation{}}
	store := testWalletStore(backend)
	var gotIdentity walletIdentity
	var gotDomain, gotOperation string
	store.walletFactory = func(_ context.Context, identity walletIdentity, domain, operation string) (cost.Reserver, error) {
		gotIdentity, gotDomain, gotOperation = identity, domain, operation
		return backend, nil
	}

	got, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(context.Context, any) (any, error) {
			return map[string]any{"route": "ai_draft"}, nil
		},
	)(context.Background(), walletRequest())
	if err != nil {
		t.Fatalf("successful action returned error: %v", err)
	}
	result, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", got)
	}
	if result["quota_reserved"] != true || result["quota_state"] != "wallet_charged" {
		t.Fatalf("wallet state = %#v", result)
	}
	if gotIdentity != (walletIdentity{tenantID: "tn_acme", userID: "usr_alex", walletID: "wal_acme_alex_usd"}) {
		t.Fatalf("wallet identity = %#v", gotIdentity)
	}
	if gotDomain != "pipeline" || gotOperation != "draft_reply" {
		t.Fatalf("action classification = %q.%q", gotDomain, gotOperation)
	}
	if !backend.reservation.commitCalled || backend.reservation.committed != 200_000 {
		t.Fatalf("committed estimate = %#v, want 200000 micros", backend.reservation)
	}
	if len(backend.events) != 0 {
		t.Fatalf("wallet path should write the debit and action label atomically, not a second monthly event: %#v", backend.events)
	}
}

func TestWalletMiddlewareDoesNotTurnSettlementOverdrawIntoLowBalanceBranch(t *testing.T) {
	backend := &fakeReserver{reservation: &fakeReservation{commitErr: cost.ErrBudgetExceeded}}
	store := testWalletStore(backend)
	got, err := store.quotaMiddleware(200_000, "pipeline.draft_reply")(
		func(context.Context, any) (any, error) { return map[string]any{"route": "ai_draft"}, nil },
	)(context.Background(), walletRequest())
	if got != nil {
		t.Fatalf("failed settlement output = %#v, want nil", got)
	}
	if !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("settlement error = %v, want ErrBudgetExceeded", err)
	}
	if !backend.reservation.releaseCalled {
		t.Fatal("failed settlement must attempt to release the uncommitted hold")
	}
}

func TestNewPostgresStoreValidatesWalletModeBeforeConnecting(t *testing.T) {
	base := Config{
		Backend: "postgres", AccountingMode: "wallet", Currency: "USD", DSN: "postgres://localhost/cost",
		SchemaMode: "external", TenantField: "tenant_id", UserField: "user_id", WalletField: "wallet_id",
		ReservationTTL: "5m", MaxOpenConns: 10, MaxIdleConns: 5,
	}
	for _, test := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "auto DDL forbidden", edit: func(cfg *Config) { cfg.SchemaMode = "auto" }, want: "schema_mode=external"},
		{name: "invalid user field", edit: func(cfg *Config) { cfg.UserField = "user.id" }, want: "user_field"},
		{name: "invalid wallet field", edit: func(cfg *Config) { cfg.WalletField = "wallet-id" }, want: "wallet_field"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			test.edit(&cfg)
			if _, err := newPostgresStore(cfg, cost.USD); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newPostgresStore error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func testWalletStore(backend *fakeReserver) *postgresStore {
	return &postgresStore{
		accountingMode: "wallet",
		currency:       cost.USD,
		tenantField:    "tenant_id",
		userField:      "user_id",
		walletField:    "wallet_id",
		reservationTTL: 5 * time.Minute,
		walletFactory: func(context.Context, walletIdentity, string, string) (cost.Reserver, error) {
			return backend, nil
		},
	}
}

func walletRequest() map[string]any {
	return map[string]any{
		"tenant_id": "tn_acme", "user_id": "usr_alex", "wallet_id": "wal_acme_alex_usd",
		"ticket_id": "ticket_42",
	}
}
