package nexssflow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx as the database/sql driver.
	"github.com/nexssp/cost"
	postgres "github.com/nexssp/cost/adapters/postgres"
	"github.com/nexssp/flow/core"
	"github.com/nexssp/kernel/action"
)

const (
	accountingTimeout     = 5 * time.Second
	accountingModeMonthly = "monthly"
	accountingModeWallet  = "wallet"
	fallbackActionName    = "action"
)

type postgresStore struct {
	db             *sql.DB
	budget         int64
	currency       cost.Currency
	schemaMode     string
	accountingMode string
	scopePrefix    string
	tenantField    string
	userField      string
	walletField    string
	reservationTTL time.Duration
	ledgerFactory  func(context.Context, string) (cost.Reserver, error)
	walletFactory  func(context.Context, walletIdentity, string, string) (cost.Reserver, error)
}

func newPostgresStore(cfg Config, currency cost.Currency) (*postgresStore, error) {
	settings, err := parsePostgresSettings(cfg, currency)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", settings.dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	db.SetMaxOpenConns(settings.maxOpenConns)
	db.SetMaxIdleConns(settings.maxIdleConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), accountingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return &postgresStore{
		db:             db,
		budget:         settings.budget,
		currency:       settings.currency,
		schemaMode:     settings.schemaMode,
		accountingMode: settings.accountingMode,
		scopePrefix:    settings.scopePrefix,
		tenantField:    settings.tenantField,
		userField:      settings.userField,
		walletField:    settings.walletField,
		reservationTTL: settings.reservationTTL,
	}, nil
}

type postgresSettings struct {
	dsn            string
	budget         int64
	currency       cost.Currency
	schemaMode     string
	accountingMode string
	scopePrefix    string
	tenantField    string
	userField      string
	walletField    string
	reservationTTL time.Duration
	maxOpenConns   int
	maxIdleConns   int
}

func parsePostgresSettings(cfg Config, currency cost.Currency) (postgresSettings, error) {
	settings := postgresSettings{
		dsn:            strings.TrimSpace(os.ExpandEnv(cfg.DSN)),
		budget:         cfg.Budget,
		currency:       currency,
		schemaMode:     strings.ToLower(strings.TrimSpace(cfg.SchemaMode)),
		accountingMode: strings.ToLower(strings.TrimSpace(cfg.AccountingMode)),
		scopePrefix:    strings.TrimSpace(cfg.ScopePrefix),
		tenantField:    cfg.TenantField,
		userField:      cfg.UserField,
		walletField:    cfg.WalletField,
		maxOpenConns:   cfg.MaxOpenConns,
		maxIdleConns:   cfg.MaxIdleConns,
	}
	if settings.dsn == "" {
		return postgresSettings{}, errors.New("backend=postgres requires dsn; use an environment reference such as $COST_POSTGRES_DSN")
	}
	if settings.schemaMode != "external" && settings.schemaMode != "auto" {
		return postgresSettings{}, fmt.Errorf("schema_mode %q must be external or auto", cfg.SchemaMode)
	}
	if settings.accountingMode == "" {
		settings.accountingMode = accountingModeMonthly
	}
	if settings.accountingMode != accountingModeMonthly && settings.accountingMode != accountingModeWallet {
		return postgresSettings{}, fmt.Errorf("accounting_mode %q must be monthly or wallet", cfg.AccountingMode)
	}
	if err := validatePostgresAccountingSettings(cfg, settings); err != nil {
		return postgresSettings{}, err
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(cfg.ReservationTTL))
	if err != nil || ttl < time.Second {
		return postgresSettings{}, fmt.Errorf("reservation_ttl must be at least 1s: %q", cfg.ReservationTTL)
	}
	settings.reservationTTL = ttl
	if settings.maxOpenConns < 1 {
		return postgresSettings{}, errors.New("max_open_conns must be at least 1")
	}
	if settings.maxIdleConns < 0 {
		return postgresSettings{}, errors.New("max_idle_conns must not be negative")
	}
	settings.maxIdleConns = min(settings.maxIdleConns, settings.maxOpenConns)
	return settings, nil
}

func validatePostgresAccountingSettings(cfg Config, settings postgresSettings) error {
	if !isIdentifier(settings.tenantField) {
		return fmt.Errorf("tenant_field %q must be a top-level identifier", settings.tenantField)
	}
	if settings.accountingMode == accountingModeMonthly {
		if settings.scopePrefix == "" || len(settings.scopePrefix) > 128 || strings.ContainsAny(settings.scopePrefix, "\x00\r\n") {
			return errors.New("scope_prefix must be a non-empty string of at most 128 characters")
		}
		if settings.budget < 0 {
			return fmt.Errorf("a PostgreSQL monthly budget must be non-negative; unlimited budgets are unsupported for durable quota (accounting_mode=%q)", cfg.AccountingMode)
		}
		return nil
	}
	if settings.schemaMode != "external" {
		return errors.New("accounting_mode=wallet requires schema_mode=external; provision wallet tables through migrations")
	}
	if !isIdentifier(settings.userField) {
		return fmt.Errorf("user_field %q must be a top-level identifier", settings.userField)
	}
	if !isIdentifier(settings.walletField) {
		return fmt.Errorf("wallet_field %q must be a top-level identifier", settings.walletField)
	}
	return nil
}

func postgresBundle(store *postgresStore) core.Bundle {
	modifier := costEstimateModifierWithApply(func(builder *action.Builder[any, any], estimate int64) {
		name := ""
		if meta := builder.Describe(); meta != nil {
			name = meta.Name
		}
		builder.UseFirst(store.quotaMiddleware(estimate, name))
	})
	return core.Bundle{
		ID:              ID,
		Modifiers:       []core.Modifier{modifier},
		AcceptedOptions: acceptedOptions,
		Shutdowns: []core.ShutdownFunc{func(context.Context) error {
			return store.db.Close()
		}},
	}
}

func (s *postgresStore) quotaMiddleware(estimate int64, actionName string) action.Middleware[any, any] {
	return func(next action.Fn[any, any]) action.Fn[any, any] {
		return func(ctx context.Context, input any) (output any, resultErr error) {
			ledger, deniedState, err := s.ledgerForInput(ctx, input, actionName, time.Now().UTC())
			if err != nil {
				return nil, err
			}
			reservation, err := ledger.Reserve(ctx, estimate)
			if err != nil {
				if errors.Is(err, cost.ErrBudgetExceeded) {
					return setQuotaState(input, false, deniedState)
				}
				return nil, fmt.Errorf("cost: reserve %s for account: %w", s.accountingMode, err)
			}

			settled := false
			defer func() {
				recovered := recover()
				if !settled {
					if cleanupErr := finishReservation(ctx, reservation, false, 0); cleanupErr != nil {
						wrappedErr := fmt.Errorf("cost: release %s reservation: %w", s.accountingMode, cleanupErr)
						if recovered == nil {
							resultErr = errors.Join(resultErr, wrappedErr)
						} else {
							slog.Error("failed to release PostgreSQL reservation after panic", "error", wrappedErr)
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
				return nil, fmt.Errorf("cost: settle %s reservation: %w", s.accountingMode, err)
			}
			settled = true
			if actual > 0 {
				if s.accountingMode == "wallet" {
					// Commit writes the debit and action classification atomically.
					return setQuotaState(output, true, "wallet_charged")
				}
				domain, operation := actionDomainOperation(actionName)
				recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
				defer cancel()
				if err := ledger.Record(recordCtx, cost.Event{
					Domain: domain, Operation: operation, CostMicros: actual, Currency: s.currency,
				}); err != nil {
					return nil, fmt.Errorf("cost: record durable usage: %w", err)
				}
			}
			return setQuotaState(output, true, "reserved")
		}
	}
}

func (s *postgresStore) ledgerForInput(ctx context.Context, input any, actionName string, now time.Time) (cost.Reserver, string, error) {
	if s.accountingMode == "wallet" {
		identity, err := walletIdentityFromInput(input, s.tenantField, s.userField, s.walletField)
		if err != nil {
			return nil, "", err
		}
		var ledger cost.Reserver
		if s.walletFactory != nil {
			var err error
			domain, operation := actionDomainOperation(actionName)
			ledger, err = s.walletFactory(ctx, identity, domain, operation)
			if err != nil {
				return nil, "", err
			}
		} else {
			domain, operation := actionDomainOperation(actionName)
			ledger = &walletLedger{
				db:        s.db,
				identity:  identity,
				currency:  s.currency,
				ttl:       s.reservationTTL,
				domain:    domain,
				operation: operation,
			}
		}
		return ledger, "wallet_insufficient_funds", nil
	}

	tenant, err := tenantValue(input, s.tenantField)
	if err != nil {
		return nil, "", err
	}
	ledger, err := s.ledgerForTenant(ctx, tenant, now)
	if err != nil {
		return nil, "", err
	}
	return ledger, "monthly_quota_exceeded", nil
}

func (s *postgresStore) ledgerForTenant(ctx context.Context, tenant string, now time.Time) (cost.Reserver, error) {
	scope, err := monthlyScope(s.scopePrefix, tenant, now)
	if err != nil {
		return nil, err
	}
	if s.ledgerFactory != nil {
		return s.ledgerFactory(ctx, scope)
	}
	ledger := postgres.New(postgres.Config{
		DB:             s.db,
		Scope:          scope,
		LimitMicros:    s.budget,
		Currency:       s.currency,
		ReservationTTL: s.reservationTTL,
	})
	if s.schemaMode == "auto" {
		if err := ledger.EnsureSchema(ctx); err != nil {
			return nil, fmt.Errorf("initialize PostgreSQL monthly quota: %w", err)
		}
	} else if err := s.ensureBudgetRow(ctx, scope); err != nil {
		return nil, fmt.Errorf("initialize PostgreSQL monthly quota: %w", err)
	}
	return ledger, nil
}

func (s *postgresStore) ensureBudgetRow(ctx context.Context, scope string) error {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO cost_budgets(scope, limit_micros, currency)
		VALUES($1, $2, $3)
		ON CONFLICT(scope) DO UPDATE
		SET limit_micros = EXCLUDED.limit_micros
		WHERE cost_budgets.currency = EXCLUDED.currency
	`, scope, s.budget, s.currency.String())
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 0 {
		return nil
	}
	var existingCurrency string
	if err := s.db.QueryRowContext(ctx, `SELECT currency FROM cost_budgets WHERE scope = $1`, scope).Scan(&existingCurrency); err != nil {
		return err
	}
	return fmt.Errorf("currency mismatch for scope %s: database has %s, config has %s", scope, existingCurrency, s.currency)
}

func monthlyScope(prefix, tenant string, now time.Time) (string, error) {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return "", errors.New("cost: tenant identifier is required for durable quota")
	}
	digest := sha256.Sum256([]byte(tenant))
	return prefix + ":" + hex.EncodeToString(digest[:]) + ":" + now.UTC().Format("2006-01"), nil
}

func tenantValue(input any, field string) (string, error) {
	value, err := stringField(input, field)
	if err != nil {
		return "", fmt.Errorf("cost: %w", err)
	}
	return value, nil
}

func walletIdentityFromInput(input any, tenantField, userField, walletField string) (walletIdentity, error) {
	tenantID, err := stringField(input, tenantField)
	if err != nil {
		return walletIdentity{}, fmt.Errorf("cost: %w", err)
	}
	userID, err := stringField(input, userField)
	if err != nil {
		return walletIdentity{}, fmt.Errorf("cost: %w", err)
	}
	walletID, err := stringField(input, walletField)
	if err != nil {
		return walletIdentity{}, fmt.Errorf("cost: %w", err)
	}
	return walletIdentity{tenantID: tenantID, userID: userID, walletID: walletID}, nil
}

func stringField(input any, field string) (string, error) {
	var raw any
	var ok bool
	switch object := input.(type) {
	case map[string]any:
		raw, ok = object[field]
	case map[string]string:
		value, found := object[field]
		raw, ok = value, found
	default:
		return "", fmt.Errorf("durable accounting requires an object input with string field %q, got %T", field, input)
	}
	if !ok {
		return "", fmt.Errorf("durable accounting input is missing identity field %q", field)
	}
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("durable accounting identity field %q must be a non-empty string", field)
	}
	return strings.TrimSpace(value), nil
}

func setQuotaState(value any, reserved bool, state string) (any, error) {
	var object map[string]any
	switch input := value.(type) {
	case map[string]any:
		object = input
	case map[string]string:
		object = make(map[string]any, len(input))
		for key, field := range input {
			object[key] = field
		}
	default:
		return nil, fmt.Errorf("cost: durable accounting target must return an object, got %T", value)
	}
	result := make(map[string]any, len(object)+2)
	maps.Copy(result, object)
	result["quota_reserved"] = reserved
	result["quota_state"] = state
	return result, nil
}

func finishReservation(ctx context.Context, reservation cost.Reservation, commit bool, actual int64) error {
	accountingCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if commit {
		return reservation.Commit(accountingCtx, actual)
	}
	return reservation.Release(accountingCtx)
}

func actionDomainOperation(name string) (domain, operation string) {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[:i], name[i+1:]
		}
	}
	if name == "" {
		return fallbackActionName, fallbackActionName
	}
	return fallbackActionName, name
}

func isIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if !validIdentifierCharacter(char, index == 0) {
			return false
		}
	}
	return true
}

func validIdentifierCharacter(char rune, first bool) bool {
	if char == '_' || (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') {
		return true
	}
	return !first && char >= '0' && char <= '9'
}
