package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nexssp/cost"
)

const Schema = `
CREATE TABLE IF NOT EXISTS cost_budgets (
	scope TEXT PRIMARY KEY,
	limit_micros BIGINT NOT NULL,
	used_micros BIGINT NOT NULL DEFAULT 0,
	currency CHAR(3) NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cost_reservations (
	id TEXT PRIMARY KEY,
	scope TEXT NOT NULL REFERENCES cost_budgets(scope) ON DELETE CASCADE,
	amount_micros BIGINT NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS cost_reservations_expiry_idx ON cost_reservations(scope, expires_at);

CREATE TABLE IF NOT EXISTS cost_entries (
	id BIGSERIAL PRIMARY KEY,
	scope TEXT NOT NULL,
	domain TEXT NOT NULL,
	operation TEXT NOT NULL,
	cost_micros BIGINT NOT NULL,
	currency CHAR(3) NOT NULL,
	event_id TEXT,
	recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS cost_entries_scope_time_idx ON cost_entries(scope, recorded_at);
CREATE UNIQUE INDEX IF NOT EXISTS cost_entries_scope_event_id_idx ON cost_entries(scope, event_id) WHERE event_id IS NOT NULL;
`

type Config struct {
	DB             *sql.DB
	Scope          string
	LimitMicros    int64
	Currency       cost.Currency
	ReservationTTL time.Duration
}

type Ledger struct {
	db       *sql.DB
	scope    string
	limit    int64
	currency cost.Currency
	ttl      time.Duration
}

func New(cfg Config) *Ledger {
	ttl := cfg.ReservationTTL
	if ttl < time.Second {
		ttl = 30 * time.Second
	}

	return &Ledger{
		db:       cfg.DB,
		scope:    cfg.Scope,
		limit:    cfg.LimitMicros,
		currency: cfg.Currency,
		ttl:      ttl,
	}
}

func (l *Ledger) EnsureSchema(ctx context.Context) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if l.db == nil {
		return fmt.Errorf("postgres: nil database")
	}

	if _, err := l.db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("postgres: schema: %w", err)
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin schema upsert: %w", err)
	}
	defer tx.Rollback()

	var existingCurrency string
	err = tx.QueryRowContext(ctx, `SELECT currency FROM cost_budgets WHERE scope = $1 FOR UPDATE`, l.scope).Scan(&existingCurrency)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("postgres: check existing currency: %w", err)
	}

	if err == nil && existingCurrency != l.currency.String() {
		return fmt.Errorf("postgres: currency mismatch for scope %s: database has %s, config has %s", l.scope, existingCurrency, l.currency)
	}

	upsert := `
		INSERT INTO cost_budgets(scope, limit_micros, currency)
		VALUES($1, $2, $3)
		ON CONFLICT(scope) DO UPDATE
		SET limit_micros = EXCLUDED.limit_micros
	`
	if _, err = tx.ExecContext(ctx, upsert, l.scope, l.limit, l.currency.String()); err != nil {
		return fmt.Errorf("postgres: upsert budget: %w", err)
	}

	return tx.Commit()
}

func (l *Ledger) Reserve(ctx context.Context, estimate int64) (cost.Reservation, error) {
	if ctx == nil {
		return nil, cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.db == nil {
		return nil, fmt.Errorf("postgres: nil database")
	}
	if estimate < 0 || estimate > math.MaxInt64/2 {
		return nil, &cost.ValidationError{
			Field: "estimate_micros",
			Value: estimate,
		}
	}

	id, err := newID()
	if err != nil {
		return nil, fmt.Errorf("postgres: reservation id: %w", err)
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin: %w", err)
	}
	defer tx.Rollback()

	var used, limit int64
	var currency string
	row := tx.QueryRowContext(ctx, `
		SELECT used_micros, limit_micros, currency
		FROM cost_budgets
		WHERE scope = $1
		FOR UPDATE
	`, l.scope)

	if err = row.Scan(&used, &limit, &currency); err != nil {
		return nil, fmt.Errorf("postgres: load budget: %w", err)
	}

	if currency != l.currency.String() {
		return nil, fmt.Errorf("postgres: currency mismatch: database=%s ledger=%s", currency, l.currency)
	}

	// Reclaim expired reservations
	var reclaimed int64
	reclaimQuery := `
		WITH expired AS (
			DELETE FROM cost_reservations
			WHERE scope = $1 AND expires_at <= CURRENT_TIMESTAMP
			RETURNING amount_micros
		)
		SELECT COALESCE(SUM(amount_micros), 0) FROM expired
	`
	if err = tx.QueryRowContext(ctx, reclaimQuery, l.scope).Scan(&reclaimed); err != nil {
		return nil, fmt.Errorf("postgres: reclaim: %w", err)
	}

	if reclaimed > 0 {
		used -= reclaimed
		_, err = tx.ExecContext(ctx, `
			UPDATE cost_budgets
			SET used_micros = $1, updated_at = CURRENT_TIMESTAMP
			WHERE scope = $2
		`, used, l.scope)
		if err != nil {
			return nil, fmt.Errorf("postgres: update reclaimed budget: %w", err)
		}
	}

	if limit >= 0 && used > limit-estimate {
		// Commit the reclaimed budget so expired leases remain freed
		_ = tx.Commit()
		return nil, fmt.Errorf("%w: used=%d estimate=%d limit=%d", cost.ErrBudgetExceeded, used, estimate, limit)
	}

	seconds := int64(l.ttl / time.Second)
	if seconds < 1 {
		seconds = 1
	}

	insertReservation := `
		INSERT INTO cost_reservations(id, scope, amount_micros, expires_at)
		VALUES($1, $2, $3, CURRENT_TIMESTAMP + ($4 * INTERVAL '1 second'))
	`
	if _, err = tx.ExecContext(ctx, insertReservation, id, l.scope, estimate, seconds); err != nil {
		return nil, fmt.Errorf("postgres: reserve row: %w", err)
	}

	updateBudget := `
		UPDATE cost_budgets
		SET used_micros = $1, updated_at = CURRENT_TIMESTAMP
		WHERE scope = $2
	`
	if _, err = tx.ExecContext(ctx, updateBudget, used+estimate, l.scope); err != nil {
		return nil, fmt.Errorf("postgres: update budget: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres: commit reserve: %w", err)
	}

	return &reservation{
		ledger: l,
		id:     id,
	}, nil
}

func (l *Ledger) Record(ctx context.Context, event cost.Event) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.db == nil {
		return fmt.Errorf("postgres: nil database")
	}
	if event.CostMicros <= 0 {
		return &cost.ValidationError{
			Field: "cost_micros",
			Value: event.CostMicros,
		}
	}

	if event.Currency == (cost.Currency{}) {
		event.Currency = l.currency
	}
	if event.Currency != l.currency {
		return fmt.Errorf("postgres: currency mismatch")
	}

	insertEntry := `
		INSERT INTO cost_entries(scope, domain, operation, cost_micros, currency, event_id)
		VALUES($1, $2, $3, $4, $5, $6)
		ON CONFLICT (scope, event_id) WHERE event_id IS NOT NULL DO NOTHING
	`
	_, err := l.db.ExecContext(ctx, insertEntry, l.scope, event.Domain, event.Operation, event.CostMicros, event.Currency.String(), event.ID)
	return err
}

type reservation struct {
	ledger *Ledger
	id     string
	mu     sync.Mutex
	done   bool
}

func (r *reservation) Commit(ctx context.Context, actual int64) error {
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

	tx, err := r.ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin commit: %w", err)
	}
	defer tx.Rollback()

	var reserved int64
	deleteReservation := `DELETE FROM cost_reservations WHERE id = $1 RETURNING amount_micros`
	if err = tx.QueryRowContext(ctx, deleteReservation, r.id).Scan(&reserved); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
		}
		return fmt.Errorf("postgres: commit reservation: %w", err)
	}

	updateBudget := `
		UPDATE cost_budgets
		SET used_micros = used_micros + $1, updated_at = CURRENT_TIMESTAMP
		WHERE scope = $2
	`
	if _, err = tx.ExecContext(ctx, updateBudget, actual-reserved, r.ledger.scope); err != nil {
		return fmt.Errorf("postgres: commit budget: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgres: commit transaction: %w", err)
	}

	r.done = true
	return nil
}

func (r *reservation) Release(ctx context.Context) error {
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

	tx, err := r.ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin release: %w", err)
	}
	defer tx.Rollback()

	var reserved int64
	deleteReservation := `DELETE FROM cost_reservations WHERE id = $1 RETURNING amount_micros`
	if err = tx.QueryRowContext(ctx, deleteReservation, r.id).Scan(&reserved); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
		}
		return fmt.Errorf("postgres: release reservation: %w", err)
	}

	updateBudget := `
		UPDATE cost_budgets
		SET used_micros = used_micros - $1, updated_at = CURRENT_TIMESTAMP
		WHERE scope = $2
	`
	if _, err = tx.ExecContext(ctx, updateBudget, reserved, r.ledger.scope); err != nil {
		return fmt.Errorf("postgres: release budget: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgres: release transaction: %w", err)
	}

	r.done = true
	return nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
