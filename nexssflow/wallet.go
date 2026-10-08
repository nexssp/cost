package nexssflow

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/nexssp/cost"
)

type walletIdentity struct {
	tenantID string
	userID   string
	walletID string
}

const activeStatus = "active"

type walletLedger struct {
	db        *sql.DB
	identity  walletIdentity
	currency  cost.Currency
	ttl       time.Duration
	domain    string
	operation string
}

func (l *walletLedger) Reserve(ctx context.Context, estimate int64) (cost.Reservation, error) {
	tx, id, err := l.beginReservation(ctx, estimate)
	if err != nil {
		return nil, err
	}
	defer rollbackWalletTx(tx)

	balance, reserved, status, err := l.lockWallet(ctx, tx, true)
	if err != nil {
		return nil, err
	}
	if status != activeStatus {
		return nil, fmt.Errorf("postgres wallet: wallet %q is %q", l.identity.walletID, status)
	}

	reserved, err = l.reclaimExpired(ctx, tx, reserved)
	if err != nil {
		return nil, err
	}
	if balance < 0 || reserved < 0 || reserved > balance {
		return nil, fmt.Errorf("postgres wallet: invalid balance state for wallet %q", l.identity.walletID)
	}
	if estimate > balance-reserved {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("postgres wallet: commit expired-reservation cleanup: %w", err)
		}
		return nil, fmt.Errorf("%w: wallet=%s available=%d estimate=%d", cost.ErrBudgetExceeded, l.identity.walletID, balance-reserved, estimate)
	}
	if err := l.insertReservation(ctx, tx, id, estimate); err != nil {
		return nil, err
	}
	if err := l.updateReservedAmount(ctx, tx, reserved+estimate); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres wallet: commit reservation: %w", err)
	}
	return &walletReservation{ledger: l, id: id}, nil
}

func (l *walletLedger) beginReservation(ctx context.Context, estimate int64) (*sql.Tx, string, error) {
	if err := validateWalletContext(ctx); err != nil {
		return nil, "", err
	}
	if err := l.validateEstimate(estimate); err != nil {
		return nil, "", err
	}
	id, err := newWalletReservationID()
	if err != nil {
		return nil, "", fmt.Errorf("postgres wallet: reservation id: %w", err)
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", fmt.Errorf("postgres wallet: begin reservation: %w", err)
	}
	return tx, id, nil
}

func validateWalletContext(ctx context.Context) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	return ctx.Err()
}

func (l *walletLedger) validateEstimate(estimate int64) error {
	if estimate < 0 || estimate > math.MaxInt64/2 {
		return &cost.ValidationError{Field: "estimate_micros", Value: estimate}
	}
	if l.db == nil {
		return errors.New("postgres wallet: nil database")
	}
	return nil
}

func (l *walletLedger) reclaimExpired(ctx context.Context, tx *sql.Tx, reserved int64) (int64, error) {
	var reclaimed int64
	err := tx.QueryRowContext(ctx, `
		WITH expired AS (
			DELETE FROM cost_wallet_reservations
			WHERE wallet_id = $1 AND expires_at <= CURRENT_TIMESTAMP
			RETURNING amount_micros
		)
		SELECT COALESCE(SUM(amount_micros), 0) FROM expired
	`, l.identity.walletID).Scan(&reclaimed)
	if err != nil {
		return 0, fmt.Errorf("postgres wallet: reclaim expired reservations: %w", err)
	}
	if reclaimed > reserved {
		return 0, fmt.Errorf("postgres wallet: reservation invariant violated for wallet %q", l.identity.walletID)
	}
	reserved -= reclaimed
	if reclaimed > 0 {
		if err := l.updateReservedAmount(ctx, tx, reserved); err != nil {
			return 0, fmt.Errorf("postgres wallet: release expired holds: %w", err)
		}
	}
	return reserved, nil
}

func (l *walletLedger) insertReservation(ctx context.Context, tx *sql.Tx, id string, estimate int64) error {
	seconds := max(int64(l.ttl/time.Second), 1)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cost_wallet_reservations(id, wallet_id, amount_micros, expires_at)
		VALUES($1, $2, $3, CURRENT_TIMESTAMP + ($4 * INTERVAL '1 second'))
	`, id, l.identity.walletID, estimate, seconds); err != nil {
		return fmt.Errorf("postgres wallet: insert reservation: %w", err)
	}
	return nil
}

func (l *walletLedger) updateReservedAmount(ctx context.Context, tx *sql.Tx, reserved int64) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE cost_wallets
		SET reserved_micros = $1, updated_at = CURRENT_TIMESTAMP
		WHERE wallet_id = $2
	`, reserved, l.identity.walletID); err != nil {
		return fmt.Errorf("postgres wallet: hold funds: %w", err)
	}
	return nil
}

// Record appends post-hoc audit events. Successful guarded actions are written
// atomically with the wallet debit by Commit; this method serves explicit audit
// records and is intentionally balance-neutral.
func (l *walletLedger) Record(ctx context.Context, event cost.Event) error {
	if ctx == nil {
		return cost.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.db == nil {
		return errors.New("postgres wallet: nil database")
	}
	if event.CostMicros <= 0 {
		return &cost.ValidationError{Field: "cost_micros", Value: event.CostMicros}
	}
	if event.Currency == (cost.Currency{}) {
		event.Currency = l.currency
	}
	if event.Currency != l.currency {
		return errors.New("postgres wallet: currency mismatch")
	}
	_, err := l.db.ExecContext(ctx, `
		INSERT INTO cost_wallet_usage_events(wallet_id, domain, operation, cost_micros, currency, event_id)
		VALUES($1, $2, $3, $4, $5, NULLIF($6, ''))
		ON CONFLICT (wallet_id, event_id) WHERE event_id IS NOT NULL DO NOTHING
	`, l.identity.walletID, event.Domain, event.Operation, event.CostMicros, event.Currency.String(), event.ID)
	if err != nil {
		return fmt.Errorf("postgres wallet: record usage event: %w", err)
	}
	return nil
}

func (l *walletLedger) lockWallet(ctx context.Context, tx *sql.Tx, requireActiveOwner bool) (balance, reserved int64, status string, err error) {
	var currency, tenantStatus, userStatus string
	if requireActiveOwner {
		err = tx.QueryRowContext(ctx, `
			SELECT w.balance_micros, w.reserved_micros, w.currency, w.status, t.status, u.status
			FROM cost_wallets AS w
			JOIN cost_tenants AS t ON t.tenant_id = w.tenant_id
			JOIN cost_users AS u ON u.tenant_id = w.tenant_id AND u.user_id = w.user_id
			WHERE w.wallet_id = $1 AND w.tenant_id = $2 AND w.user_id = $3
			FOR UPDATE OF w
		`, l.identity.walletID, l.identity.tenantID, l.identity.userID).
			Scan(&balance, &reserved, &currency, &status, &tenantStatus, &userStatus)
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT balance_micros, reserved_micros, currency, status
			FROM cost_wallets
			WHERE wallet_id = $1 AND tenant_id = $2 AND user_id = $3
			FOR UPDATE
		`, l.identity.walletID, l.identity.tenantID, l.identity.userID).
			Scan(&balance, &reserved, &currency, &status)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", fmt.Errorf("postgres wallet: active wallet %q was not found for tenant %q and user %q", l.identity.walletID, l.identity.tenantID, l.identity.userID)
	}
	if err != nil {
		return 0, 0, "", fmt.Errorf("postgres wallet: load wallet: %w", err)
	}
	if currency != l.currency.String() {
		return 0, 0, "", fmt.Errorf("postgres wallet: currency mismatch: database=%s configured=%s", currency, l.currency)
	}
	if requireActiveOwner && tenantStatus != activeStatus {
		return 0, 0, "", fmt.Errorf("postgres wallet: tenant %q is %q", l.identity.tenantID, tenantStatus)
	}
	if requireActiveOwner && userStatus != activeStatus {
		return 0, 0, "", fmt.Errorf("postgres wallet: user %q is %q", l.identity.userID, userStatus)
	}
	return balance, reserved, status, nil
}

type walletReservation struct {
	ledger *walletLedger
	id     string
	mu     sync.Mutex
	done   bool
}

func (r *walletReservation) Commit(ctx context.Context, actual int64) error {
	if err := validateWalletContext(ctx); err != nil {
		return err
	}
	if actual < 0 {
		actual = 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}

	tx, err := r.ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres wallet: begin settlement: %w", err)
	}
	defer rollbackWalletTx(tx)

	balance, reserved, status, err := r.ledger.lockWallet(ctx, tx, true)
	if err != nil {
		return err
	}
	if status != activeStatus {
		return fmt.Errorf("postgres wallet: wallet %q is %q", r.ledger.identity.walletID, status)
	}
	if err := r.settle(ctx, tx, balance, reserved, actual); err != nil {
		return err
	}
	r.done = true
	return nil
}

func (r *walletReservation) settle(ctx context.Context, tx *sql.Tx, balance, reserved, actual int64) error {
	var held int64
	err := tx.QueryRowContext(ctx, `
		DELETE FROM cost_wallet_reservations
		WHERE id = $1 AND wallet_id = $2 AND expires_at > CURRENT_TIMESTAMP
		RETURNING amount_micros
	`, r.id, r.ledger.identity.walletID).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
	}
	if err != nil {
		return fmt.Errorf("postgres wallet: load settlement reservation: %w", err)
	}
	if held > reserved {
		return fmt.Errorf("postgres wallet: reservation invariant violated for wallet %q", r.ledger.identity.walletID)
	}
	availableAfterRelease := balance - (reserved - held)
	if availableAfterRelease < 0 || actual > availableAfterRelease {
		return fmt.Errorf("%w: wallet=%s available_after_release=%d actual=%d", cost.ErrBudgetExceeded, r.ledger.identity.walletID, availableAfterRelease, actual)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE cost_wallets
		SET balance_micros = balance_micros - $1,
		    reserved_micros = reserved_micros - $2,
		    updated_at = CURRENT_TIMESTAMP
		WHERE wallet_id = $3
	`, actual, held, r.ledger.identity.walletID); err != nil {
		return fmt.Errorf("postgres wallet: debit wallet: %w", err)
	}
	if actual > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO cost_wallet_entries(wallet_id, entry_type, amount_micros, idempotency_key, domain, operation)
			VALUES($1, 'debit', $2, $3, $4, $5)
		`, r.ledger.identity.walletID, actual, r.id, r.ledger.domain, r.ledger.operation); err != nil {
			return fmt.Errorf("postgres wallet: write debit entry: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres wallet: commit settlement: %w", err)
	}
	return nil
}

func (r *walletReservation) Release(ctx context.Context) error {
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
		return fmt.Errorf("postgres wallet: begin release: %w", err)
	}
	defer rollbackWalletTx(tx)
	_, _, _, err = r.ledger.lockWallet(ctx, tx, false)
	if err != nil {
		return err
	}
	var held int64
	err = tx.QueryRowContext(ctx, `
		DELETE FROM cost_wallet_reservations
		WHERE id = $1 AND wallet_id = $2
		RETURNING amount_micros
	`, r.id, r.ledger.identity.walletID).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", cost.ErrReservationNotFound, r.id)
	}
	if err != nil {
		return fmt.Errorf("postgres wallet: release reservation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE cost_wallets
		SET reserved_micros = reserved_micros - $1, updated_at = CURRENT_TIMESTAMP
		WHERE wallet_id = $2
	`, held, r.ledger.identity.walletID); err != nil {
		return fmt.Errorf("postgres wallet: release held funds: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres wallet: commit release: %w", err)
	}
	r.done = true
	return nil
}

func newWalletReservationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func rollbackWalletTx(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		slog.Error("failed to roll back PostgreSQL wallet transaction", "error", err)
	}
}
