package cost

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const auditRingCapacity = 1024

// Ledger is the bounded-memory, in-process implementation. Reserve/commit/
// release use atomics; only audit snapshots take a short read lock.
type Ledger struct {
	currency    Currency
	limitMicros int64
	usedMicros  atomic.Int64
	spentMicros atomic.Int64
	mu          sync.RWMutex
	head        uint64
	entries     [auditRingCapacity]Event
}

func NewLedger(limitMicros int64, currency Currency) *Ledger {
	return &Ledger{
		limitMicros: limitMicros,
		currency:    currency,
	}
}

func (l *Ledger) UsedMicros() int64 {
	return l.usedMicros.Load()
}

func (l *Ledger) SpentMicros() int64 {
	return l.spentMicros.Load()
}

func (l *Ledger) LimitMicros() int64 {
	return l.limitMicros
}

func (l *Ledger) Currency() Currency {
	return l.currency
}

func (l *Ledger) Reserve(ctx context.Context, estimateMicros int64) (Reservation, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if estimateMicros < 0 || estimateMicros > math.MaxInt64/2 {
		return nil, &ValidationError{
			Field: "estimate_micros",
			Value: estimateMicros,
		}
	}

	for {
		used := l.usedMicros.Load()

		// Guard against signed integer overflow on unlimited (limit < 0) or massive budgets
		if estimateMicros > math.MaxInt64-used {
			return nil, fmt.Errorf(
				"%w: overflow prevented used=%d estimate=%d",
				ErrBudgetExceeded, used, estimateMicros,
			)
		}

		if l.limitMicros >= 0 && used > l.limitMicros-estimateMicros {
			return nil, fmt.Errorf(
				"%w: used=%d estimate=%d limit=%d",
				ErrBudgetExceeded, used, estimateMicros, l.limitMicros,
			)
		}

		if l.usedMicros.CompareAndSwap(used, used+estimateMicros) {
			return &localReservation{
				ledger:   l,
				reserved: estimateMicros,
			}, nil
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (l *Ledger) Record(ctx context.Context, event Event) error {
	if ctx == nil {
		return ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if event.CostMicros <= 0 {
		return &ValidationError{
			Field: "cost_micros",
			Value: event.CostMicros,
		}
	}

	if event.Currency == (Currency{}) {
		event.Currency = l.currency
	}

	if event.Currency != l.currency {
		return fmt.Errorf("cost: currency mismatch: event=%s ledger=%s", event.Currency, l.currency)
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	l.mu.Lock()
	l.entries[l.head%auditRingCapacity] = event
	l.head++
	l.mu.Unlock()

	return nil
}

func (l *Ledger) Entries() []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	count := l.head
	if count > auditRingCapacity {
		count = auditRingCapacity
	}

	n := int(count)
	out := make([]Event, n)
	start := l.head - count

	for i := range out {
		out[i] = l.entries[(start+uint64(i))%auditRingCapacity]
	}

	return out
}
