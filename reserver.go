package cost

import "context"

// Reserver is the backend-neutral budget contract.
type Reserver interface {
	Reserve(ctx context.Context, estimateMicros int64) (Reservation, error)
	Record(ctx context.Context, event Event) error
}

// Reservation is a terminal, idempotent lease. Terminal operations return
// backend failures; callers must not claim success when an error is returned.
type Reservation interface {
	Commit(ctx context.Context, actualMicros int64) error
	Release(ctx context.Context) error
}
