package cost

import (
	"errors"
	"fmt"
)

var (
	ErrNilContext          = errors.New("cost: nil context")
	ErrBudgetExceeded      = errors.New("cost: budget exceeded")
	ErrValidation          = errors.New("cost: validation error")
	ErrInvalidEstimate     = errors.New("cost: invalid estimate")
	ErrInvalidCost         = errors.New("cost: invalid cost")
	ErrReservationNotFound = errors.New("cost: reservation not found or expired")
)

type ValidationError struct {
	Field string
	Value any
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("cost: invalid %s: %v", e.Field, e.Value)
}

func (e *ValidationError) Unwrap() error {
	switch e.Field {
	case "estimate_micros":
		return ErrInvalidEstimate
	case "cost_micros":
		return ErrInvalidCost
	default:
		return ErrValidation
	}
}
