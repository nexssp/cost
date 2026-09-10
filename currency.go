package cost

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// Currency is a 3-byte ISO-4217 code (USD, EUR) or custom credit code (TOK).
// Packed into 3 bytes for register-passing and zero heap allocations.
type Currency [3]byte

var (
	USD = Currency{'U', 'S', 'D'}
	EUR = Currency{'E', 'U', 'R'}
	GBP = Currency{'G', 'B', 'P'}
	PLN = Currency{'P', 'L', 'N'}
	TOK = Currency{'T', 'O', 'K'}
)

func (c Currency) String() string {
	return string(c[:])
}

func (c Currency) MarshalText() ([]byte, error) {
	if c == (Currency{}) {
		return nil, fmt.Errorf("cost: empty currency")
	}

	return c[:], nil
}

func (c *Currency) UnmarshalText(b []byte) error {
	if c == nil {
		return errors.New("cost: nil Currency pointer")
	}

	if len(b) != 3 {
		return fmt.Errorf("cost: currency must be exactly 3 bytes, got %d", len(b))
	}

	for _, ch := range b {
		if ch < 'A' || ch > 'Z' {
			return fmt.Errorf("cost: currency must contain uppercase ASCII letters")
		}
	}

	copy(c[:], b)

	return nil
}

func (c Currency) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

func (c *Currency) UnmarshalJSON(b []byte) error {
	if c == nil {
		return errors.New("cost: nil Currency pointer")
	}

	var s string

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	return c.UnmarshalText([]byte(s))
}

// Micro represents fixed-point 1/1,000,000th of a currency unit ($0.000001).
type Micro int64

func ToMicro(amount float64) Micro {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0
	}

	const (
		maxFloat = float64(math.MaxInt64) / 1_000_000.0
		minFloat = float64(math.MinInt64) / 1_000_000.0
	)

	if amount >= maxFloat {
		return Micro(math.MaxInt64)
	}

	if amount <= minFloat {
		return Micro(math.MinInt64)
	}

	return Micro(math.Round(amount * 1_000_000.0))
}

func (m Micro) Float64() float64 {
	return float64(m) / 1_000_000.0
}
