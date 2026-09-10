package cost_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/nexssp/cost"
)

func TestCurrencyString(t *testing.T) {
	if cost.USD.String() != "USD" {
		t.Fatalf("expected USD, got %s", cost.USD.String())
	}

	if cost.EUR.String() != "EUR" {
		t.Fatalf("expected EUR, got %s", cost.EUR.String())
	}
}

func TestCurrencyTextMarshaling(t *testing.T) {
	b, err := cost.USD.MarshalText()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	if string(b) != "USD" {
		t.Fatalf("expected USD, got %s", string(b))
	}

	var empty cost.Currency
	if _, err := empty.MarshalText(); err == nil {
		t.Fatal("expected error marshaling empty currency")
	}

	var parsed cost.Currency
	if err := parsed.UnmarshalText([]byte("PLN")); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if parsed != cost.PLN {
		t.Fatalf("expected PLN, got %s", parsed)
	}

	// Invalid length
	if err := parsed.UnmarshalText([]byte("US")); err == nil {
		t.Fatal("expected error on 2-byte code")
	}

	if err := parsed.UnmarshalText([]byte("USDD")); err == nil {
		t.Fatal("expected error on 4-byte code")
	}

	// Lowercase / non-uppercase ASCII
	if err := parsed.UnmarshalText([]byte("usd")); err == nil {
		t.Fatal("expected error on lowercase currency code")
	}

	if err := parsed.UnmarshalText([]byte("U1D")); err == nil {
		t.Fatal("expected error on non-letter character")
	}
}

func TestCurrencyJSONMarshaling(t *testing.T) {
	data, err := json.Marshal(cost.TOK)
	if err != nil {
		t.Fatalf("unexpected json marshal error: %v", err)
	}

	if string(data) != `"TOK"` {
		t.Fatalf("expected \"TOK\", got %s", string(data))
	}

	var c cost.Currency
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("unexpected json unmarshal error: %v", err)
	}

	if c != cost.TOK {
		t.Fatalf("expected TOK, got %s", c)
	}
}

func TestCurrencyNilPointerSafety(t *testing.T) {
	var c *cost.Currency
	if err := c.UnmarshalText([]byte("USD")); err == nil {
		t.Fatal("expected error on nil pointer unmarshal text")
	}

	if err := c.UnmarshalJSON([]byte(`"USD"`)); err == nil {
		t.Fatal("expected error on nil pointer unmarshal json")
	}
}

func TestMicroToMicro(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected cost.Micro
	}{
		{"Zero", 0.0, 0},
		{"OneDollar", 1.0, 1_000_000},
		{"FractionalCent", 0.000001, 1},
		{"RoundingUp", 0.0000016, 2},
		{"RoundingDown", 0.0000014, 1},
		{"NegativeAmount", -2.5, -2_500_000},
		{"NaN", math.NaN(), 0},
		{"InfPositive", math.Inf(1), 0},
		{"InfNegative", math.Inf(-1), 0},
		{"OverflowSaturation", 1e16, cost.Micro(math.MaxInt64)},
		{"UnderflowSaturation", -1e16, cost.Micro(math.MinInt64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := cost.ToMicro(tt.input)
			if res != tt.expected {
				t.Fatalf("ToMicro(%f) = %d; expected %d", tt.input, res, tt.expected)
			}
		})
	}
}

func TestMicroFloat64(t *testing.T) {
	m := cost.Micro(1_500_000)
	if m.Float64() != 1.5 {
		t.Fatalf("expected 1.5, got %f", m.Float64())
	}
}
