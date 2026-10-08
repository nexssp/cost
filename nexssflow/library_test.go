package nexssflow_test

import (
	"testing"

	"github.com/nexssp/cost/nexssflow"
	"github.com/nexssp/flow/core"
)

func TestBundle_Contract(t *testing.T) {
	if _, ok := core.Lookup(nexssflow.ID); !ok {
		t.Fatalf("init() did not register bundle under %q", nexssflow.ID)
	}

	bundle := nexssflow.Bundle(nil)

	if bundle.ID != nexssflow.ID {
		t.Fatalf("ID = %q, want %q", bundle.ID, nexssflow.ID)
	}
	if len(bundle.Libraries) != 1 {
		t.Fatalf("Libraries = %d, want 1", len(bundle.Libraries))
	}
	if len(bundle.Modifiers) != 1 {
		t.Fatalf("Modifiers = %d, want 1", len(bundle.Modifiers))
	}
	if bundle.Modifiers[0].Name != "cost_estimate" {
		t.Fatalf("Modifiers[0].Name = %q, want cost_estimate", bundle.Modifiers[0].Name)
	}
	if bundle.Modifiers[0].ValueKind != core.ModifierKindInt64 {
		t.Fatalf("Modifiers[0].ValueKind = %v, want ModifierKindInt64", bundle.Modifiers[0].ValueKind)
	}
	if bundle.AtomAdvise != nil {
		t.Fatal("AtomAdvise must be nil; cost_estimate is a registered modifier")
	}
}

func TestBundle_RejectsUnknownOption(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unknown @require option")
		}
	}()
	nexssflow.Bundle(map[string]string{"unknown_option": "x"})
}

func TestBundle_RejectsInvalidCurrency(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on invalid currency code")
		}
	}()
	nexssflow.Bundle(map[string]string{"currency": "US"})
}
