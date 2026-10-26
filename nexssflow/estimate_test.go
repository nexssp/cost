package nexssflow

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestParseEstimateSpec(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantAmount int64
		wantPath   string
		wantErr    bool
	}{
		{name: "literal", raw: "1200", wantAmount: 1200},
		{name: "leading dot", raw: ".policy.estimate_micros", wantPath: "policy.estimate_micros"},
		{name: "root path", raw: "$.estimate_micros", wantPath: "estimate_micros"},
		{name: "plain path", raw: "cost_micros", wantPath: "cost_micros"},
		{name: "negative literal", raw: "-1", wantErr: true},
		{name: "invalid expression", raw: "estimate * 2", wantErr: true},
		{name: "invalid path segment", raw: ".policy..estimate", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseEstimateSpec(test.raw)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseEstimateSpec(%q) error = %v, wantErr %v", test.raw, err, test.wantErr)
			}
			if err != nil {
				return
			}
			if got.literal != test.wantAmount {
				t.Errorf("literal = %d, want %d", got.literal, test.wantAmount)
			}
			if test.wantPath != "" && got.path == nil {
				t.Fatal("expected a dynamic path")
			}
			if test.wantPath == "" && got.dynamic() {
				t.Fatalf("literal unexpectedly parsed as dynamic path %v", got.path)
			}
			if got.dynamic() {
				joined := strings.Join(got.path, ".")
				if joined != test.wantPath {
					t.Errorf("path = %q, want %q", joined, test.wantPath)
				}
			}
		})
	}
}

func TestEstimateSpecResolvesRuntimeNumbersFromNestedObjects(t *testing.T) {
	spec, err := parseEstimateSpec(".value.estimate_micros")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []any{
		map[string]any{"value": map[string]any{"estimate_micros": int64(2_345)}},
		map[string]any{"value": map[string]any{"estimate_micros": float64(2_345)}},
		map[string]any{"value": map[string]any{"estimate_micros": json.Number("2345")}},
		struct {
			Value struct {
				Estimate int64 `json:"estimate_micros"`
			} `json:"value"`
		}{Value: struct {
			Estimate int64 `json:"estimate_micros"`
		}{Estimate: 2_345}},
	} {
		got, err := spec.resolve(input)
		if err != nil || got != 2_345 {
			t.Errorf("resolve(%T) = %d, %v; want 2345", input, got, err)
		}
	}
}

func TestEstimateSpecRejectsMissingOrInvalidRuntimeValues(t *testing.T) {
	spec, err := parseEstimateSpec(".value.estimate_micros")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []any{
		map[string]any{},
		map[string]any{"value": map[string]any{"estimate_micros": "2345"}},
		map[string]any{"value": map[string]any{"estimate_micros": -1}},
		map[string]any{"value": map[string]any{"estimate_micros": 2.5}},
		map[string]any{"value": map[string]any{"estimate_micros": math.NaN()}},
		map[string]any{"value": map[string]any{"estimate_micros": math.MaxFloat64}},
	} {
		if _, err := spec.resolve(input); err == nil {
			t.Errorf("resolve(%#v) unexpectedly succeeded", input)
		}
	}
}
