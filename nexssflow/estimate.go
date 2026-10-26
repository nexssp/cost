package nexssflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// estimateSpec is immutable after compilation. A literal is still parsed once
// at build time; a path is resolved from each action request at runtime.
type estimateSpec struct {
	literal int64
	path    []string
}

func (s estimateSpec) dynamic() bool { return len(s.path) != 0 }

func parseEstimateSpec(raw string) (estimateSpec, error) {
	raw = strings.TrimSpace(raw)
	amount, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		if amount < 0 {
			return estimateSpec{}, errors.New("estimate must be non-negative")
		}
		return estimateSpec{literal: amount}, nil
	}

	path := strings.TrimPrefix(raw, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return estimateSpec{}, errors.New("expected a non-negative integer or an input field path such as .cost_micros")
	}
	parts := strings.Split(path, ".")
	for _, part := range parts {
		if !isIdentifier(part) {
			return estimateSpec{}, fmt.Errorf("expected a non-negative integer or an input field path such as .cost_micros, got %q", raw)
		}
	}
	return estimateSpec{path: parts}, nil
}

func (s estimateSpec) resolve(input any) (int64, error) {
	if !s.dynamic() {
		return s.literal, nil
	}
	value := input
	for _, field := range s.path {
		next, ok := estimateField(value, field)
		if !ok {
			return 0, fmt.Errorf("cost: estimate input is missing field path %q", "."+strings.Join(s.path, "."))
		}
		value = next
	}
	amount, ok := integerMicros(value)
	if !ok {
		return 0, fmt.Errorf("cost: estimate field %q must contain a non-negative integer number of micros, got %T", "."+strings.Join(s.path, "."), value)
	}
	if amount < 0 || amount > math.MaxInt64/2 {
		return 0, fmt.Errorf("cost: estimate field %q is outside the supported range [0,%d]", "."+strings.Join(s.path, "."), int64(math.MaxInt64/2))
	}
	return amount, nil
}

func estimateField(value any, name string) (any, bool) {
	if value == nil {
		return nil, false
	}
	v := reflect.ValueOf(value)
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil, false
	}
	//nolint:exhaustive // Only maps and structs can provide named input fields.
	switch v.Kind() {
	case reflect.Map:
		return estimateMapField(v, name)
	case reflect.Struct:
		return estimateStructField(v, name)
	default:
		return nil, false
	}
}

func estimateMapField(v reflect.Value, name string) (any, bool) {
	if v.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	item := v.MapIndex(reflect.ValueOf(name).Convert(v.Type().Key()))
	if !item.IsValid() {
		return nil, false
	}
	return item.Interface(), true
}

func estimateStructField(v reflect.Value, name string) (any, bool) {
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if jsonName == "-" {
			continue
		}
		if jsonName == "" {
			jsonName = field.Name
		}
		if jsonName == name {
			item := v.Field(i)
			if item.CanInterface() {
				return item.Interface(), true
			}
			return nil, false
		}
	}
	return nil, false
}

func integerMicros(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	if number, ok := value.(json.Number); ok {
		parsed, err := number.Int64()
		return parsed, err == nil
	}
	v := reflect.ValueOf(value)
	//nolint:exhaustive // Only integer and float reflection kinds represent an estimate.
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n := v.Uint()
		if n > uint64(math.MaxInt64) {
			return 0, false
		}
		return int64(n), true
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		if math.IsNaN(n) || math.IsInf(n, 0) || math.Trunc(n) != n || n < 0 || n > float64(math.MaxInt64/2) {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}
