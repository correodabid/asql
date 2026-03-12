package wal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

// CanonicalJSON serializes payloads into deterministic JSON bytes.
func CanonicalJSON(payload any) ([]byte, error) {
	normalized, err := normalize(payload)
	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	return bytes, nil
}

func normalize(payload any) (any, error) {
	if payload == nil {
		return nil, nil
	}

	return normalizeValue(reflect.ValueOf(payload))
}

func normalizeValue(value reflect.Value) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}

	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Map:
		return normalizeMap(value)
	case reflect.Slice, reflect.Array:
		items := make([]any, value.Len())
		for index := range items {
			item, err := normalizeValue(value.Index(index))
			if err != nil {
				return nil, err
			}
			items[index] = item
		}
		return items, nil
	default:
		return value.Interface(), nil
	}
}

func normalizeMap(value reflect.Value) (map[string]any, error) {
	keys := value.MapKeys()
	ordered := make([]string, 0, len(keys))
	lookup := make(map[string]reflect.Value, len(keys))

	for _, key := range keys {
		stableKey := fmt.Sprint(key.Interface())
		ordered = append(ordered, stableKey)
		lookup[stableKey] = key
	}

	sort.Strings(ordered)

	normalized := make(map[string]any, len(keys))
	for _, stableKey := range ordered {
		normalizedValue, err := normalizeValue(value.MapIndex(lookup[stableKey]))
		if err != nil {
			return nil, err
		}
		normalized[stableKey] = normalizedValue
	}

	return normalized, nil
}
