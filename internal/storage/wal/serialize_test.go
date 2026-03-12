package wal

import "testing"

func TestCanonicalJSONStableAcrossMapOrder(t *testing.T) {
	leftPayload := map[string]any{
		"domain": "accounts",
		"data": map[string]any{
			"z": 9,
			"a": 1,
		},
		"tx_id": "tx-001",
	}

	rightPayload := map[string]any{
		"tx_id": "tx-001",
		"data": map[string]any{
			"a": 1,
			"z": 9,
		},
		"domain": "accounts",
	}

	leftBytes, err := CanonicalJSON(leftPayload)
	if err != nil {
		t.Fatalf("canonical json left payload: %v", err)
	}

	rightBytes, err := CanonicalJSON(rightPayload)
	if err != nil {
		t.Fatalf("canonical json right payload: %v", err)
	}

	if string(leftBytes) != string(rightBytes) {
		t.Fatalf("expected equal canonical output, got left=%s right=%s", string(leftBytes), string(rightBytes))
	}
}

func TestCanonicalJSONReturnsErrorForUnsupportedValue(t *testing.T) {
	payload := map[string]any{
		"bad": func() {},
	}

	_, err := CanonicalJSON(payload)
	if err == nil {
		t.Fatal("expected error for unsupported payload value")
	}
}
