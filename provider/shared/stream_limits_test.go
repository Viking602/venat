package shared

import (
	"strings"
	"testing"
)

func TestStreamBudget_CountsSilentFramesAndBytes(t *testing.T) {
	budget := StreamBudget{frames: MaxStreamFrames - 1, bytes: MaxStreamBytes - 1}
	if err := budget.Observe(Event{Comment: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := budget.Observe(Event{}); err == nil {
		t.Fatal("silent frame bypassed budget")
	}
	budget = StreamBudget{bytes: MaxStreamBytes - 1}
	if err := budget.Observe(Event{ID: "xx"}); err == nil {
		t.Fatal("metadata bypassed byte budget")
	}
}

func TestDecodeStreamJSON_RejectsDenseArraysBeforeUnmarshal(t *testing.T) {
	var target struct {
		Items []struct{} `json:"items"`
	}
	data := []byte(`{"items":[` + strings.Repeat("{},", MaxStreamItems) + `{}]}`)
	if err := DecodeStreamJSON(data, &target); err == nil || target.Items != nil {
		t.Fatalf("decoded dense array before rejecting it: error=%v items=%d", err, len(target.Items))
	}
}

func TestReader_ChargesIgnoredWireFields(t *testing.T) {
	reader := NewReader(strings.NewReader("unknown: xxxxxxxxxxxxxxxxxx\ndata: {}\n\n"))
	reader.bytes = MaxStreamBytes - 10
	if _, err := reader.Next(); err == nil || !strings.Contains(err.Error(), "wire bytes") {
		t.Fatalf("ignored fields bypassed wire byte budget: %v", err)
	}
}
