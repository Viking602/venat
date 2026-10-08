package durable

import (
	"bytes"
	"strings"
	"testing"
)

func TestAttemptDecoders_RejectExcessivePayloadBeforeMaterialization(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{"bytes", bytes.Repeat([]byte(" "), maxAttemptPayloadBytes+1)},
		{"depth", []byte(`{"x":` + strings.Repeat("[", 128) + `0` + strings.Repeat("]", 128) + `}`)},
		{"events", []byte(`{"version":1,"events":[` + strings.Repeat("{},", maxAttemptEvents) + `{}]}`)},
		{"members", []byte(`{"version":1,"result":{"structured":[` + strings.Repeat("0,", maxAttemptEvents) + `0]}}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if events, _, err := decodeModelAttempt(test.payload); err == nil || events != nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("model decoder=%d events error=%v", len(events), err)
			}
			if _, _, err := decodeToolAttempt(test.payload); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("tool decoder error=%v", err)
			}
		})
	}
}
