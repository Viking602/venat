package agent

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDecodeContinuation_RejectsResourceExhaustionBeforeSemanticDecode(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"bytes", bytes.Repeat([]byte(" "), maxContinuationBytes+1)},
		{"depth", []byte(`{"x":` + strings.Repeat("[", maxContinuationDepth) + `0` + strings.Repeat("]", maxContinuationDepth) + `}`)},
		{"collection", []byte(`{"x":[` + strings.Repeat("0,", maxContinuationCollection) + `0]}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeContinuation(test.data)
			if !errors.Is(err, ErrInvalidContinuation) || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("DecodeContinuation() = %v", err)
			}
		})
	}
}
