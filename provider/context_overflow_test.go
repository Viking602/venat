package provider

import "testing"

func TestHTTPContextOverflowClassification(t *testing.T) {
	cases := []struct {
		name     string
		provider string
		status   int
		body     string
		want     bool
	}{
		{"anthropic prompt", "anthropic", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 220000 tokens > 200000 maximum"}}`, true},
		{"openai context code", "openai", 400, `{"error":{"code":"context_length_exceeded","message":"context too large"}}`, true},
		{"media is not context", "anthropic", 400, `{"error":{"type":"invalid_request_error","message":"image exceeds size limit"}}`, false},
		{"arbitrary payload rejection", "anthropic", 413, `request body too large`, false},
		{"unstructured text", "anthropic", 400, `prompt is too long: 220000 tokens > 200000 maximum`, false},
		{"different error type", "anthropic", 401, `{"error":{"type":"authentication_error","message":"prompt is too long: quoted diagnostic"}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failure := NewHTTPError(tc.provider, tc.status, tc.body)
			if IsContextOverflow(failure) != tc.want {
				t.Fatalf("classification=%s, want context=%v", failure.Kind, tc.want)
			}
			if failure.Message != tc.body || failure.StatusCode != tc.status {
				t.Fatal("classification discarded original failure facts")
			}
		})
	}
}
