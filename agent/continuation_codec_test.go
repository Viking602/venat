package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
)

func TestContinuationCodec_CanonicalRoundTrip(t *testing.T) {
	left := codecReadyContinuation()
	left.OutputPolicy.Schema = json.RawMessage(` { "z": 1, "a": { "y": 2, "x": 1 } } `)
	left.Messages[0].Metadata = map[string]string{"z": "last", "a": "first"}
	left.Messages[0].ProviderState = json.RawMessage(` { "z": true, "a": [2, 1] } `)

	right := codecReadyContinuation()
	right.OutputPolicy.Schema = json.RawMessage(`{"a":{"x":1,"y":2},"z":1}`)
	right.Messages[0].Metadata = map[string]string{"a": "first", "z": "last"}
	right.Messages[0].ProviderState = json.RawMessage(`{"a":[2,1],"z":true}`)

	leftEncoded, err := EncodeContinuation(left)
	if err != nil {
		t.Fatalf("EncodeContinuation(left) error = %v", err)
	}
	rightEncoded, err := EncodeContinuation(right)
	if err != nil {
		t.Fatalf("EncodeContinuation(right) error = %v", err)
	}
	if !bytes.Equal(leftEncoded, rightEncoded) {
		t.Fatalf("canonical encodings differ:\nleft  = %s\nright = %s", leftEncoded, rightEncoded)
	}

	decoded, err := DecodeContinuation(leftEncoded)
	if err != nil {
		t.Fatalf("DecodeContinuation() error = %v", err)
	}
	reencoded, err := EncodeContinuation(decoded)
	if err != nil {
		t.Fatalf("EncodeContinuation(decoded) error = %v", err)
	}
	if !bytes.Equal(reencoded, leftEncoded) {
		t.Fatalf("re-encoding changed bytes:\nfirst  = %s\nsecond = %s", leftEncoded, reencoded)
	}

	marshaled, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !bytes.Equal(marshaled, leftEncoded) {
		t.Fatalf("MarshalJSON bytes = %s, want %s", marshaled, leftEncoded)
	}
	var unmarshaled Continuation
	if err := json.Unmarshal(marshaled, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(unmarshaled, decoded) {
		t.Fatalf("JSON round trip = %#v, want %#v", unmarshaled, decoded)
	}

	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(leftEncoded, &topLevel); err != nil {
		t.Fatalf("decode top-level fields: %v", err)
	}
	for _, name := range continuationV1Fields {
		if _, ok := topLevel[name]; !ok {
			t.Errorf("encoded continuation omitted %q", name)
		}
	}
	if len(topLevel) != len(continuationV1Fields) {
		t.Fatalf("top-level field count = %d, want %d", len(topLevel), len(continuationV1Fields))
	}
}

func TestContinuationCodec_PreservesExecutionEnvelope(t *testing.T) {
	continuation := codecReadyContinuation()
	continuation.Request.SessionBudget = &SessionBudget{MaxTokens: 1000, MaxWallClock: time.Minute}
	continuation.Request.ModelTimeouts = &ModelTimeoutPolicy{
		ConnectTimeout:    time.Second,
		RequestTimeout:    2 * time.Second,
		StreamIdleTimeout: 3 * time.Second,
		DisableDefaults:   true,
	}

	encoded, err := EncodeContinuation(continuation)
	if err != nil {
		t.Fatalf("EncodeContinuation() error = %v", err)
	}
	decoded, err := DecodeContinuation(encoded)
	if err != nil {
		t.Fatalf("DecodeContinuation() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Request.SessionBudget, continuation.Request.SessionBudget) || !reflect.DeepEqual(decoded.Request.ModelTimeouts, continuation.Request.ModelTimeouts) {
		t.Fatalf("execution envelope = %#v/%#v, want %#v/%#v", decoded.Request.SessionBudget, decoded.Request.ModelTimeouts, continuation.Request.SessionBudget, continuation.Request.ModelTimeouts)
	}
}

func TestContinuationCodec_RejectsOpenOrAmbiguousDocuments(t *testing.T) {
	valid, err := EncodeContinuation(codecReadyContinuation())
	if err != nil {
		t.Fatalf("EncodeContinuation() error = %v", err)
	}

	for _, missing := range continuationV1Fields {
		t.Run("missing_"+missing, func(t *testing.T) {
			candidate := mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
				delete(fields, missing)
			})
			assertInvalidContinuationJSON(t, candidate)
		})
	}

	tests := map[string][]byte{
		"unknown top-level field": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["unknown"] = json.RawMessage(`true`)
		}),
		"unknown request field": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["request"] = json.RawMessage(`{"prompt":"hi","unknown":true}`)
		}),
		"unknown message field": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["messages"] = json.RawMessage(`[{"role":"user","text":"hi","unknown":true}]`)
		}),
		"zero schema version": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["schemaVersion"] = json.RawMessage(`0`)
		}),
		"future schema version": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["schemaVersion"] = json.RawMessage(`3`)
		}),
		"string schema version": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["schemaVersion"] = json.RawMessage(`"1"`)
		}),
		"null scalar": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["toolCallsUsed"] = json.RawMessage(`null`)
		}),
		"trailing object":     append(append([]byte(nil), valid...), []byte(` {}`)...),
		"top-level array":     []byte(`[]`),
		"top-level null":      []byte(`null`),
		"duplicate top field": append([]byte(`{"schemaVersion":1,`), valid[1:]...),
		"duplicate nested field": mutateContinuationJSON(t, valid, func(fields map[string]json.RawMessage) {
			fields["request"] = json.RawMessage(`{"prompt":"hi","prompt":"again"}`)
		}),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			assertInvalidContinuationJSON(t, candidate)
		})
	}
}

func TestContinuationCodec_RejectsInvalidValueOnEncode(t *testing.T) {
	continuation := codecReadyContinuation()
	continuation.SchemaVersion = 0
	if _, err := EncodeContinuation(continuation); !errors.Is(err, ErrInvalidContinuation) {
		t.Fatalf("EncodeContinuation() error = %v, want ErrInvalidContinuation", err)
	}
	if _, err := json.Marshal(continuation); !errors.Is(err, ErrInvalidContinuation) {
		t.Fatalf("json.Marshal() error = %v, want ErrInvalidContinuation", err)
	}
}

func codecReadyContinuation() Continuation {
	return Continuation{
		SchemaVersion: ContinuationSchemaVersion,
		Request:       Request{Prompt: "hi"},
		Messages:      []message.Message{message.NewText(message.RoleUser, "hi")},
		Phase:         ContinuationReady,
	}
}

func mutateContinuationJSON(t *testing.T, encoded []byte, mutate func(map[string]json.RawMessage)) []byte {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode continuation fixture: %v", err)
	}
	mutate(fields)
	candidate, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode continuation fixture: %v", err)
	}
	return candidate
}

func assertInvalidContinuationJSON(t *testing.T, candidate []byte) {
	t.Helper()
	if _, err := DecodeContinuation(candidate); !errors.Is(err, ErrInvalidContinuation) {
		t.Fatalf("DecodeContinuation(%s) error = %v, want ErrInvalidContinuation", candidate, err)
	}
	var continuation Continuation
	if err := json.Unmarshal(candidate, &continuation); err == nil {
		t.Fatalf("json.Unmarshal(%s) unexpectedly succeeded", candidate)
	}
}
