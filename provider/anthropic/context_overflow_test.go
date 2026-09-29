package anthropic

import (
	"testing"

	"github.com/Viking602/venat/provider"
)

func TestStreamErrorDistinguishesContextAndMediaLimits(t *testing.T) {
	if !provider.IsContextOverflow(anthropicError("invalid_request_error", "prompt is too long: 220000 tokens > 200000 maximum")) {
		t.Fatal("stream context rejection was not classified")
	}
	for _, failure := range []error{
		anthropicError("invalid_request_error", "image dimensions exceed limit"),
		anthropicError("overloaded_error", "prompt is too long: upstream quoted diagnostic"),
	} {
		if provider.IsContextOverflow(failure) {
			t.Fatalf("unrelated failure classified as context overflow: %v", failure)
		}
	}
}
