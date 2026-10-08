package shared

import (
	"encoding/json"
	"errors"

	"github.com/Viking602/venat/message"
)

const (
	MaxStreamFrames = 65536
	MaxStreamBytes  = 64 << 20
	MaxStreamItems  = 1024
)

// StreamBudget counts wire frames, including frames that emit no SDK event.
// Its zero value is ready for one response stream.
type StreamBudget struct {
	frames int
	bytes  int
}

func (budget *StreamBudget) Observe(frame Event) error {
	budget.frames++
	size := len(frame.Data) + len(frame.Name) + len(frame.ID) + len(frame.Comment)
	if budget.frames > MaxStreamFrames || size > MaxStreamBytes-budget.bytes {
		return errors.New("provider stream exceeds frame or byte limit")
	}
	budget.bytes += size
	return nil
}

// DecodeStreamJSON bounds arrays, object members, nesting and total values
// before json.Unmarshal can allocate a provider-specific object graph.
func DecodeStreamJSON(data []byte, target any) error {
	if err := message.CheckJSON(data, message.JSONLimits{
		Bytes: MaxSSEFrameBytes, Depth: 128, Values: 65536, Collection: MaxStreamItems,
	}); err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
