package scripted

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

type ScriptedProvider struct {
	metadata provider.Metadata
	events   []provider.Event
}

func New(events []provider.Event) *ScriptedProvider {
	return &ScriptedProvider{
		metadata: provider.Metadata{Name: "scripted", Models: []string{"scripted"}},
		events:   cloneEvents(events),
	}
}

func (p *ScriptedProvider) Metadata() provider.Metadata {
	return p.metadata
}

func (p *ScriptedProvider) Stream(_ context.Context, _ provider.Request) (provider.Stream, error) {
	return provider.NewSliceStream(cloneEvents(p.events)), nil
}

func LoadScript(path string) ([]provider.Event, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read script: %w", err)
	}
	var raw []scriptEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode script: %w", err)
	}
	events := make([]provider.Event, 0, len(raw))
	for _, item := range raw {
		event, err := item.toEvent()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

type scriptEvent struct {
	Kind       provider.EventKind  `json:"kind"`
	Text       string              `json:"text,omitempty"`
	StopReason provider.StopReason `json:"stopReason,omitempty"`
	Usage      provider.Usage      `json:"usage"`
	ToolCall   *scriptToolCall     `json:"toolCall,omitempty"`
}

type scriptToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func (s scriptEvent) toEvent() (provider.Event, error) {
	switch s.Kind {
	case provider.EventTextDelta:
		return provider.Event{Kind: s.Kind, Text: s.Text, Usage: s.Usage}, nil
	case provider.EventToolCall:
		if s.ToolCall == nil {
			return provider.Event{}, fmt.Errorf("script tool_call event requires toolCall")
		}
		return provider.Event{
			Kind: s.Kind,
			ToolCall: &message.ToolCall{
				ID:        s.ToolCall.ID,
				Name:      s.ToolCall.Name,
				Arguments: s.ToolCall.Arguments,
			},
			Usage: s.Usage,
		}, nil
	case provider.EventDone:
		return provider.Event{Kind: s.Kind, StopReason: s.StopReason, Usage: s.Usage}, nil
	default:
		return provider.Event{}, fmt.Errorf("unsupported scripted event kind %q", s.Kind)
	}
}

func cloneEvents(src []provider.Event) []provider.Event {
	cloned := make([]provider.Event, 0, len(src))
	for _, event := range src {
		current := event
		if event.ToolCall != nil {
			toolCall := *event.ToolCall
			if event.ToolCall.Arguments != nil {
				toolCall.Arguments = append(json.RawMessage(nil), event.ToolCall.Arguments...)
			}
			current.ToolCall = &toolCall
		}
		if event.ToolCallDelta != nil {
			delta := *event.ToolCallDelta
			if event.ToolCallDelta.Index != nil {
				index := *event.ToolCallDelta.Index
				delta.Index = &index
			}
			current.ToolCallDelta = &delta
		}
		if event.ProviderState != nil {
			current.ProviderState = append(json.RawMessage(nil), event.ProviderState...)
		}
		current.Response = message.CloneResponseMetadata(event.Response)
		cloned = append(cloned, current)
	}
	return cloned
}
