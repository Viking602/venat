package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestEngine_OutputStoreRecoversOmittedEvidenceThroughModelToolCall(t *testing.T) {
	store, err := tool.NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), tool.WithPreviewThreshold(128))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lookup, err := kit.Tool("lookup", func(context.Context, struct{}) (string, error) {
		return strings.Repeat("a", 1024) + "needle to recover" + strings.Repeat("b", 1024), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	model := &artifactFlowProvider{}
	result := (Engine{Provider: model, Tools: tool.NewBus(lookup), OutputStore: store}).Run(
		context.Background(), Request{Prompt: "Find the evidence in the report."}, OutputPolicy{},
	)
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if result.Text != "evidence recovered" || result.ToolCallsUsed != 2 {
		t.Fatalf("result=%+v", result)
	}
}

type artifactFlowProvider struct{ calls int }

func (*artifactFlowProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "artifact-flow"}
}

func (driver *artifactFlowProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	driver.calls++
	call := message.ToolCall{ID: "lookup-report", Name: "lookup", Arguments: []byte(`{}`)}
	switch driver.calls {
	case 1:
	case 2:
		last := request.Messages[len(request.Messages)-1]
		if last.ToolResult == nil || last.ToolResult.IsError {
			return nil, fmt.Errorf("report lookup failed")
		}
		text := last.ToolResult.TextContent()
		start := strings.Index(text, "artifact://")
		if start < 0 || strings.Contains(text, "needle to recover") {
			return nil, fmt.Errorf("report was not externalized")
		}
		ref := text[start : start+len("artifact://")+64]
		call = message.ToolCall{ID: "read-evidence", Name: "tool_output_read", Arguments: []byte(fmt.Sprintf(`{"reference":%q,"offset":1024,"limit":32}`, ref))}
	case 3:
		last := request.Messages[len(request.Messages)-1]
		if last.ToolResult == nil || last.ToolResult.IsError || !strings.Contains(last.ToolResult.TextContent(), "needle to recover") {
			return nil, fmt.Errorf("stored middle evidence was not retrieved")
		}
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "evidence recovered"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	default:
		return nil, fmt.Errorf("unexpected model turn")
	}
	return provider.NewSliceStream([]provider.Event{{Kind: provider.EventToolCall, ToolCall: &call}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}}), nil
}

type overflowProvider struct{ calls int }

func (p *overflowProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "overflow"} }
func (p *overflowProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	p.calls++
	if p.calls == 1 {
		return nil, &provider.Error{Provider: "overflow", Kind: provider.ErrorContextLength, Code: "context_length_exceeded"}
	}
	return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "recovered"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
}

func TestRunMessages_ContextOverflowCompactsAndRetriesWithoutReplayingEffects(t *testing.T) {
	model := &overflowProvider{}
	compactions := 0
	out, err := (Engine{Provider: model}).RunMessages(context.Background(), LoopInput{
		Messages: []message.Message{message.NewText(message.RoleUser, "large")},
		Compact: func(_ context.Context, history []message.Message) ([]message.Message, error) {
			compactions++
			return append(history, message.NewText(message.RoleSystem, "summary")), nil
		},
	})
	if err != nil || out.Messages[len(out.Messages)-1].Text != "recovered" || model.calls != 2 || compactions != 1 {
		t.Fatalf("out=%+v err=%v calls=%d compactions=%d", out, err, model.calls, compactions)
	}
}
