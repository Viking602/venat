package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/orchestration"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

const untrustedResultMarker = "[UNTRUSTED WORKER RESULT"

type workerProvider struct {
	fastDone         chan struct{}
	fastOnce         sync.Once
	followupObserved chan []message.Message
	followupRelease  chan struct{}
}

func (p *workerProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "async-worker"} }
func (p *workerProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	if len(request.Messages) == 0 {
		return nil, fmt.Errorf("worker received no prompt")
	}
	last := request.Messages[len(request.Messages)-1].Text
	if strings.HasPrefix(last, "follow-up") {
		copyMessages := message.CloneMessages(request.Messages)
		p.followupObserved <- copyMessages
		select {
		case <-p.followupRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, ctx.Err()
	}
	if strings.Contains(last, "fast") {
		p.fastOnce.Do(func() { close(p.fastDone) })
	}
	if strings.Contains(last, "slow") {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: "fast worker result", TextPhase: provider.TextPhaseFinalAnswer},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}), nil
}

type parentProvider struct {
	mu      sync.Mutex
	calls   int
	runtime *orchestration.Runtime
}

func (p *parentProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "async-parent"} }

func (p *parentProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	p.mu.Lock()
	call := p.calls
	p.calls++
	p.mu.Unlock()
	switch call {
	case 0:
		return parentToolCall("spawn-fast", "spawn", `{"id":"fast","prompt":"fast"}`), nil
	case 1:
		return parentToolCall("spawn-slow", "spawn", `{"id":"slow","prompt":"slow"}`), nil
	case 2:
		return parentToolCall("parent-action", "parent_action", `{}`), nil
	case 3:
		snapshot, err := p.runtime.Inspect("slow")
		if err != nil || (snapshot.Status != orchestration.TaskQueued && snapshot.Status != orchestration.TaskRunning) {
			return nil, fmt.Errorf("slow worker was not active when fast evidence was used: status=%v err=%v", snapshot.Status, err)
		}
		if !containsEvidence(request.Messages) {
			return nil, fmt.Errorf("parent did not receive fast worker evidence")
		}
		return parentToolCall("send-fast", "send", `{"id":"fast","prompt":"follow-up: keep your context"}`), nil
	case 4:
		return parentToolCall("cancel-slow", "cancel", `{"id":"slow"}`), nil
	default:
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: "parent used fast evidence while slow was still active", TextPhase: provider.TextPhaseFinalAnswer},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}), nil
	}
}

func parentToolCall(id, name, arguments string) provider.Stream {
	var raw json.RawMessage = []byte(arguments)
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: id, Name: name, Arguments: raw}},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	})
}

func containsEvidence(messages []message.Message) bool {
	for _, current := range messages {
		if strings.Contains(current.Text, untrustedResultMarker) {
			return true
		}
	}
	return false
}

type parentActionTool struct{ runtime *orchestration.Runtime }

func (parentActionTool) Definition() tool.Definition {
	return tool.Definition{Name: "parent_action", Description: "Perform the parent's independent action.", InputSchema: message.JSONSchema{Type: "object"}}
}

func (action parentActionTool) Execute(ctx context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	if _, err := action.runtime.Await(ctx, "fast"); err != nil {
		return tool.Result{}, err
	}
	return tool.Result{ToolCallID: call.ID, Name: call.Name, Content: "parent action complete"}, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	worker := &workerProvider{fastDone: make(chan struct{}), followupObserved: make(chan []message.Message, 1), followupRelease: make(chan struct{})}
	runtime, err := orchestration.NewRuntime(ctx, orchestration.RuntimeOptions{
		MaxConcurrency: 2,
		QueueSize:      4,
		Factory: func(orchestration.TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: worker, Model: "credential-free-worker"}, nil
		},
	})
	if err != nil {
		panic(err)
	}

	parentControl := &agent.Control{}
	tools := runtime.Tools()
	tools = append(tools, parentActionTool{runtime: runtime})
	parent := agent.Engine{
		Provider: (&parentProvider{runtime: runtime}),
		Tools:    tool.NewBus(tools...),
		Model:    "credential-free-parent",
		Control:  parentControl,
		Hooks:    agent.NewHookChain(runtime.CompletionHook(parentControl)),
	}
	result := parent.Run(ctx, agent.Request{Prompt: "Spawn workers, continue your own action, then use their evidence."}, agent.OutputPolicy{})
	if result.Failure != nil {
		panic(result.Failure)
	}
	fastEvidenceCount := 0
	for _, current := range result.Messages {
		if strings.Contains(current.Text, untrustedResultMarker) && strings.Contains(current.Text, `"id":"fast"`) && strings.Contains(current.Text, `"turn":1`) {
			fastEvidenceCount++
		}
	}
	if fastEvidenceCount != 1 {
		panic(fmt.Sprintf("persistent fast result count = %d", fastEvidenceCount))
	}
	fmt.Printf("parent result: %q; fast evidence persisted exactly once\n", result.Text)
	followupHistory := <-worker.followupObserved
	if len(followupHistory) < 3 {
		panic(fmt.Sprintf("follow-up lost worker context: %d messages", len(followupHistory)))
	}
	fmt.Println("follow-up preserved child context")
	if err := runtime.Close(); err != nil {
		panic(err)
	}
	for _, id := range []string{"fast", "slow"} {
		snapshot, inspectErr := runtime.Inspect(id)
		if inspectErr != nil || snapshot.Status == orchestration.TaskQueued || snapshot.Status == orchestration.TaskRunning {
			panic(fmt.Sprintf("active work remains for %s: status=%v err=%v", id, snapshot.Status, inspectErr))
		}
	}
	fmt.Println("runtime closed with no active work")
}
