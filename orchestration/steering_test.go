package orchestration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/provider"
)

type steeringWorkerProvider struct {
	started chan struct{}
	calls   int
}

func (*steeringWorkerProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "worker-steering"}
}
func (model *steeringWorkerProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	model.calls++
	if model.calls == 1 {
		close(model.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	found := false
	for _, current := range request.Messages {
		if current.Text == "Inspect only; do not edit." {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("worker did not receive direction correction")
	}
	return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "inspection complete"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
}

func TestRuntimeSteerInterruptsSamplingWithoutLosingWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	model := &steeringWorkerProvider{started: make(chan struct{})}
	runtime, err := NewRuntime(ctx, RuntimeOptions{Factory: func(TaskRequest) (agent.Engine, error) { return agent.Engine{Provider: model}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "worker", Request: agent.Request{Prompt: "Investigate the repository."}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-model.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := handle.Steer(ctx, agent.Request{Prompt: "Inspect only; do not edit."}); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Await(ctx)
	if err != nil || result.Result.Failure != nil || result.Result.Text != "inspection complete" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if model.calls != 2 {
		t.Fatalf("model calls=%d, want interrupted request plus redirected request", model.calls)
	}
}
