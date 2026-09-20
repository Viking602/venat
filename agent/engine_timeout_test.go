package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

func TestEngineRunMaxWallClockPrecedenceReturnsBudgetFailure(t *testing.T) {
	tests := []struct {
		name          string
		requestBudget time.Duration
		engineBudget  time.Duration
		want          time.Duration
	}{
		{
			name:          "request budget overrides engine budget",
			requestBudget: 50 * time.Millisecond,
			engineBudget:  10 * time.Millisecond,
			want:          50 * time.Millisecond,
		},
		{
			name:         "engine budget applies without request budget",
			engineBudget: 40 * time.Millisecond,
			want:         40 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := deadlineRecordingProvider{observed: make(chan time.Duration, 1)}
			engine := Engine{
				Provider:   driver,
				LoopPolicy: LoopPolicy{Budget: &Budget{MaxWallClock: test.engineBudget}},
			}
			request := Request{Prompt: "wait for deadline"}
			if test.requestBudget > 0 {
				request.Budget = &Budget{MaxWallClock: test.requestBudget}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			started := time.Now()
			result := engine.Run(ctx, request, OutputPolicy{})
			elapsed := time.Since(started)
			if result.Failure == nil || result.Failure.Kind != FailureKindBudgetExhausted {
				t.Fatalf("Failure = %#v, want budget_exhausted", result.Failure)
			}
			if !errors.Is(result.Failure, context.DeadlineExceeded) {
				t.Fatalf("failure cause = %v, want context deadline exceeded", result.Failure)
			}
			observed := <-driver.observed
			assertDurationNear(t, observed, test.want)
			if elapsed > test.want+100*time.Millisecond {
				t.Fatalf("Engine.Run elapsed = %s, want deadline near %s", elapsed, test.want)
			}
		})
	}
}

func TestMaxWallClockBudgetPrecedence(t *testing.T) {
	engine := Engine{LoopPolicy: LoopPolicy{Budget: &Budget{MaxWallClock: 2 * time.Second}}}
	if got := engine.maxWallClock(Request{}); got != 2*time.Second {
		t.Fatalf("maxWallClock = %v, want engine default 2s", got)
	}
	if got := engine.maxWallClock(Request{Budget: &Budget{}}); got != 0 {
		t.Fatalf("maxWallClock = %v, want request zero to mean unbounded", got)
	}
	if got := engine.maxWallClock(Request{Budget: &Budget{MaxWallClock: time.Second}}); got != time.Second {
		t.Fatalf("maxWallClock = %v, want request override 1s", got)
	}
	engine.LoopPolicy.Budget = nil
	if got := engine.maxWallClock(Request{}); got != 0 {
		t.Fatalf("maxWallClock = %v, want unbounded without a budget", got)
	}
}

func TestSessionBudgetBoundsEngineExecution(t *testing.T) {
	driver := deadlineRecordingProvider{observed: make(chan time.Duration, 1)}
	engine := Engine{Provider: driver}
	request := Request{
		Prompt:        "session deadline",
		SessionBudget: &SessionBudget{MaxWallClock: 20 * time.Millisecond},
	}

	result := engine.Run(context.Background(), request, OutputPolicy{})
	if result.Failure == nil || result.Failure.Kind != FailureKindBudgetExhausted {
		t.Fatalf("Failure = %#v, want budget_exhausted", result.Failure)
	}
	if !errors.Is(result.Failure, context.DeadlineExceeded) {
		t.Fatalf("failure cause = %v, want context deadline exceeded", result.Failure)
	}
	assertDurationNear(t, <-driver.observed, request.SessionBudget.MaxWallClock)
}

func TestSessionBudgetOverridesEngineDefault(t *testing.T) {
	engine := Engine{LoopPolicy: LoopPolicy{SessionBudget: &SessionBudget{MaxWallClock: time.Second}}}
	if got := engine.maxWallClock(Request{SessionBudget: &SessionBudget{MaxWallClock: 30 * time.Millisecond}}); got != 30*time.Millisecond {
		t.Fatalf("maxWallClock = %s, want request session budget", got)
	}
	if got := engine.maxWallClock(Request{SessionBudget: &SessionBudget{}}); got != 0 {
		t.Fatalf("maxWallClock = %s, want request zero session budget to disable default", got)
	}
}

func TestEngineRunContextBuildHonorsMaxWallClock(t *testing.T) {
	tests := []struct {
		name          string
		requestBudget time.Duration
		engineBudget  time.Duration
		want          time.Duration
	}{
		{
			name:         "engine budget bounds context build",
			engineBudget: 20 * time.Millisecond,
			want:         20 * time.Millisecond,
		},
		{
			name:          "request budget bounds context build",
			requestBudget: 30 * time.Millisecond,
			engineBudget:  100 * time.Millisecond,
			want:          30 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := &blockingContextBuilder{observed: make(chan time.Duration, 1)}
			engine := Engine{
				ContextBuilder: builder,
				LoopPolicy:     LoopPolicy{Budget: &Budget{MaxWallClock: test.engineBudget}},
			}
			request := Request{Prompt: "build context"}
			if test.requestBudget > 0 {
				request.Budget = &Budget{MaxWallClock: test.requestBudget}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			started := time.Now()
			result := engine.Run(ctx, request, OutputPolicy{})
			elapsed := time.Since(started)
			if result.Failure == nil || result.Failure.Kind != FailureKindBudgetExhausted {
				t.Fatalf("Failure = %#v, want budget_exhausted", result.Failure)
			}
			if !errors.Is(result.Failure, context.DeadlineExceeded) {
				t.Fatalf("failure cause = %v, want context deadline exceeded", result.Failure)
			}
			observed := <-builder.observed
			assertDurationNear(t, observed, test.want)
			if elapsed > test.want+100*time.Millisecond {
				t.Fatalf("Engine.Run elapsed = %s, want context build deadline near %s", elapsed, test.want)
			}
		})
	}
}

func TestEngineRunMessagesHonorsToolDefinitionTimeout(t *testing.T) {
	driver := &blockingTimeoutTool{
		name:     "slow",
		timeout:  10 * time.Millisecond,
		observed: make(chan time.Duration, 1),
	}
	engine := Engine{
		Provider: &scriptedProvider{
			turns: [][]provider.Event{{
				{
					Kind: provider.EventToolCall,
					ToolCall: &message.ToolCall{
						ID:   "call-1",
						Name: "slow",
					},
				},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			}},
		},
		Tools: tool.NewBus(driver),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := engine.RunMessages(ctx, LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "call slow")},
		MaxIterations: 1,
	})
	elapsed := time.Since(started)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunMessages() error = %v, want context deadline exceeded", err)
	}
	observed := <-driver.observed
	assertDurationNear(t, observed, driver.timeout)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("RunMessages() elapsed = %s, want tool timeout near %s", elapsed, driver.timeout)
	}
}

func TestEngineRunToolDefinitionTimeoutWithActiveRunBudgetReturnsEngineError(t *testing.T) {
	driver := &blockingTimeoutTool{
		name:     "slow",
		timeout:  10 * time.Millisecond,
		observed: make(chan time.Duration, 1),
	}
	engine := Engine{
		Provider: &scriptedProvider{
			turns: [][]provider.Event{{
				{
					Kind: provider.EventToolCall,
					ToolCall: &message.ToolCall{
						ID:   "call-1",
						Name: "slow",
					},
				},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			}},
		},
		Tools: tool.NewBus(driver),
		LoopPolicy: LoopPolicy{
			Budget: &Budget{MaxWallClock: 250 * time.Millisecond},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	result := engine.Run(ctx, Request{Prompt: "call slow"}, OutputPolicy{})
	elapsed := time.Since(started)

	if result.Failure == nil {
		t.Fatal("expected tool timeout failure, got nil")
	}
	if result.Failure.Kind != FailureKindEngineError {
		t.Fatalf("failure kind = %s, want %s", result.Failure.Kind, FailureKindEngineError)
	}
	if !errors.Is(result.Failure, context.DeadlineExceeded) {
		t.Fatalf("failure cause = %v, want context deadline exceeded", result.Failure)
	}
	if ctx.Err() != nil {
		t.Fatalf("parent context error = %v, want nil", ctx.Err())
	}
	observed := <-driver.observed
	assertDurationNear(t, observed, driver.timeout)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("Engine.Run elapsed = %s, want tool timeout near %s", elapsed, driver.timeout)
	}
}

type deadlineRecordingProvider struct {
	observed chan time.Duration
}

type blockingContextBuilder struct {
	observed chan time.Duration
}

func (b *blockingContextBuilder) Build(ctx context.Context, _ Request) ([]message.Message, error) {
	recordObservedDeadline(ctx, b.observed)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingContextBuilder) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	return history, nil
}

func (deadlineRecordingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "deadline-recording"}
}

func (p deadlineRecordingProvider) Stream(ctx context.Context, _ provider.Request) (provider.Stream, error) {
	recordObservedDeadline(ctx, p.observed)
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingTimeoutTool struct {
	name     string
	timeout  time.Duration
	observed chan time.Duration
}

func (t *blockingTimeoutTool) Definition() tool.Definition {
	return tool.Definition{
		Name:        t.name,
		InputSchema: tool.Schema{Type: "object"},
		Timeout:     t.timeout,
	}
}

func (t *blockingTimeoutTool) Execute(ctx context.Context, _ tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	recordObservedDeadline(ctx, t.observed)
	<-ctx.Done()
	return tool.Result{}, ctx.Err()
}

func recordObservedDeadline(ctx context.Context, observed chan<- time.Duration) {
	deadline, ok := ctx.Deadline()
	if !ok {
		observed <- 0
		return
	}
	observed <- time.Until(deadline)
}

func assertDurationNear(t *testing.T, got, want time.Duration) {
	t.Helper()
	if got < want/2 || got > want+75*time.Millisecond {
		t.Fatalf("deadline = %s, want near %s", got, want)
	}
}
