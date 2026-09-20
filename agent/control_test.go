package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

type controlledProvider struct {
	run func(context.Context, provider.Request) (provider.Stream, error)
}

func (controlledProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "controlled"} }
func (driver controlledProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	return driver.run(ctx, request)
}

func controlFinal(text string) (provider.Stream, error) {
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: text},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}), nil
}

func awaitControlResult(t *testing.T, done <-chan Result) Result {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not complete")
		return Result{}
	}
}

func awaitInput(t *testing.T, ack <-chan error) error {
	t.Helper()
	select {
	case err := <-ack:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("input acknowledgement missing")
		return nil
	}
}

func TestControl_InputContinuesTerminalTurnAfterCheckpoint(t *testing.T) {
	for _, lowLevel := range []bool{false, true} {
		t.Run(map[bool]string{false: "Run", true: "RunMessages"}[lowLevel], func(t *testing.T) {
			control := &Control{}
			started, release := make(chan struct{}), make(chan struct{})
			calls, recorded := 0, false
			driver := controlledProvider{run: func(ctx context.Context, request provider.Request) (provider.Stream, error) {
				calls++
				if calls == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				} else if !recorded || request.Messages[len(request.Messages)-1].Text != "correction" {
					return nil, errors.New("input was not recorded before the model call")
				}
				return controlFinal("answer")
			}}
			engine := Engine{Provider: driver, Control: control, Boundaries: BoundaryObserverFunc(func(_ context.Context, value Continuation) error {
				if value.Phase == ContinuationReady && value.Messages[len(value.Messages)-1].Text == "correction" {
					recorded = true
				}
				return nil
			})}
			done := make(chan Result, 1)
			go func() {
				if lowLevel {
					output, err := engine.RunMessages(context.Background(), LoopInput{Messages: []message.Message{message.NewText(message.RoleUser, "original")}, Control: control})
					result := resultFromLoopOutput(output, 0)
					if err != nil {
						result.Failure = loopErrorFailure(context.Background(), err, false)
					}
					done <- result
				} else {
					done <- engine.Run(context.Background(), Request{Prompt: "original"}, OutputPolicy{})
				}
			}()
			t.Cleanup(control.Cancel)
			<-started
			parts := []message.ContentPart{message.TextPart("correction")}
			ack, err := control.Send(Request{Content: parts})
			if err != nil {
				t.Fatal(err)
			}
			parts[0].Text = "mutated"
			select {
			case <-ack:
				t.Fatal("acknowledged before consumption")
			default:
			}
			close(release)
			if err := awaitInput(t, ack); err != nil {
				t.Fatal(err)
			}
			result := awaitControlResult(t, done)
			if result.Failure != nil || calls != 2 {
				t.Fatalf("result=%+v calls=%d", result, calls)
			}
			if _, err := control.Send(Request{Prompt: "late"}); !errors.Is(err, errControlClosed) {
				t.Fatalf("late input=%v", err)
			}
			result = engine.Run(context.Background(), Request{Prompt: "reuse"}, OutputPolicy{})
			if result.Failure == nil || !errors.Is(result.Failure, errControlUsed) {
				t.Fatal("reused execution handle")
			}
		})
	}
}

func TestControl_CancelBoundsQueueAndRejectsUnconsumedInput(t *testing.T) {
	control := &Control{}
	started := make(chan struct{})
	driver := controlledProvider{run: func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	done := make(chan Result, 1)
	go func() {
		done <- (Engine{Provider: driver, Control: control}).Run(context.Background(), Request{Prompt: "wait"}, OutputPolicy{})
	}()
	t.Cleanup(control.Cancel)
	<-started
	if _, err := control.Send(Request{Prompt: strings.Repeat("x", 1<<20)}); !errors.Is(err, errControlLimit) {
		t.Fatalf("oversized input=%v", err)
	}
	if _, err := control.Send(Request{Prompt: "change", Budget: &Budget{MaxSteps: 2}}); err == nil {
		t.Fatal("budget mutation accepted")
	}
	acks := make([]<-chan error, 0, 64)
	for range 64 {
		ack, err := control.Send(Request{Prompt: "queued"})
		if err != nil {
			t.Fatal(err)
		}
		acks = append(acks, ack)
	}
	if _, err := control.Send(Request{Prompt: "overflow"}); !errors.Is(err, errControlLimit) {
		t.Fatalf("queue overflow=%v", err)
	}
	control.Cancel()
	result := awaitControlResult(t, done)
	if result.Failure == nil || !errors.Is(result.Failure, context.Canceled) {
		t.Fatalf("cancel=%v", result.Failure)
	}
	for _, ack := range acks {
		if err := awaitInput(t, ack); err == nil {
			t.Fatal("unconsumed input acknowledged")
		}
	}
}

func TestControl_CheckpointFailureDoesNotAcknowledgeInput(t *testing.T) {
	control := &Control{}
	var ack <-chan error
	failure := errors.New("checkpoint unavailable")
	engine := Engine{Control: control, Provider: controlledProvider{run: func(context.Context, provider.Request) (provider.Stream, error) {
		var err error
		ack, err = control.Send(Request{Prompt: "correction"})
		if err != nil {
			return nil, err
		}
		return controlFinal("first")
	}}, Boundaries: BoundaryObserverFunc(func(_ context.Context, value Continuation) error {
		if value.Phase == ContinuationReady && len(value.Steps) > 0 {
			return failure
		}
		return nil
	})}
	result := engine.Run(context.Background(), Request{Prompt: "original"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, failure) || !errors.Is(awaitInput(t, ack), failure) {
		t.Fatalf("result=%+v", result)
	}
}

func TestControl_ConcurrentSendAndFinishNeverLoseAcknowledgements(t *testing.T) {
	for range 30 {
		control := &Control{}
		ready, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		engine := Engine{Control: control, Provider: controlledProvider{run: func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
			if calls.Add(1) == 1 {
				close(ready)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return controlFinal("done")
		}}}
		done := make(chan Result, 1)
		go func() { done <- engine.Run(context.Background(), Request{Prompt: "original"}, OutputPolicy{}) }()
		<-ready
		var wait sync.WaitGroup
		wait.Add(1)
		var ack <-chan error
		var sendErr error
		go func() { defer wait.Done(); <-release; ack, sendErr = control.Send(Request{Prompt: "correction"}) }()
		close(release)
		result := awaitControlResult(t, done)
		wait.Wait()
		if result.Failure != nil {
			t.Fatal(result.Failure)
		}
		if sendErr != nil {
			if !errors.Is(sendErr, errControlClosed) {
				t.Fatal(sendErr)
			}
			continue
		}
		if err := awaitInput(t, ack); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("accepted input lost: calls=%d", calls.Load())
		}
	}
}
