package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Viking602/venat/provider"
)

func TestAgentToolReservationExhaustionDoesNotWaitWithoutActiveChildren(t *testing.T) {
	for _, spent := range []bool{false, true} {
		t.Run(map[bool]string{false: "initially empty", true: "fully spent"}[spent], func(t *testing.T) {
			tracker := newAgentToolTokenTracker(0)
			if spent {
				tracker = newAgentToolTokenTracker(10)
				reservation, _, err := tracker.reserve(context.Background(), "first", &Budget{MaxTokens: 10})
				if err != nil {
					t.Fatal(err)
				}
				reservation.settle(provider.Usage{TotalTokens: 10})
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, err := tracker.reserve(ctx, "next", nil)
			if !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("got %v, want exhaustion without waiting for a nonexistent child", err)
			}
		})
	}
}

func TestAgentToolReservationReusesReleasedTokens(t *testing.T) {
	tracker := newAgentToolTokenTracker(10)
	first, _, err := tracker.reserve(context.Background(), "first", &Budget{MaxTokens: 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		reservation, budget, reserveErr := tracker.reserve(ctx, "next", &Budget{MaxTokens: 10})
		if reserveErr != nil {
			done <- reserveErr
			return
		}
		if budget.MaxTokens != 7 {
			done <- errors.New("released tokens were not available to waiting child")
			return
		}
		reservation.settle(provider.Usage{TotalTokens: 2})
		done <- nil
	}()
	first.settle(provider.Usage{TotalTokens: 3})
	first.settle(provider.Usage{TotalTokens: 3})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	tracker.mu.Lock()
	remaining, active := tracker.remaining, tracker.active
	tracker.mu.Unlock()
	if remaining != 5 || active != 0 {
		t.Fatalf("remaining=%d active=%d, want 5 and 0", remaining, active)
	}
}
