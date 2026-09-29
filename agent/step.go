package agent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Viking602/venat/provider"
)

// Step is one completed model turn in the bounded loop. ContextUsage is the
// cumulative usage of auxiliary context-model work observed by this boundary.
type Step struct {
	Index        int             `json:"index"`
	ModelCall    *ModelCall      `json:"modelCall,omitempty"`
	ToolCalls    []ToolCallTrace `json:"toolCalls,omitempty"`
	Observations []Observation   `json:"observations,omitempty"`
	Decision     StepDecision    `json:"decision,omitempty"`
	// ContextUsage is a cumulative snapshot, not a per-turn delta. It keeps
	// summary calls attributable even when they occur between model steps.
	ContextUsage provider.Usage `json:"contextUsage,omitzero"`
	BudgetUsed   BudgetUsage    `json:"budgetUsed,omitempty"`
}

type ModelCall struct {
	Provider                      string              `json:"provider,omitempty"`
	Model                         string              `json:"model"`
	InputTokens                   int                 `json:"inputTokens,omitempty"`
	CachedInputTokens             int                 `json:"cachedInputTokens,omitempty"`
	CachedInputTokensReported     bool                `json:"cachedInputTokensReported,omitempty"`
	CacheWriteInputTokens         int                 `json:"cacheWriteInputTokens,omitempty"`
	CacheWriteInputTokensReported bool                `json:"cacheWriteInputTokensReported,omitempty"`
	OutputTokens                  int                 `json:"outputTokens,omitempty"`
	ReasoningTokens               int                 `json:"reasoningTokens,omitempty"`
	TotalTokens                   int                 `json:"totalTokens,omitempty"`
	StopReason                    provider.StopReason `json:"stopReason,omitempty"`
}

type ToolCallTrace struct {
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type Observation struct {
	Kind    string `json:"kind"`
	Message string `json:"message,omitempty"`
}

// StepDecision is the loop's factual decision at a completed step boundary.
type StepDecision string

const (
	StepDecisionContinue StepDecision = "continue"
	StepDecisionFinish   StepDecision = "finish"
	StepDecisionFail     StepDecision = "fail"
)

type BudgetUsage struct {
	Tokens    int64         `json:"tokens,omitempty"`
	ToolCalls int           `json:"toolCalls,omitempty"`
	WallClock time.Duration `json:"wallClock,omitempty"`
}

// StepObserver observes a finalized step. An observer failure aborts before the
// loop advances.
type StepObserver interface {
	ObserveStep(context.Context, Step) error
}

// StepObserverFunc adapts a function to StepObserver.
type StepObserverFunc func(context.Context, Step) error

// ObserveStep delegates to f.
func (f StepObserverFunc) ObserveStep(ctx context.Context, step Step) error {
	return f(ctx, step)
}

// StepDecider may override the loop's natural decision at a continue boundary.
// It may continue, finish, or fail; routing is application-owned.
type StepDecider interface {
	Decide(state LoopSnapshot) (StepDecision, error)
}

// LoopSnapshot is the immutable trace presented to a StepDecider.
type LoopSnapshot struct {
	Steps []Step `json:"steps,omitempty"`
}
