package agent

import "time"

// ModelTimeoutPolicy follows the Codex provider defaults: a bounded connection
// phase and a bounded idle gap between streamed events. RequestTimeout is an
// optional whole-model-call cap; zero leaves the session/execution context as
// the outer deadline.
type ModelTimeoutPolicy struct {
	ConnectTimeout    time.Duration `json:"connectTimeout,omitempty"`
	RequestTimeout    time.Duration `json:"requestTimeout,omitempty"`
	StreamIdleTimeout time.Duration `json:"streamIdleTimeout,omitempty"`
	// DisableDefaults makes zero durations mean disabled instead of adopting
	// the Codex-compatible connect and stream-idle defaults.
	DisableDefaults bool `json:"disableDefaults,omitempty"`
}

const (
	defaultModelConnectTimeout    = 15 * time.Second
	defaultModelStreamIdleTimeout = 5 * time.Minute
)

func (policy ModelTimeoutPolicy) resolved() ModelTimeoutPolicy {
	if policy.DisableDefaults {
		return policy
	}
	if policy.ConnectTimeout == 0 {
		policy.ConnectTimeout = defaultModelConnectTimeout
	}
	if policy.StreamIdleTimeout == 0 {
		policy.StreamIdleTimeout = defaultModelStreamIdleTimeout
	}
	return policy
}

// LoopPolicy carries Engine defaults for one execution. A Request with a
// non-nil Budget replaces Budget as a whole.
type LoopPolicy struct {
	MaxIterations       int                 `json:"maxIterations,omitempty"`
	UnlimitedIterations bool                `json:"unlimitedIterations,omitempty"`
	Budget              *Budget             `json:"budget,omitempty"`
	SessionBudget       *SessionBudget      `json:"sessionBudget,omitempty"`
	ModelTimeouts       *ModelTimeoutPolicy `json:"modelTimeouts,omitempty"`
	ContextTokenTarget  int                 `json:"contextTokenTarget,omitempty"`
}
