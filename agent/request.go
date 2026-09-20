package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/Viking602/venat/message"
)

// Request is the application-neutral input for one Engine execution.
// A nil Budget uses Engine.LoopPolicy.Budget. A non-nil Budget replaces the
// engine default as a whole; each zero dimension is unbounded.
type Request struct {
	Prompt string  `json:"prompt,omitempty"`
	Budget *Budget `json:"budget,omitempty"`
	// SessionBudget bounds cumulative work across this execution and its
	// continuations. A non-nil request value replaces the Engine default.
	SessionBudget *SessionBudget `json:"sessionBudget,omitempty"`
	// ModelTimeouts carries provider connect, total-request, and stream-idle
	// limits. A nil value uses the Engine policy.
	ModelTimeouts *ModelTimeoutPolicy `json:"modelTimeouts,omitempty"`
	// Content follows Prompt in the user message. Only user text and media
	// are accepted; provider reasoning and tool results have separate origins.
	Content []message.ContentPart `json:"content,omitempty"`
}

// Validate checks user-content provenance, media payloads, and budget bounds
// before a dispatch or durable execution can retain this request.
func (request Request) Validate() error {
	if err := validateBudget(request.Budget); err != nil {
		return err
	}
	if err := validateSessionBudget(request.SessionBudget); err != nil {
		return err
	}
	if err := validateModelTimeouts(request.ModelTimeouts); err != nil {
		return err
	}
	meaningful := strings.TrimSpace(request.Prompt) != ""
	for _, part := range request.Content {
		if part.Source != nil || part.Signature != "" || len(part.ProviderData) > 0 {
			return fmt.Errorf("agent: user content cannot contain provider or evidence metadata")
		}
		switch part.Kind {
		case message.ContentText:
			meaningful = meaningful || strings.TrimSpace(part.Text) != ""
		case message.ContentImage, message.ContentAudio, message.ContentFile:
			if len(part.Data) == 0 && strings.TrimSpace(part.URI) == "" {
				return fmt.Errorf("agent: %s input requires data or URI", part.Kind)
			}
			meaningful = true
		default:
			return fmt.Errorf("agent: unsupported user content kind %q", part.Kind)
		}
	}
	if len(request.Content) > 0 && !meaningful {
		return fmt.Errorf("agent: empty user content")
	}
	return nil
}

func requestMessage(request Request) (message.Message, error) {
	if err := request.Validate(); err != nil {
		return message.Message{}, err
	}
	parts := message.CloneContent(request.Content)
	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" && len(parts) == 0 {
		prompt = "Complete the assigned task and return a concise result."
	}
	if prompt != "" {
		parts = append([]message.ContentPart{message.TextPart(prompt)}, parts...)
	}
	result := message.Message{Role: message.RoleUser, Kind: message.KindStandard, Content: parts}
	result.SyncLegacyContent()
	return result, nil
}

// Budget bounds cumulative work performed by one Engine execution. Zero means
// unbounded for that dimension.
type Budget struct {
	MaxTokens int64 `json:"maxTokens,omitempty"`
	// MaxToolCalls counts dispatched attempts, including rejected names/arguments.
	MaxToolCalls int           `json:"maxToolCalls,omitempty"`
	MaxSteps     int           `json:"maxSteps,omitempty"`
	MaxWallClock time.Duration `json:"maxWallClock,omitempty"`
}

// SessionBudget bounds cumulative work across one execution and its
// continuations. Zero means unbounded for that dimension.
type SessionBudget struct {
	MaxTokens    int64         `json:"maxTokens,omitempty"`
	MaxWallClock time.Duration `json:"maxWallClock,omitempty"`
}

func validateBudget(budget *Budget) error {
	if budget != nil && (budget.MaxTokens < 0 || budget.MaxSteps < 0 || budget.MaxToolCalls < 0 || budget.MaxWallClock < 0) {
		return fmt.Errorf("agent: negative request budget")
	}
	return nil
}

func validateSessionBudget(budget *SessionBudget) error {
	if budget != nil && (budget.MaxTokens < 0 || budget.MaxWallClock < 0) {
		return fmt.Errorf("agent: negative session budget")
	}
	return nil
}

func validateModelTimeouts(policy *ModelTimeoutPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.ConnectTimeout < 0 || policy.RequestTimeout < 0 || policy.StreamIdleTimeout < 0 {
		return fmt.Errorf("agent: negative model timeout")
	}
	return nil
}

func cloneBudget(budget *Budget) *Budget {
	if budget == nil {
		return nil
	}
	cloned := *budget
	return &cloned
}

func cloneSessionBudget(budget *SessionBudget) *SessionBudget {
	if budget == nil {
		return nil
	}
	cloned := *budget
	return &cloned
}

func cloneModelTimeouts(policy *ModelTimeoutPolicy) *ModelTimeoutPolicy {
	if policy == nil {
		return nil
	}
	cloned := *policy
	return &cloned
}
