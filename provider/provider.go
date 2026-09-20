package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"

	"github.com/Viking602/venat/message"
)

type StopReason string

const (
	StopReasonUnknown       StopReason = "unknown"
	StopReasonComplete      StopReason = "complete"
	StopReasonToolUse       StopReason = "tool_use"
	StopReasonLength        StopReason = "length"
	StopReasonContentFilter StopReason = "content_filter"
	StopReasonMaxTurns      StopReason = "max_turns"
	StopReasonAborted       StopReason = "aborted"
	StopReasonError         StopReason = "error"
)

// TextPhase identifies the semantic phase of streamed assistant text.
type TextPhase string

const (
	// TextPhaseCommentary is intermediate commentary emitted before the answer.
	TextPhaseCommentary TextPhase = "commentary"
	// TextPhaseFinalAnswer is assistant text intended as the final answer.
	TextPhaseFinalAnswer TextPhase = "final_answer"
)

type EventKind string

const (
	EventTextDelta     EventKind = "text_delta"
	EventThinkingDelta EventKind = "thinking_delta"
	EventToolCallDelta EventKind = "tool_call_delta"
	EventToolCall      EventKind = "tool_call"
	EventDone          EventKind = "done"
	EventError         EventKind = "error"
)

type Metadata struct {
	Name    string   `json:"name"`
	Models  []string `json:"models,omitempty"`
	Version string   `json:"version,omitempty"`
}

type Usage struct {
	InputTokens                   int  `json:"inputTokens,omitempty"`
	CachedInputTokens             int  `json:"cachedInputTokens,omitempty"`
	CachedInputTokensReported     bool `json:"cachedInputTokensReported,omitempty"`
	CacheWriteInputTokens         int  `json:"cacheWriteInputTokens,omitempty"`
	CacheWriteInputTokensReported bool `json:"cacheWriteInputTokensReported,omitempty"`
	OutputTokens                  int  `json:"outputTokens,omitempty"`
	ReasoningTokens               int  `json:"reasoningTokens,omitempty"`
	TotalTokens                   int  `json:"totalTokens,omitempty"`
}

func (u Usage) Add(v Usage) Usage {
	u = normalizedUsage(u)
	v = normalizedUsage(v)
	return Usage{
		InputTokens:                   saturatingTokenAdd(u.InputTokens, v.InputTokens),
		CachedInputTokens:             saturatingTokenAdd(u.CachedInputTokens, v.CachedInputTokens),
		CachedInputTokensReported:     u.CachedInputTokensReported || v.CachedInputTokensReported,
		CacheWriteInputTokens:         saturatingTokenAdd(u.CacheWriteInputTokens, v.CacheWriteInputTokens),
		CacheWriteInputTokensReported: u.CacheWriteInputTokensReported || v.CacheWriteInputTokensReported,
		OutputTokens:                  saturatingTokenAdd(u.OutputTokens, v.OutputTokens),
		ReasoningTokens:               saturatingTokenAdd(u.ReasoningTokens, v.ReasoningTokens),
		TotalTokens:                   saturatingTokenAdd(u.TotalTokens, v.TotalTokens),
	}
}

func normalizedUsage(usage Usage) Usage {
	usage.InputTokens = max(0, usage.InputTokens)
	usage.CachedInputTokens = min(max(0, usage.CachedInputTokens), usage.InputTokens)
	usage.CacheWriteInputTokens = min(max(0, usage.CacheWriteInputTokens), usage.InputTokens)
	usage.OutputTokens = max(0, usage.OutputTokens)
	usage.ReasoningTokens = min(max(0, usage.ReasoningTokens), usage.OutputTokens)
	usage.CachedInputTokensReported = usage.CachedInputTokensReported || usage.CachedInputTokens > 0
	usage.CacheWriteInputTokensReported = usage.CacheWriteInputTokensReported || usage.CacheWriteInputTokens > 0
	usage.TotalTokens = max(max(0, usage.TotalTokens), saturatingTokenAdd(usage.InputTokens, usage.OutputTokens))
	return usage
}

func saturatingTokenAdd(left, right int) int {
	const maxInt = int(^uint(0) >> 1)
	if left >= maxInt-right {
		return maxInt
	}
	return left + right
}

type ToolCallDelta struct {
	Index          *int   `json:"index,omitempty"`
	ID             string `json:"id,omitempty"`
	Name           string `json:"name,omitempty"`
	ArgumentsDelta string `json:"argumentsDelta,omitempty"`
}

// ContextUsage reports non-billable provider context occupancy.
type ContextUsage struct {
	UsedTokens int `json:"usedTokens"`
	MaxTokens  int `json:"maxTokens,omitempty"`
}

type ContextUsageObserver func(ContextUsage)

type ResponseMetadata = message.ResponseMetadata

type Request struct {
	Model             string                   `json:"model"`
	OperationID       string                   `json:"-"`
	Messages          []message.Message        `json:"messages"`
	Temperature       float64                  `json:"temperature,omitempty"`
	TopP              float64                  `json:"topP,omitempty"`
	MaxTokens         int                      `json:"maxTokens,omitempty"`
	Tools             []message.ToolDefinition `json:"tools,omitempty"`
	Metadata          map[string]string        `json:"metadata,omitempty"`
	StopSequences     []string                 `json:"stopSequences,omitempty"`
	ThinkingBudget    int                      `json:"thinkingBudget,omitempty"`
	ResponseFormat    *ResponseFormat          `json:"responseFormat,omitempty"`
	PromptCacheKey    string                   `json:"promptCacheKey,omitempty"`
	ServiceTier       string                   `json:"serviceTier,omitempty"`
	ParallelToolCalls *bool                    `json:"parallelToolCalls,omitempty"`
	ContextUsage      ContextUsageObserver     `json:"-"`
	// ExtraBody contains provider wire fields, not process objects.
	// godoc-allow-any
	ExtraBody map[string]any `json:"extraBody,omitempty"`
}

type ResponseFormat struct {
	Type   string              `json:"type"`
	Name   string              `json:"name,omitempty"`
	Strict bool                `json:"strict,omitempty"`
	Schema *message.JSONSchema `json:"schema,omitempty"`
	// RawSchema preserves JSON Schema values that Schema cannot represent,
	// such as numeric enums. When present it takes precedence over Schema.
	RawSchema json.RawMessage `json:"rawSchema,omitempty"`
}

type Event struct {
	Kind      EventKind `json:"kind"`
	Text      string    `json:"text,omitempty"`
	TextPhase TextPhase `json:"textPhase,omitempty"`
	Thinking  string    `json:"thinking,omitempty"`
	// Signature carries the opaque thinking-block signature emitted alongside
	// reasoning (Anthropic signature_delta). It is associated with the
	// current thinking block and accumulated by NormalizeEvents.
	Signature string `json:"signature,omitempty"`
	// RedactedThinking carries the opaque payload of a redacted_thinking
	// block delivered whole by the provider.
	RedactedThinking string            `json:"redactedThinking,omitempty"`
	ToolCall         *message.ToolCall `json:"toolCall,omitempty"`
	ToolCallDelta    *ToolCallDelta    `json:"toolCallDelta,omitempty"`
	Usage            Usage             `json:"usage,omitempty"`
	StopReason       StopReason        `json:"stopReason,omitempty"`
	// ProviderState carries an opaque provider-owned turn payload that must be
	// replayed verbatim on a later request.
	ProviderState json.RawMessage  `json:"providerState,omitempty"`
	Response      ResponseMetadata `json:"response,omitempty"`
	Err           error            `json:"-"`
}

type Stream interface {
	Recv() (Event, error)
	Close() error
}

// StreamIdentity identifies the provider and model that opened a stream.
// Composite drivers attach this to the returned stream so callers can
// attribute the actual selected backend rather than the wrapper's metadata.
type StreamIdentity struct {
	Provider Metadata
	Model    string
}

// IdentifiedStream is optionally implemented by streams returned from
// composite drivers such as failover wrappers.
type IdentifiedStream interface {
	Stream
	Identity() StreamIdentity
}

type identifiedStream struct {
	Stream
	identity StreamIdentity
}

func (s identifiedStream) Identity() StreamIdentity {
	return s.identity
}

func identifyStream(stream Stream, identity StreamIdentity) Stream {
	if identified, ok := stream.(IdentifiedStream); ok {
		current := identified.Identity()
		if current.Provider.Name != "" || current.Model != "" {
			return stream
		}
	}
	return identifiedStream{Stream: stream, identity: identity}
}

type Driver interface {
	Metadata() Metadata
	Stream(ctx context.Context, request Request) (Stream, error)
}

var ErrNotImplemented = errors.New("provider driver not implemented")

// RetryableError marks a provider failure that is safe to retry from the last
// completed turn checkpoint.
type RetryableError interface {
	error
	Retryable() bool
}

// RetryDelayError carries a provider-requested minimum delay.
type RetryDelayError interface {
	error
	RetryDelay() time.Duration
}

// SuggestedRetryDelay returns a typed provider-requested retry delay.
func SuggestedRetryDelay(err error) time.Duration {
	var delayed RetryDelayError
	if !errors.As(err, &delayed) {
		return 0
	}
	return max(time.Duration(0), delayed.RetryDelay())
}

type ErrorKind string

const (
	ErrorUnknown        ErrorKind = "unknown"
	ErrorAuthentication ErrorKind = "authentication"
	ErrorPermission     ErrorKind = "permission"
	ErrorInvalidRequest ErrorKind = "invalid_request"
	ErrorNotFound       ErrorKind = "not_found"
	ErrorRateLimit      ErrorKind = "rate_limit"
	ErrorServer         ErrorKind = "server"
	ErrorStream         ErrorKind = "stream"
)

// Error is a provider-neutral failure classification. Provider adapters map
// wire-specific statuses and codes into Kind without string matching.
type Error struct {
	Provider   string
	Kind       ErrorKind
	Code       string
	StatusCode int
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil provider error>"
	}
	label := e.Provider
	if e.Code != "" {
		if label != "" {
			label += " "
		}
		label += e.Code
	}
	if label == "" {
		label = string(e.Kind)
	}
	if e.StatusCode != 0 {
		label = fmt.Sprintf("%s HTTP %d", label, e.StatusCode)
	}
	if e.Message == "" {
		return label
	}
	return label + ": " + e.Message
}

func (e *Error) Category() ErrorKind {
	if e == nil {
		return ErrorUnknown
	}
	return e.Kind
}

func (e *Error) Retryable() bool {
	return e != nil && (e.Kind == ErrorRateLimit || e.Kind == ErrorServer || e.Kind == ErrorStream)
}

func (e *Error) RetryDelay() time.Duration {
	if e == nil {
		return 0
	}
	return max(time.Duration(0), e.RetryAfter)
}

// ClassifiedError exposes a provider-neutral failure category.
type ClassifiedError interface {
	error
	Category() ErrorKind
}

// ErrorKindOf returns a typed provider failure category through wrapped errors.
func ErrorKindOf(err error) ErrorKind {
	var classified ClassifiedError
	if !errors.As(err, &classified) {
		return ErrorUnknown
	}
	return classified.Category()
}

// NewHTTPError maps a provider HTTP response to a generic failure category.
func NewHTTPError(providerName string, statusCode int, message string) *Error {
	return &Error{
		Provider:   providerName,
		Kind:       httpErrorKind(statusCode),
		StatusCode: statusCode,
		Message:    message,
	}
}

func httpErrorKind(statusCode int) ErrorKind {
	switch {
	case statusCode == http.StatusUnauthorized:
		return ErrorAuthentication
	case statusCode == http.StatusForbidden:
		return ErrorPermission
	case statusCode == http.StatusNotFound:
		return ErrorNotFound
	case statusCode == http.StatusTooManyRequests:
		return ErrorRateLimit
	case statusCode >= 500 && statusCode <= 599:
		return ErrorServer
	case statusCode >= 400 && statusCode <= 499:
		return ErrorInvalidRequest
	default:
		return ErrorUnknown
	}
}

// IsRetryableError recognizes typed transient provider failures and short
// transport interruptions. Context cancellation and deadlines are terminal.
func IsRetryableError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var marked RetryableError
	if errors.As(err, &marked) {
		return marked.Retryable()
	}
	if isRetryableSystemError(err) {
		return true
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Err != nil {
		return IsRetryableError(urlError.Err)
	}
	var operationError *net.OpError
	if errors.As(err, &operationError) && operationError.Err != nil {
		return IsRetryableError(operationError.Err)
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func isRetryableSystemError(err error) bool {
	for _, target := range []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		io.ErrClosedPipe,
		syscall.ECONNRESET,
		syscall.ECONNREFUSED,
		syscall.ECONNABORTED,
		syscall.EPIPE,
		syscall.ENETDOWN,
		syscall.ENETUNREACH,
		syscall.EHOSTUNREACH,
		syscall.ETIMEDOUT,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

type SliceStream struct {
	events []Event
	index  int
}

func NewSliceStream(events []Event) *SliceStream {
	return &SliceStream{events: events}
}

func (s *SliceStream) Recv() (Event, error) {
	if s.index >= len(s.events) {
		return Event{}, io.EOF
	}
	event := s.events[s.index]
	s.index++
	return event, nil
}

func (s *SliceStream) Close() error {
	return nil
}
