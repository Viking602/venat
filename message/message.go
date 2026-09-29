package message

import (
	"encoding/json"
	"time"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
	RoleCustom    Role = "custom"
)

type Kind string

const (
	KindStandard          Kind = "standard"
	KindBranchSummary     Kind = "branch_summary"
	KindCompactionSummary Kind = "compaction_summary"
	KindCommandOutput     Kind = "command_output"
	KindCustom            Kind = "custom"
)

type JSONSchema struct {
	Type                 string                `json:"type,omitempty"`
	Description          string                `json:"description,omitempty"`
	Properties           map[string]JSONSchema `json:"properties,omitempty"`
	Required             []string              `json:"required,omitempty"`
	Items                *JSONSchema           `json:"items,omitempty"`
	Enum                 []string              `json:"enum,omitempty"`
	AdditionalProperties *bool                 `json:"additionalProperties,omitempty"`
}

type ToolConcurrencyMode string

const (
	ToolConcurrencyParallel   ToolConcurrencyMode = "parallel"
	ToolConcurrencySequential ToolConcurrencyMode = "sequential"
	ToolConcurrencyExclusive  ToolConcurrencyMode = "exclusive"
)

type ToolDefinition struct {
	Name             string              `json:"name"`
	Description      string              `json:"description,omitempty"`
	InputSchema      JSONSchema          `json:"inputSchema"`
	Terminal         bool                `json:"terminal,omitempty"`
	Timeout          time.Duration       `json:"timeout,omitempty"`
	Concurrency      ToolConcurrencyMode `json:"concurrency,omitempty"`
	ConcurrencyGroup string              `json:"concurrencyGroup,omitempty"`
	MaxConcurrency   int                 `json:"maxConcurrency,omitempty"`
}

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	// OperationID identifies the logical tool-call slot across provider retries.
	// The agent loop assigns it; providers and callers should leave it empty.
	OperationID string `json:"operationId,omitempty"`
}

type ToolResult struct {
	ToolCallID string          `json:"toolCallId,omitempty"`
	Name       string          `json:"name"`
	Content    string          `json:"content,omitempty"`
	Parts      []ContentPart   `json:"parts,omitempty"`
	Structured json.RawMessage `json:"structured,omitempty"`
	IsError    bool            `json:"isError,omitempty"`
}

type Message struct {
	ID      string        `json:"id,omitempty"`
	Role    Role          `json:"role"`
	Kind    Kind          `json:"kind,omitempty"`
	Name    string        `json:"name,omitempty"`
	Text    string        `json:"text,omitempty"`
	Content []ContentPart `json:"content,omitempty"`
	// CacheBoundary marks the end of a stable prompt prefix at this text
	// message. Providers with explicit prefix caching may map it to their
	// native cache-control marker; unsupported providers may ignore it.
	CacheBoundary bool `json:"cacheBoundary,omitempty"`
	// ContextArchived retains execution evidence replaced by a working summary.
	// Archived entries are omitted from model requests, not from transcripts.
	ContextArchived bool   `json:"contextArchived,omitempty"`
	Thinking        string `json:"thinking,omitempty"`
	// ThinkingSignature is the opaque signature Anthropic attaches to a
	// thinking block; it must be round-tripped verbatim on the next request
	// when extended thinking is combined with tool use, or the API rejects
	// the assistant turn. Empty for providers that do not sign reasoning.
	ThinkingSignature string `json:"thinkingSignature,omitempty"`
	// RedactedThinking carries the opaque payload of a redacted_thinking
	// block (reasoning encrypted by Anthropic's safety systems) so it can be
	// replayed verbatim on a later turn. Empty in the common case.
	RedactedThinking string `json:"redactedThinking,omitempty"`
	// ProviderState carries opaque provider output that must be replayed
	// verbatim on the next turn. Empty for providers that do not require it.
	ProviderState json.RawMessage   `json:"providerState,omitempty"`
	Response      ResponseMetadata  `json:"response,omitempty"`
	ToolCalls     []ToolCall        `json:"toolCalls,omitempty"`
	ToolResult    *ToolResult       `json:"toolResult,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func NewText(role Role, text string) Message {
	return Message{
		Role:    role,
		Kind:    KindStandard,
		Text:    text,
		Content: []ContentPart{TextPart(text)},
	}
}

func NewToolResult(result ToolResult) Message {
	result.SyncLegacyContent()
	return Message{
		Role:       RoleTool,
		Kind:       KindStandard,
		Name:       result.Name,
		ToolResult: &result,
	}
}
