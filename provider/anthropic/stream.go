package anthropic

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

type requestBody struct {
	Model         string             `json:"model"`
	MaxTokens     int                `json:"max_tokens"`
	Temperature   float64            `json:"temperature,omitempty"`
	TopP          float64            `json:"top_p,omitempty"`
	System        any                `json:"system,omitempty"`
	Messages      []anthropicMessage `json:"messages"`
	Tools         []anthropicTool    `json:"tools,omitempty"`
	Stream        bool               `json:"stream"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
	Thinking      *thinkingOptions   `json:"thinking,omitempty"`
	OutputConfig  *outputConfig      `json:"output_config,omitempty"`
	ToolChoice    *toolChoice        `json:"tool_choice,omitempty"`
	Metadata      map[string]string  `json:"metadata,omitempty"`
}

type outputConfig struct {
	Format *outputFormat `json:"format,omitempty"`
}

type outputFormat struct {
	Type   string          `json:"type"`
	Schema json.RawMessage `json:"schema"`
}

type toolChoice struct {
	Type               string `json:"type"`
	DisableParallelUse bool   `json:"disable_parallel_tool_use,omitempty"`
}

type cacheControl struct {
	Type string `json:"type"`
}

type thinkingOptions struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens"`
}

type anthropicMessage struct {
	Role    string         `json:"role"`
	Content []contentBlock `json:"content"`
}

// contentBlock is the union of the Anthropic content-block shapes the driver
// produces. A single block instance sets only the fields for its Type; all
// other fields are omitted, so one struct safely marshals every variant
// (text / tool_use / tool_result / thinking / redacted_thinking).
type contentBlock struct {
	Type string `json:"type"`
	// text
	Text         string        `json:"text,omitempty"`
	CacheControl *cacheControl `json:"cache_control,omitempty"`
	// image / document
	Source *anthropicContentSource `json:"source,omitempty"`
	// tool_use
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	// tool_result
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   any    `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
	// thinking
	Thinking  string `json:"thinking,omitempty"`
	Signature string `json:"signature,omitempty"`
	// redacted_thinking
	Data string `json:"data,omitempty"`
	// text citations are provider-owned metadata that must survive replay.
	Citations []json.RawMessage `json:"citations,omitempty"`
}

type anthropicContentSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

// anthropicUsage is the wire usage shape: message_start nests it under
// message.usage; message_delta carries it at event level (output_tokens).
type anthropicUsage struct {
	InputTokens              int  `json:"input_tokens"`
	CacheReadInputTokens     *int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens *int `json:"cache_creation_input_tokens"`
	OutputTokens             int  `json:"output_tokens"`
}

type anthropicTool struct {
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	InputSchema message.JSONSchema `json:"input_schema"`
}

type eventEnvelope struct {
	Type         string          `json:"type"`
	Index        int             `json:"index"`
	Message      json.RawMessage `json:"message"`
	ContentBlock struct {
		Type      string            `json:"type"`
		ID        string            `json:"id"`
		Name      string            `json:"name"`
		Data      string            `json:"data"`
		Text      string            `json:"text"`
		Input     json.RawMessage   `json:"input"`
		Citations []json.RawMessage `json:"citations"`
	} `json:"content_block"`
	Delta struct {
		Type        string          `json:"type"`
		Text        string          `json:"text"`
		Thinking    string          `json:"thinking"`
		Signature   string          `json:"signature"`
		PartialJSON string          `json:"partial_json"`
		StopReason  string          `json:"stop_reason"`
		Citation    json.RawMessage `json:"citation"`
	} `json:"delta"`
	Usage anthropicUsage `json:"usage"`
	// Error carries a mid-stream API error (overload, content policy,
	// invalid request that passed the initial 200). Anthropic's SSE error
	// event shape is {"type":"error","error":{"type":"...","message":"..."}}.
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

type anthropicProviderState struct {
	Message json.RawMessage `json:"message,omitempty"`
	Content []contentBlock  `json:"content,omitempty"`
}

type streamState struct {
	reader             *shared.Reader
	pending            []provider.Event
	finished           bool
	truncated          error
	usage              provider.Usage
	response           provider.ResponseMetadata
	stopReason         provider.StopReason
	toolCalls          map[int]provider.ToolCallDelta
	blocks             map[int]contentBlock
	blockOrder         []int
	message            json.RawMessage
	contextUsage       provider.ContextUsageObserver
	contextUsageCalled bool
}

func (d Driver) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	apiKey := d.config.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("anthropic api key is required")
	}
	body, err := d.buildRequestBody(request)
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(d.config.BaseURL, "/") + "/messages"
	client := shared.ClientOrDefault(d.config.Client, defaultResponseHeaderTimeout)
	idempotencyKey, err := shared.NewIdempotencyKey()
	if err != nil {
		return nil, fmt.Errorf("anthropic: generate idempotency key: %w", err)
	}
	// Stream initiation is retried (never mid-stream): the request body is
	// rebuilt per attempt and transient 429/5xx responses back off per
	// Config.Retry.
	resp, err := shared.DoWithRetry(ctx, client, d.config.Retry, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("x-api-key", apiKey)
		req.Header.Set("anthropic-version", d.config.Version)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		if len(d.config.Betas) > 0 {
			req.Header.Set("anthropic-beta", strings.Join(d.config.Betas, ","))
		}
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return nil, provider.NewHTTPError("anthropic", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	if !shared.IsEventStreamContentType(resp.Header.Get("Content-Type")) {
		defer func() { _ = resp.Body.Close() }()
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return nil, fmt.Errorf("anthropic api returned unexpected content type %q: %s", resp.Header.Get("Content-Type"), strings.TrimSpace(string(payload)))
	}
	return &anthropicStream{
		body: resp.Body,
		state: streamState{
			reader:       shared.NewReader(resp.Body),
			toolCalls:    map[int]provider.ToolCallDelta{},
			blocks:       map[int]contentBlock{},
			contextUsage: request.ContextUsage,
		},
	}, nil
}

// buildRequestBody maps the provider-neutral request onto Anthropic's wire
// contract, dropping unsupported fields with an operator-visible warning.
func (d Driver) buildRequestBody(request provider.Request) ([]byte, error) {
	if request.PromptCacheKey != "" {
		shared.WarnDrop("anthropic", "promptCacheKey", "prompt cache key is unsupported")
	}
	if request.ServiceTier != "" {
		shared.WarnDrop("anthropic", "serviceTier", "service tier is unsupported; use config.betas for beta tiers")
	}
	system, messages := toAnthropicRequest(request.Messages)
	outputConfig, err := anthropicResponseFormat(request.ResponseFormat)
	if err != nil {
		return nil, err
	}
	metadata := anthropicMetadata(request.Metadata)
	maxTokens := d.config.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	if request.MaxTokens > 0 {
		maxTokens = request.MaxTokens
	}
	var choice *toolChoice
	if request.ParallelToolCalls != nil && !*request.ParallelToolCalls && len(request.Tools) > 0 {
		choice = &toolChoice{Type: "auto", DisableParallelUse: true}
	}
	payload := requestBody{
		Model:         request.Model,
		MaxTokens:     maxTokens,
		Temperature:   request.Temperature,
		TopP:          request.TopP,
		System:        system,
		Messages:      messages,
		Tools:         toAnthropicTools(request.Tools),
		Stream:        true,
		StopSequences: request.StopSequences,
		Thinking:      thinkingFromBudget(request.ThinkingBudget),
		OutputConfig:  outputConfig,
		ToolChoice:    choice,
		Metadata:      metadata,
	}
	return marshalAnthropicRequest(payload, request.ExtraBody)
}

func anthropicResponseFormat(format *provider.ResponseFormat) (*outputConfig, error) {
	if format == nil {
		return nil, nil
	}
	switch format.Type {
	case "text":
		return nil, nil
	case "json_object":
		shared.WarnDrop("anthropic", "responseFormat", "json_object is unsupported; use json_schema")
		return nil, nil
	case "json_schema":
		var schema json.RawMessage
		if len(format.RawSchema) > 0 {
			schema = append(json.RawMessage(nil), format.RawSchema...)
		} else if format.Schema != nil {
			encoded, err := json.Marshal(format.Schema)
			if err != nil {
				return nil, fmt.Errorf("anthropic marshal response format schema: %w", err)
			}
			schema = encoded
		} else {
			shared.WarnDrop("anthropic", "responseFormat", "json_schema requires schema")
			return nil, nil
		}
		// Anthropic always grammar-enforces schemas; Name and Strict have no
		// corresponding wire fields and therefore have no behavioral effect.
		return &outputConfig{Format: &outputFormat{Type: "json_schema", Schema: schema}}, nil
	default:
		shared.WarnDrop("anthropic", "responseFormat", "unsupported response format type")
		return nil, nil
	}
}

func anthropicMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	unsupported := make([]string, 0, len(metadata))
	for key := range metadata {
		if key != "user_id" {
			unsupported = append(unsupported, key)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		shared.WarnDrop("anthropic", "metadata", "metadata keys unsupported: "+strings.Join(unsupported, ", "))
	}
	if value, ok := metadata["user_id"]; ok {
		return map[string]string{"user_id": value}
	}
	return nil
}

var managedAnthropicBodyFields = map[string]struct{}{
	"model":          {},
	"max_tokens":     {},
	"messages":       {},
	"system":         {},
	"tools":          {},
	"stream":         {},
	"stop_sequences": {},
	"thinking":       {},
	"output_config":  {},
	"tool_choice":    {},
	"metadata":       {},
}

var protectedAnthropicModelFields = map[string]struct{}{
	"temperature": {},
	"top_p":       {},
}

func marshalAnthropicRequest(payload requestBody, extraBody map[string]any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	merged := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &merged); err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(extraBody))
	maps.Copy(fields, extraBody)
	for key := range managedAnthropicBodyFields {
		delete(fields, key)
	}
	for key, value := range fields {
		if _, protected := protectedAnthropicModelFields[key]; protected {
			if _, set := merged[key]; set {
				continue
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal anthropic extra body field %q: %w", key, err)
		}
		merged[key] = encoded
	}
	return json.Marshal(merged)
}

type anthropicStream struct {
	body  io.ReadCloser
	state streamState
}

func (s *anthropicStream) Recv() (provider.Event, error) {
	for {
		if len(s.state.pending) > 0 {
			event := s.state.pending[0]
			s.state.pending = s.state.pending[1:]
			return event, nil
		}
		if s.state.truncated != nil {
			err := s.state.truncated
			s.state.truncated = nil
			s.state.finished = true
			return provider.Event{}, err
		}
		if s.state.finished {
			return provider.Event{}, io.EOF
		}
		current, readErr := s.state.reader.Next()
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return provider.Event{}, readErr
		}
		if readErr == io.EOF {
			s.state.truncated = io.ErrUnexpectedEOF
			continue
		}
		truncated := readErr == io.ErrUnexpectedEOF
		if strings.TrimSpace(current.Data) == "" {
			if truncated {
				s.state.truncated = io.ErrUnexpectedEOF
			}
			continue
		}
		var parsed eventEnvelope
		if err := json.Unmarshal([]byte(current.Data), &parsed); err != nil {
			if truncated {
				// Partial JSON from a cut connection: surface the truncation
				// error, not a decode error, so OpenRetryingStream can
				// classify the failure as retryable.
				return provider.Event{}, io.ErrUnexpectedEOF
			}
			return provider.Event{}, err
		}
		event, emit, err := s.consume(parsed)
		if err != nil {
			return provider.Event{}, err
		}
		if emit {
			if truncated {
				// The frame carrying the terminal event arrived unterminated:
				// its completion cannot be trusted, so the stream is rejected
				// as truncated instead of reporting success (ADR-030).
				return provider.Event{}, io.ErrUnexpectedEOF
			}
			return event, nil
		}
		if truncated {
			s.state.truncated = io.ErrUnexpectedEOF
		}
	}
}

// consume handles one decoded SSE envelope. The boolean reports a terminal
// event (done or error) that must reach the caller; non-terminal work
// accumulates into state and queues into pending.
func (s *anthropicStream) consume(parsed eventEnvelope) (provider.Event, bool, error) {
	switch parsed.Type {
	case "message_start":
		return provider.Event{}, false, s.recordMessageStart(parsed)
	case "content_block_start":
		s.recordContentBlockStart(parsed)
	case "content_block_delta":
		s.recordContentBlockDelta(parsed)
	case "message_delta":
		s.state.usage.OutputTokens = parsed.Usage.OutputTokens
		s.state.usage.TotalTokens = s.state.usage.InputTokens + parsed.Usage.OutputTokens
		s.state.stopReason = mapAnthropicStopReason(parsed.Delta.StopReason)
	case "message_stop":
		s.state.finished = true
		return provider.Event{
			Kind:          provider.EventDone,
			Usage:         s.state.usage,
			StopReason:    s.state.stopReason,
			ProviderState: s.providerState(),
			Response:      s.state.response,
		}, true, nil
	case "error":
		s.state.finished = true
		return provider.Event{
			Kind: provider.EventError,
			Err:  anthropicError(parsed.Error.Type, parsed.Error.Message),
		}, true, nil
	}
	return provider.Event{}, false, nil
}

// validateAnthropicBlocks rejects state that is not Anthropic content blocks.
// Other providers encode their replay state as JSON arrays too (OpenAI
// Responses items such as {"type":"message"} or {"type":"function_call"}),
// and a foreign array unmarshals into contentBlock without error, so a
// cross-provider fallback would silently replay garbage. Only block types
// this driver produces are accepted.
func validateAnthropicBlocks(blocks []contentBlock) error {
	for _, block := range blocks {
		switch block.Type {
		case "text", "tool_use", "tool_result", "thinking", "redacted_thinking",
			"image", "document", "web_search_tool_result":
		default:
			return fmt.Errorf("decode anthropic provider state: unsupported content block type %q", block.Type)
		}
	}
	return nil
}

func (s *anthropicStream) recordMessageStart(parsed eventEnvelope) error {
	s.state.message = append(s.state.message[:0], parsed.Message...)
	// Real message_start events nest input/cache usage under message.usage;
	// the event-level usage object is absent there and only appears on
	// message_delta (output_tokens).
	var metadata struct {
		ID    string         `json:"id"`
		Model string         `json:"model"`
		Usage anthropicUsage `json:"usage"`
	}
	if len(parsed.Message) > 0 {
		if err := json.Unmarshal(parsed.Message, &metadata); err != nil {
			return fmt.Errorf("decode anthropic message_start: %w", err)
		}
	}
	cacheReadTokens, cacheReadReported := optionalToken(metadata.Usage.CacheReadInputTokens)
	cacheWriteTokens, cacheWriteReported := optionalToken(metadata.Usage.CacheCreationInputTokens)
	s.state.usage.InputTokens = metadata.Usage.InputTokens + cacheReadTokens + cacheWriteTokens
	s.state.usage.CachedInputTokens = cacheReadTokens
	s.state.usage.CachedInputTokensReported = cacheReadReported
	s.state.usage.CacheWriteInputTokens = cacheWriteTokens
	s.state.usage.CacheWriteInputTokensReported = cacheWriteReported
	if s.state.contextUsage != nil && !s.state.contextUsageCalled {
		s.state.contextUsageCalled = true
		s.state.contextUsage(provider.ContextUsage{UsedTokens: s.state.usage.InputTokens})
	}
	s.state.response = provider.ResponseMetadata{ID: metadata.ID, Model: metadata.Model}
	return nil
}

func (s *anthropicStream) recordContentBlockStart(parsed eventEnvelope) {
	s.startBlock(parsed.Index, contentBlock{
		Type:      parsed.ContentBlock.Type,
		ID:        parsed.ContentBlock.ID,
		Name:      parsed.ContentBlock.Name,
		Data:      parsed.ContentBlock.Data,
		Text:      parsed.ContentBlock.Text,
		Input:     append(json.RawMessage(nil), parsed.ContentBlock.Input...),
		Citations: cloneRawSlice(parsed.ContentBlock.Citations),
	})
	switch parsed.ContentBlock.Type {
	case "tool_use":
		s.state.toolCalls[parsed.Index] = provider.ToolCallDelta{
			ID:   parsed.ContentBlock.ID,
			Name: parsed.ContentBlock.Name,
		}
		current := s.state.toolCalls[parsed.Index]
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:          provider.EventToolCallDelta,
			ToolCallDelta: &current,
		})
	case "redacted_thinking":
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:             provider.EventThinkingDelta,
			RedactedThinking: parsed.ContentBlock.Data,
		})
	}
}

func (s *anthropicStream) recordContentBlockDelta(parsed eventEnvelope) {
	block := s.block(parsed.Index)
	switch parsed.Delta.Type {
	case "text_delta":
		block.Text += parsed.Delta.Text
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:      provider.EventTextDelta,
			Text:      parsed.Delta.Text,
			TextPhase: provider.TextPhaseFinalAnswer,
		})
	case "thinking_delta":
		block.Thinking += parsed.Delta.Thinking
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:     provider.EventThinkingDelta,
			Thinking: parsed.Delta.Thinking,
		})
	case "signature_delta":
		block.Signature += parsed.Delta.Signature
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:      provider.EventThinkingDelta,
			Signature: parsed.Delta.Signature,
		})
	case "input_json_delta":
		if bytes.Equal(bytes.TrimSpace(block.Input), []byte("{}")) {
			block.Input = nil
		}
		block.Input = append(block.Input, parsed.Delta.PartialJSON...)
		current := s.state.toolCalls[parsed.Index]
		current.ArgumentsDelta = parsed.Delta.PartialJSON
		s.state.toolCalls[parsed.Index] = current
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:          provider.EventToolCallDelta,
			ToolCallDelta: &current,
		})
	case "citations_delta":
		if len(parsed.Delta.Citation) > 0 {
			block.Citations = append(block.Citations, append(json.RawMessage(nil), parsed.Delta.Citation...))
		}
	}
	s.state.blocks[parsed.Index] = block
}

func (s *anthropicStream) startBlock(index int, block contentBlock) {
	if existing, ok := s.state.blocks[index]; ok {
		if block.Type == "" {
			block.Type = existing.Type
		}
		if block.ID == "" {
			block.ID = existing.ID
		}
		if block.Name == "" {
			block.Name = existing.Name
		}
		if block.Text == "" {
			block.Text = existing.Text
		}
		if block.Thinking == "" {
			block.Thinking = existing.Thinking
		}
		if block.Signature == "" {
			block.Signature = existing.Signature
		}
		if block.Data == "" {
			block.Data = existing.Data
		}
		if len(block.Input) == 0 {
			block.Input = existing.Input
		}
		if len(block.Citations) == 0 {
			block.Citations = existing.Citations
		}
	} else {
		s.state.blockOrder = append(s.state.blockOrder, index)
	}
	s.state.blocks[index] = block
}

func (s *anthropicStream) block(index int) contentBlock {
	block, ok := s.state.blocks[index]
	if !ok {
		s.state.blockOrder = append(s.state.blockOrder, index)
	}
	return block
}

func (s *anthropicStream) providerState() json.RawMessage {
	content := make([]contentBlock, 0, len(s.state.blockOrder))
	for _, index := range s.state.blockOrder {
		if block, ok := s.state.blocks[index]; ok {
			content = append(content, block)
		}
	}
	state, err := json.Marshal(anthropicProviderState{
		Message: append(json.RawMessage(nil), s.state.message...),
		Content: content,
	})
	if err != nil {
		return nil
	}
	return state
}

func cloneRawSlice(values []json.RawMessage) []json.RawMessage {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]json.RawMessage, len(values))
	for index := range values {
		cloned[index] = append(json.RawMessage(nil), values[index]...)
	}
	return cloned
}

func decodeAnthropicProviderState(raw json.RawMessage) ([]contentBlock, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '[' {
		var blocks []contentBlock
		if err := json.Unmarshal(trimmed, &blocks); err != nil {
			return nil, fmt.Errorf("decode anthropic provider state: %w", err)
		}
		if err := validateAnthropicBlocks(blocks); err != nil {
			return nil, err
		}
		return blocks, nil
	}
	if trimmed[0] != '{' {
		return nil, fmt.Errorf("decode anthropic provider state: expected object or array")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return nil, fmt.Errorf("decode anthropic provider state: %w", err)
	}
	if _, hasMessage := envelope["message"]; !hasMessage {
		if _, hasContent := envelope["content"]; !hasContent {
			return nil, fmt.Errorf("decode anthropic provider state: expected message or content")
		}
	}
	var state anthropicProviderState
	if err := json.Unmarshal(trimmed, &state); err != nil {
		return nil, fmt.Errorf("decode anthropic provider state: %w", err)
	}
	if err := validateAnthropicBlocks(state.Content); err != nil {
		return nil, err
	}
	return state.Content, nil
}

func (s *anthropicStream) Close() error {
	return s.body.Close()
}

func anthropicError(errorType, message string) error {
	kind := provider.ErrorUnknown
	switch errorType {
	case "overloaded_error", "api_error":
		kind = provider.ErrorServer
	case "rate_limit_error":
		kind = provider.ErrorRateLimit
	case "invalid_request_error":
		kind = provider.ErrorInvalidRequest
	case "authentication_error":
		kind = provider.ErrorAuthentication
	case "permission_error":
		kind = provider.ErrorPermission
	case "not_found_error":
		kind = provider.ErrorNotFound
	}
	return &provider.Error{Provider: "anthropic", Kind: kind, Code: errorType, Message: message}
}

// toAnthropicRequest maps the loop's flat message history onto the Anthropic
// Messages wire format: system messages collapse into the top-level system
// parameter, assistant turns become ordered content-block arrays
// (thinking → redacted_thinking → text → tool_use), and consecutive tool
// results are coalesced into a single user message whose tool_result blocks
// lead it — both requirements of the API.
//
// One thinking block per assistant turn is assumed: the loop accumulates a
// turn's reasoning into a single string, so interleaved multi-block thinking
// is not represented here. A thinking block is only emitted when its
// signature is present, since the API rejects unsigned thinking blocks.
// systemAssembler accumulates system texts, switching from the plain string
// form to content blocks once a cache boundary forces block granularity.
type systemAssembler struct {
	parts       []string
	blocks      []contentBlock
	hasBoundary bool
}

func (a *systemAssembler) add(msg message.Message) {
	text := anthropicSystemText(msg.CanonicalContent())
	if msg.CacheBoundary {
		if text == "" {
			shared.WarnDrop("anthropic", "cacheBoundary", "cache boundary requires non-empty text")
			return
		}
		if !a.hasBoundary {
			if joined := strings.Join(a.parts, "\n\n"); joined != "" {
				a.blocks = append(a.blocks, contentBlock{Type: "text", Text: joined})
			}
			a.parts = nil
			a.hasBoundary = true
		}
		a.blocks = append(a.blocks, contentBlock{
			Type: "text", Text: text,
			CacheControl: &cacheControl{Type: "ephemeral"},
		})
		return
	}
	if text == "" {
		return
	}
	if a.hasBoundary {
		a.blocks = append(a.blocks, contentBlock{Type: "text", Text: text})
	} else {
		a.parts = append(a.parts, text)
	}
}

func (a *systemAssembler) result() any {
	if a.hasBoundary {
		return a.blocks
	}
	joined := strings.Join(a.parts, "\n\n")
	if joined == "" {
		// An absent system must stay nil: a boxed empty string defeats
		// omitempty and Anthropic rejects empty system text.
		return nil
	}
	return joined
}

func toAnthropicRequest(messages []message.Message) (any, []anthropicMessage) {
	var system systemAssembler
	items := make([]anthropicMessage, 0, len(messages))
	var pendingToolResults []contentBlock
	flush := func() {
		if len(pendingToolResults) > 0 {
			items = append(items, anthropicMessage{Role: "user", Content: pendingToolResults})
			pendingToolResults = nil
		}
	}
	for _, msg := range messages {
		switch msg.Role {
		case message.RoleSystem:
			flush()
			system.add(msg)
		case message.RoleTool:
			if msg.ToolResult == nil {
				if msg.CacheBoundary {
					shared.WarnDrop("anthropic", "cacheBoundary", "cache boundary requires a tool result")
				}
				continue
			}
			pendingToolResults = append(pendingToolResults, toolResultBlock(*msg.ToolResult, msg.CacheBoundary))
		case message.RoleAssistant:
			flush()
			cacheBoundary := msg.CacheBoundary
			if cacheBoundary && len(msg.ProviderState) > 0 {
				shared.WarnDrop("anthropic", "cacheBoundary", "cannot annotate opaque provider state")
				cacheBoundary = false
			}
			blocks := assistantBlocks(msg)
			if cacheBoundary {
				markAnthropicCacheBoundary(blocks)
			}
			if len(blocks) > 0 {
				items = append(items, anthropicMessage{Role: "assistant", Content: blocks})
			}
		default:
			flush()
			blocks := anthropicInputBlocks(msg.CanonicalContent(), msg.Role, msg.CacheBoundary)
			if len(blocks) > 0 {
				items = append(items, anthropicMessage{Role: "user", Content: blocks})
			}
		}
	}
	flush()
	return system.result(), items
}

func anthropicSystemText(parts []message.ContentPart) string {
	var text strings.Builder
	for _, part := range parts {
		switch part.Kind {
		case message.ContentText, message.ContentCommentary, message.ContentFinalAnswer:
			text.WriteString(part.Text)
		default:
			shared.WarnDrop("anthropic", "content", fmt.Sprintf("system message cannot serialize %s content", part.Kind))
		}
	}
	return text.String()
}

func markAnthropicCacheBoundary(blocks []contentBlock) {
	// The marker belongs after the complete message so the cached prefix
	// covers trailing tool_use blocks too, not just the text before them.
	// Thinking blocks are skipped: they cannot carry cache_control.
	for index := len(blocks) - 1; index >= 0; index-- {
		block := blocks[index]
		cacheable := block.Type == "tool_use" || block.Type == "tool_result" ||
			block.Type == "image" || block.Type == "document" ||
			(block.Type == "text" && block.Text != "")
		if cacheable {
			blocks[index].CacheControl = &cacheControl{Type: "ephemeral"}
			return
		}
	}
	shared.WarnDrop("anthropic", "cacheBoundary", "cache boundary requires non-empty text")
}

func assistantBlocks(msg message.Message) []contentBlock {
	if len(msg.ProviderState) > 0 {
		blocks, err := decodeAnthropicProviderState(msg.ProviderState)
		if err == nil {
			return blocks
		}
		shared.WarnDrop("anthropic", "providerState", "decode anthropic provider state")
	}
	parts := msg.CanonicalContent()
	blocks := make([]contentBlock, 0, len(parts)+len(msg.ToolCalls))
	sawVisible := false
	for _, part := range parts {
		switch part.Kind {
		case message.ContentReasoning:
			if part.Signature == "" {
				shared.WarnDrop("anthropic", "thinking", "unsigned thinking block dropped")
				continue
			}
			if sawVisible {
				shared.WarnDrop("anthropic", "thinking", "signed thinking must precede visible assistant content")
				continue
			}
			blocks = append(blocks, contentBlock{Type: "thinking", Thinking: part.Text, Signature: part.Signature})
		case message.ContentRedactedReasoning:
			if sawVisible {
				shared.WarnDrop("anthropic", "content", "redacted thinking must precede visible assistant content")
				continue
			}
			blocks = append(blocks, contentBlock{Type: "redacted_thinking", Data: string(part.Data)})
		case message.ContentText, message.ContentCommentary, message.ContentFinalAnswer:
			sawVisible = true
			blocks = append(blocks, contentBlock{Type: "text", Text: part.Text})
		default:
			shared.WarnDrop("anthropic", "content", fmt.Sprintf("assistant message cannot serialize %s content", part.Kind))
		}
	}
	for _, call := range msg.ToolCalls {
		input := json.RawMessage(call.Arguments)
		if len(input) == 0 {
			input = json.RawMessage("{}")
		}
		blocks = append(blocks, contentBlock{Type: "tool_use", ID: call.ID, Name: call.Name, Input: input})
	}
	return blocks
}

func anthropicInputBlocks(parts []message.ContentPart, role message.Role, cacheBoundary bool) []contentBlock {
	blocks := make([]contentBlock, 0, len(parts))
	for _, part := range parts {
		switch part.Kind {
		case message.ContentText, message.ContentCommentary, message.ContentFinalAnswer:
			blocks = append(blocks, contentBlock{Type: "text", Text: part.Text})
		case message.ContentImage:
			if role == message.RoleAssistant || role == message.RoleSystem {
				shared.WarnDrop("anthropic", "content", fmt.Sprintf("%s message cannot serialize %s content", role, part.Kind))
				continue
			}
			source := anthropicSource(part)
			if source != nil {
				blocks = append(blocks, contentBlock{Type: "image", Source: source})
			}
		case message.ContentFile:
			if role == message.RoleAssistant || role == message.RoleSystem {
				shared.WarnDrop("anthropic", "content", fmt.Sprintf("%s message cannot serialize %s content", role, part.Kind))
				continue
			}
			source := anthropicSource(part)
			if source != nil {
				blocks = append(blocks, contentBlock{Type: "document", Source: source})
			}
		default:
			shared.WarnDrop("anthropic", "content", fmt.Sprintf("%s message cannot serialize %s content", role, part.Kind))
		}
	}
	if cacheBoundary {
		markAnthropicCacheBoundary(blocks)
	}
	return blocks
}

func anthropicSource(part message.ContentPart) *anthropicContentSource {
	if part.URI != "" {
		return &anthropicContentSource{Type: "url", URL: part.URI}
	}
	if len(part.Data) == 0 || part.MediaType == "" {
		shared.WarnDrop("anthropic", "content", fmt.Sprintf("%s content requires uri or inline data with media type", part.Kind))
		return nil
	}
	return &anthropicContentSource{
		Type:      "base64",
		MediaType: part.MediaType,
		Data:      base64.StdEncoding.EncodeToString(part.Data),
	}
}

func toolResultBlock(result message.ToolResult, cacheBoundary bool) contentBlock {
	parts := result.CanonicalContent()
	if cacheBoundary && result.TextContent() == "" {
		shared.WarnDrop("anthropic", "cacheBoundary", "cache boundary requires non-empty tool result text")
	}
	blocks := anthropicInputBlocks(parts, message.RoleTool, false)
	var content any = blocks
	if len(blocks) == 1 && blocks[0].Type == "text" {
		content = blocks[0].Text
	}
	block := contentBlock{
		Type:      "tool_result",
		ToolUseID: result.ToolCallID,
		Content:   content,
		IsError:   result.IsError,
	}
	if cacheBoundary && result.TextContent() != "" {
		block.CacheControl = &cacheControl{Type: "ephemeral"}
	}
	return block
}

func toAnthropicTools(defs []message.ToolDefinition) []anthropicTool {
	items := make([]anthropicTool, 0, len(defs))
	for _, def := range defs {
		items = append(items, anthropicTool{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: def.InputSchema,
		})
	}
	return items
}

// thinkingFromBudget enables Claude extended thinking with the supplied
// budget. The API requires budget_tokens >= 1024, so the provided value is
// floored; a non-positive budget leaves the feature disabled.
func thinkingFromBudget(budget int) *thinkingOptions {
	if budget <= 0 {
		return nil
	}
	if budget < 1024 {
		budget = 1024
	}
	return &thinkingOptions{Type: "enabled", BudgetTokens: budget}
}

func mapAnthropicStopReason(reason string) provider.StopReason {
	switch reason {
	case "end_turn", "stop_sequence":
		return provider.StopReasonComplete
	case "max_tokens":
		return provider.StopReasonLength
	case "tool_use":
		return provider.StopReasonToolUse
	default:
		return provider.StopReasonUnknown
	}
}

func optionalToken(value *int) (int, bool) {
	if value == nil {
		return 0, false
	}
	return max(0, *value), true
}
