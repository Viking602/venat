package openai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"os"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

type chatCompletionRequest struct {
	Model             string            `json:"model"`
	Messages          []chatMessage     `json:"messages"`
	Temperature       float64           `json:"temperature,omitempty"`
	TopP              float64           `json:"top_p,omitempty"`
	MaxTokens         int               `json:"max_tokens,omitempty"`
	Tools             []chatTool        `json:"tools,omitempty"`
	Stream            bool              `json:"stream"`
	StreamOptions     streamOptions     `json:"stream_options,omitempty"`
	Stop              []string          `json:"stop,omitempty"`
	Reasoning         *reasoningOptions `json:"reasoning,omitempty"`
	ResponseFormat    any               `json:"response_format,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	PromptCacheKey    string            `json:"prompt_cache_key,omitempty"`
	ServiceTier       string            `json:"service_tier,omitempty"`
	ParallelToolCalls *bool             `json:"parallel_tool_calls,omitempty"`
}

type reasoningOptions struct {
	Effort string `json:"effort,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

type chatMessage struct {
	Role             string         `json:"role"`
	Content          any            `json:"content,omitempty"`
	ReasoningContent string         `json:"reasoning_content,omitempty"`
	ToolCalls        []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string         `json:"tool_call_id,omitempty"`
}

type chatContentBlock struct {
	Type                  string               `json:"type"`
	Text                  string               `json:"text,omitempty"`
	ImageURL              *chatImageURL        `json:"image_url,omitempty"`
	InputAudio            *chatInputAudio      `json:"input_audio,omitempty"`
	File                  *chatInputFile       `json:"file,omitempty"`
	PromptCacheBreakpoint *chatCacheBreakpoint `json:"prompt_cache_breakpoint,omitempty"`
}

type chatImageURL struct {
	URL string `json:"url"`
}

type chatInputAudio struct {
	Data   string `json:"data"`
	Format string `json:"format"`
}

type chatInputFile struct {
	FileData string `json:"file_data,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	Filename string `json:"filename,omitempty"`
}

type chatCacheBreakpoint struct {
	Mode PromptCacheMode `json:"mode"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Parameters  message.JSONSchema `json:"parameters"`
}

type chatToolCall struct {
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function chatToolCallDetail `json:"function"`
}

type chatToolCallDetail struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type chunk struct {
	ID      string             `json:"id"`
	Model   string             `json:"model"`
	Choices []choiceChunk      `json:"choices"`
	Error   *responsesAPIError `json:"error,omitempty"`
	Usage   struct {
		PromptTokens        *int `json:"prompt_tokens"`
		CompletionTokens    int  `json:"completion_tokens"`
		TotalTokens         int  `json:"total_tokens"`
		PromptTokensDetails struct {
			CachedTokens     *int `json:"cached_tokens"`
			CacheWriteTokens *int `json:"cache_write_tokens"`
		} `json:"prompt_tokens_details"`
		CompletionTokensDetails struct {
			ReasoningTokens *int `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
}

type choiceChunk struct {
	Delta struct {
		Content          string              `json:"content"`
		ReasoningContent string              `json:"reasoning_content"`
		Reasoning        string              `json:"reasoning"`
		ToolCalls        []toolCallDeltaItem `json:"tool_calls"`
	} `json:"delta"`
	FinishReason string `json:"finish_reason"`
}

type toolCallDeltaItem struct {
	Index    *int   `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type streamState struct {
	reader               *shared.Reader
	pending              []provider.Event
	finished             bool
	usage                provider.Usage
	stopReason           provider.StopReason
	response             provider.ResponseMetadata
	splitter             thinkSplitter
	contextUsage         provider.ContextUsageObserver
	contextUsageReported bool
}

// thinkSplitter extracts <think>...</think> segments from a streamed token
// sequence. Tags may be split across chunks, so it buffers the trailing bytes
// that could still be a tag prefix until enough input arrives to decide.
type thinkSplitter struct {
	inThink bool
	buffer  string
}

const (
	thinkOpen  = "<think>"
	thinkClose = "</think>"
)

func (t *thinkSplitter) process(delta string) (text string, thinking string) {
	t.buffer += delta
	var textB, thinkB strings.Builder
	for {
		if t.inThink {
			idx := strings.Index(t.buffer, thinkClose)
			if idx >= 0 {
				thinkB.WriteString(t.buffer[:idx])
				t.buffer = t.buffer[idx+len(thinkClose):]
				t.inThink = false
				continue
			}
			safe := safeEmitLen(t.buffer, thinkClose)
			thinkB.WriteString(t.buffer[:safe])
			t.buffer = t.buffer[safe:]
			break
		}
		idx := strings.Index(t.buffer, thinkOpen)
		if idx >= 0 {
			textB.WriteString(t.buffer[:idx])
			t.buffer = t.buffer[idx+len(thinkOpen):]
			t.inThink = true
			continue
		}
		safe := safeEmitLen(t.buffer, thinkOpen)
		textB.WriteString(t.buffer[:safe])
		t.buffer = t.buffer[safe:]
		break
	}
	return textB.String(), thinkB.String()
}

// flush drains any bytes still buffered at stream end. Residual inside a
// <think> block is emitted as thinking; otherwise as text.
func (t *thinkSplitter) flush() (text string, thinking string) {
	if t.buffer == "" {
		return "", ""
	}
	out := t.buffer
	t.buffer = ""
	if t.inThink {
		return "", out
	}
	return out, ""
}

// safeEmitLen returns the count of leading bytes of s that cannot be the
// start of target. The suffix that is withheld may complete into target on
// the next chunk.
func safeEmitLen(s, target string) int {
	// Renamed from `max` to `maxLen` to avoid shadowing Go 1.21+ builtin.
	maxLen := len(target) - 1
	if maxLen > len(s) {
		maxLen = len(s)
	}
	for k := maxLen; k >= 1; k-- {
		if strings.HasPrefix(target, s[len(s)-k:]) {
			return len(s) - k
		}
	}
	return len(s)
}

func (d Driver) streamChatCompletions(ctx context.Context, request provider.Request) (provider.Stream, error) {
	responseFormat := responseFormatFromRequest(request.ResponseFormat)
	apiKey, err := d.apiKey()
	if err != nil {
		return nil, err
	}
	messages := toChatMessages(request.Messages)
	body, err := marshalChatCompletionRequest(chatCompletionRequest{
		Model:             request.Model,
		Messages:          messages,
		Temperature:       request.Temperature,
		TopP:              request.TopP,
		MaxTokens:         request.MaxTokens,
		Tools:             toChatTools(request.Tools),
		Stream:            true,
		StreamOptions:     streamOptions{IncludeUsage: true},
		Stop:              request.StopSequences,
		Reasoning:         reasoningFromBudget(request.ThinkingBudget),
		ResponseFormat:    responseFormat,
		Metadata:          request.Metadata,
		PromptCacheKey:    request.PromptCacheKey,
		ServiceTier:       request.ServiceTier,
		ParallelToolCalls: request.ParallelToolCalls,
	}, request.ExtraBody)
	if err != nil {
		return nil, err
	}
	bodyStream, err := d.postEventStream(ctx, "/chat/completions", body, apiKey)
	if err != nil {
		return nil, err
	}
	return &openAIStream{
		body: bodyStream,
		state: streamState{
			reader:       shared.NewReader(bodyStream),
			contextUsage: request.ContextUsage,
		},
	}, nil
}

func (d Driver) apiKey() (string, error) {
	apiKey := d.config.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey == "" {
		return "", fmt.Errorf("openai api key is required")
	}
	return apiKey, nil
}

func (d Driver) postEventStream(ctx context.Context, path string, body []byte, apiKey string) (io.ReadCloser, error) {
	endpoint := strings.TrimRight(d.config.BaseURL, "/") + path
	client := shared.ClientOrDefault(d.config.Client, defaultResponseHeaderTimeout)
	idempotencyKey, err := shared.NewIdempotencyKey()
	if err != nil {
		return nil, fmt.Errorf("openai: generate idempotency key: %w", err)
	}
	// Stream initiation is retried (never mid-stream): the request body is
	// rebuilt per attempt and transient 429/5xx responses back off per
	// Config.Retry.
	resp, err := shared.DoWithRetry(ctx, client, d.config.Retry, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return nil, provider.NewHTTPError("openai", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	if !isEventStreamContentType(resp.Header.Get("Content-Type")) {
		defer func() { _ = resp.Body.Close() }()
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return nil, fmt.Errorf("openai api returned unexpected content type %q: %s", resp.Header.Get("Content-Type"), strings.TrimSpace(string(payload)))
	}
	return resp.Body, nil
}

func marshalChatCompletionRequest(payload chatCompletionRequest, extraBody map[string]any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return marshalChatCompletionRequestBody(body, extraChatCompletionBodyFields(extraBody))
}

func marshalChatCompletionRequestBody(body []byte, extraFields map[string]any) ([]byte, error) {
	merged := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &merged); err != nil {
		return nil, err
	}
	for key, value := range extraFields {
		if _, protected := protectedChatModelFields[key]; protected {
			if _, set := merged[key]; set {
				continue
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal openai chat extra body field %q: %w", key, err)
		}
		merged[key] = encoded
	}
	return json.Marshal(merged)
}

func extraChatCompletionBodyFields(extraBody map[string]any) map[string]any {
	fields := make(map[string]any, len(extraBody))
	maps.Copy(fields, extraBody)
	for key := range managedChatCompletionBodyFields {
		delete(fields, key)
	}
	return fields
}

var managedChatCompletionBodyFields = map[string]struct{}{
	"model":               {},
	"messages":            {},
	"tools":               {},
	"stream":              {},
	"stream_options":      {},
	"stop":                {},
	"reasoning":           {},
	"response_format":     {},
	"metadata":            {},
	"prompt_cache_key":    {},
	"service_tier":        {},
	"parallel_tool_calls": {},
}

var protectedChatModelFields = map[string]struct{}{
	"temperature": {},
	"top_p":       {},
	"max_tokens":  {},
}

func responseFormatFromRequest(format *provider.ResponseFormat) any {
	if format == nil {
		return nil
	}
	switch format.Type {
	case "text":
		return map[string]any{"type": "text"}
	case "json_object":
		return map[string]any{"type": "json_object"}
	case "json_schema":
		if len(format.RawSchema) == 0 && format.Schema == nil {
			shared.WarnDrop("openai chat completions", "responseFormat", "response format json_schema requires schema")
			return nil
		}
		payload := map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   format.Name,
				"strict": format.Strict,
			},
		}
		if len(format.RawSchema) > 0 {
			payload["json_schema"].(map[string]any)["schema"] = format.RawSchema
		} else {
			payload["json_schema"].(map[string]any)["schema"] = format.Schema
		}
		return payload
	default:
		shared.WarnDrop("openai chat completions", "responseFormat",
			fmt.Sprintf("unsupported response format type %q", format.Type))
		return nil
	}
}

type openAIStream struct {
	body  io.ReadCloser
	state streamState
}

// Recv pulls the next provider.Event from the underlying SSE stream. The
// inner work is delegated to small helpers (handleDoneMarker / consumeChunk /
// processChoiceDelta) to keep this top-level method's cyclomatic complexity
// manageable for reviewers.
func (s *openAIStream) Recv() (provider.Event, error) {
	for {
		if len(s.state.pending) > 0 {
			event := s.state.pending[0]
			s.state.pending = s.state.pending[1:]
			return event, nil
		}
		if s.state.finished {
			return provider.Event{}, io.EOF
		}
		current, err := s.state.reader.Next()
		if err != nil {
			if err == io.EOF {
				return provider.Event{}, io.ErrUnexpectedEOF
			}
			return provider.Event{}, err
		}
		if strings.TrimSpace(current.Data) == "" {
			continue
		}
		if current.Data == "[DONE]" {
			s.handleDoneMarker()
			continue
		}
		var parsed chunk
		if err := json.Unmarshal([]byte(current.Data), &parsed); err != nil {
			return provider.Event{}, err
		}
		if parsed.Error != nil {
			s.state.finished = true
			return provider.Event{Kind: provider.EventError, Err: responsesError(parsed.Error)}, nil
		}
		s.consumeChunk(parsed)
		if parsed.ID != "" {
			s.state.response.ID = parsed.ID
		}
		if parsed.Model != "" {
			s.state.response.Model = parsed.Model
		}
	}
}

// handleDoneMarker flushes any buffered text/thinking from the splitter and
// emits the terminal Done event when the upstream sends `data: [DONE]`.
func (s *openAIStream) handleDoneMarker() {
	s.state.finished = true
	if text, thinking := s.state.splitter.flush(); text != "" || thinking != "" {
		if thinking != "" {
			s.state.pending = append(s.state.pending, provider.Event{
				Kind:     provider.EventThinkingDelta,
				Thinking: thinking,
			})
		}
		if text != "" {
			s.state.pending = append(s.state.pending, provider.Event{
				Kind:      provider.EventTextDelta,
				Text:      text,
				TextPhase: provider.TextPhaseFinalAnswer,
			})
		}
	}
	s.state.pending = append(s.state.pending, provider.Event{
		Kind:       provider.EventDone,
		Usage:      s.state.usage,
		StopReason: s.state.stopReason,
		Response:   s.state.response,
	})
}

// consumeChunk records usage tokens and dispatches each choice's delta.
func (s *openAIStream) consumeChunk(parsed chunk) {
	if parsed.Usage.TotalTokens > 0 || parsed.Usage.PromptTokens != nil || parsed.Usage.CompletionTokens > 0 ||
		parsed.Usage.PromptTokensDetails.CachedTokens != nil || parsed.Usage.PromptTokensDetails.CacheWriteTokens != nil ||
		parsed.Usage.CompletionTokensDetails.ReasoningTokens != nil {
		promptTokens := 0
		if parsed.Usage.PromptTokens != nil {
			promptTokens = *parsed.Usage.PromptTokens
		}
		if !s.state.contextUsageReported && s.state.contextUsage != nil && parsed.Usage.PromptTokens != nil {
			s.state.contextUsage(provider.ContextUsage{UsedTokens: promptTokens})
			s.state.contextUsageReported = true
		}
		cachedTokens, cacheReported := reportedToken(parsed.Usage.PromptTokensDetails.CachedTokens)
		cacheWriteTokens, cacheWriteReported := reportedToken(parsed.Usage.PromptTokensDetails.CacheWriteTokens)
		reasoningTokens, _ := reportedToken(parsed.Usage.CompletionTokensDetails.ReasoningTokens)
		s.state.usage = provider.Usage{
			InputTokens:                   promptTokens,
			CachedInputTokens:             cachedTokens,
			CachedInputTokensReported:     cacheReported,
			CacheWriteInputTokens:         cacheWriteTokens,
			CacheWriteInputTokensReported: cacheWriteReported,
			OutputTokens:                  parsed.Usage.CompletionTokens,
			ReasoningTokens:               reasoningTokens,
			TotalTokens:                   parsed.Usage.TotalTokens,
		}
	}
	for _, choice := range parsed.Choices {
		s.processChoiceDelta(choice)
	}
}

// processChoiceDelta turns a single choice delta into queued provider.Events
// (thinking, text, tool-call) and updates the recorded stop reason.
func (s *openAIStream) processChoiceDelta(choice choiceChunk) {
	if reasoning := choice.Delta.ReasoningContent; reasoning != "" {
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:     provider.EventThinkingDelta,
			Thinking: reasoning,
		})
	} else if reasoning := choice.Delta.Reasoning; reasoning != "" {
		s.state.pending = append(s.state.pending, provider.Event{
			Kind:     provider.EventThinkingDelta,
			Thinking: reasoning,
		})
	}
	if choice.Delta.Content != "" {
		text, thinking := s.state.splitter.process(choice.Delta.Content)
		if thinking != "" {
			s.state.pending = append(s.state.pending, provider.Event{
				Kind:     provider.EventThinkingDelta,
				Thinking: thinking,
			})
		}
		if text != "" {
			s.state.pending = append(s.state.pending, provider.Event{
				Kind:      provider.EventTextDelta,
				Text:      text,
				TextPhase: provider.TextPhaseFinalAnswer,
			})
		}
	}
	for _, item := range choice.Delta.ToolCalls {
		s.state.pending = append(s.state.pending, provider.Event{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				Index:          item.Index,
				ID:             item.ID,
				Name:           item.Function.Name,
				ArgumentsDelta: item.Function.Arguments,
			},
		})
	}
	if choice.FinishReason != "" {
		s.state.stopReason = mapOpenAIStopReason(choice.FinishReason)
	}
}

func (s *openAIStream) Close() error {
	return s.body.Close()
}

func toChatMessages(messages []message.Message) []chatMessage {
	items := make([]chatMessage, 0, len(messages))
	for _, msg := range messages {
		item := chatMessage{Role: string(msg.Role)}
		switch msg.Role {
		case message.RoleAssistant:
			item.Content = chatMessageContent(msg.CanonicalContent(), msg.CacheBoundary, msg.Role)
			item.ReasoningContent = msg.ReasoningContent()
			if len(msg.ToolCalls) > 0 {
				item.ToolCalls = make([]chatToolCall, 0, len(msg.ToolCalls))
				for _, call := range msg.ToolCalls {
					item.ToolCalls = append(item.ToolCalls, chatToolCall{
						ID:   call.ID,
						Type: "function",
						Function: chatToolCallDetail{
							Name:      call.Name,
							Arguments: string(call.Arguments),
						},
					})
				}
			}
		case message.RoleTool:
			if msg.ToolResult != nil {
				item.Content = chatMessageContent(msg.ToolResult.CanonicalContent(), msg.CacheBoundary, msg.Role)
				item.ToolCallID = msg.ToolResult.ToolCallID
			} else if msg.CacheBoundary {
				shared.WarnDrop("openai chat completions", "cacheBoundary",
					"cache boundary requires tool result content")
			}
		default:
			item.Content = chatMessageContent(msg.CanonicalContent(), msg.CacheBoundary, msg.Role)
		}
		items = append(items, item)
	}
	return items
}

func chatMessageContent(parts []message.ContentPart, cacheBoundary bool, role message.Role) any {
	blocks := make([]chatContentBlock, 0, len(parts))
	textOnly := true
	var plain strings.Builder
	for _, part := range parts {
		switch part.Kind {
		case message.ContentText, message.ContentCommentary, message.ContentFinalAnswer:
			plain.WriteString(part.Text)
			blocks = append(blocks, chatContentBlock{Type: "text", Text: part.Text})
		case message.ContentReasoning:
			if role != message.RoleAssistant {
				shared.WarnDrop("openai chat completions", "content",
					fmt.Sprintf("does not accept reasoning content for role %s", role))
			}
		case message.ContentRedactedReasoning:
			shared.WarnDrop("openai chat completions", "content",
				"cannot serialize redacted reasoning content")
		case message.ContentImage:
			if role != message.RoleUser {
				shared.WarnDrop("openai chat completions", "content",
					"image content requires user role")
				continue
			}
			url, err := contentPartURL(part)
			if err != nil {
				shared.WarnDrop("openai chat completions", "content",
					strings.TrimPrefix(err.Error(), "openai chat completions "))
				continue
			}
			textOnly = false
			blocks = append(blocks, chatContentBlock{Type: "image_url", ImageURL: &chatImageURL{URL: url}})
		case message.ContentAudio:
			if role != message.RoleUser || len(part.Data) == 0 {
				shared.WarnDrop("openai chat completions", "content",
					"audio content requires user-role inline data")
				continue
			}
			textOnly = false
			blocks = append(blocks, chatContentBlock{Type: "input_audio", InputAudio: &chatInputAudio{
				Data:   base64.StdEncoding.EncodeToString(part.Data),
				Format: audioFormat(part),
			}})
		case message.ContentFile:
			if role != message.RoleUser {
				shared.WarnDrop("openai chat completions", "content",
					"file content requires user role")
				continue
			}
			file, err := chatFilePart(part)
			if err != nil {
				shared.WarnDrop("openai chat completions", "content",
					strings.TrimPrefix(err.Error(), "openai chat completions "))
				continue
			}
			textOnly = false
			blocks = append(blocks, chatContentBlock{Type: "file", File: file})
		case message.ContentSource, message.ContentProviderData:
			shared.WarnDrop("openai chat completions", "content",
				fmt.Sprintf("cannot serialize %s content", part.Kind))
		default:
			shared.WarnDrop("openai chat completions", "content",
				fmt.Sprintf("received unknown content kind %q", part.Kind))
		}
	}
	if cacheBoundary {
		return chatContentWithCacheBoundary(blocks)
	}
	if textOnly {
		if plain.Len() == 0 {
			return nil
		}
		return plain.String()
	}
	return blocks
}

func chatContentWithCacheBoundary(blocks []chatContentBlock) any {
	for index := len(blocks) - 1; index >= 0; index-- {
		if blocks[index].Type == "text" && blocks[index].Text != "" {
			blocks[index].PromptCacheBreakpoint = &chatCacheBreakpoint{Mode: PromptCacheModeExplicit}
			return blocks
		}
	}
	shared.WarnDrop("openai chat completions", "cacheBoundary",
		"cache boundary requires non-empty text")
	if len(blocks) == 0 {
		return nil
	}
	return blocks
}

func contentPartURL(part message.ContentPart) (string, error) {
	if part.URI != "" {
		return part.URI, nil
	}
	if len(part.Data) == 0 || part.MediaType == "" {
		return "", fmt.Errorf("content %s requires uri or inline data with media type", part.Kind)
	}
	return "data:" + part.MediaType + ";base64," + base64.StdEncoding.EncodeToString(part.Data), nil
}

func audioFormat(part message.ContentPart) string {
	format := strings.TrimPrefix(part.MediaType, "audio/")
	switch format {
	case "mpeg":
		return "mp3"
	case "":
		return "wav"
	default:
		return format
	}
}

func chatFilePart(part message.ContentPart) (*chatInputFile, error) {
	file := &chatInputFile{Filename: part.Filename}
	switch {
	case len(part.Data) > 0 && part.MediaType != "":
		file.FileData = "data:" + part.MediaType + ";base64," + base64.StdEncoding.EncodeToString(part.Data)
	case part.URI != "":
		file.FileID = part.URI
	default:
		return nil, fmt.Errorf("openai chat completions file requires uri or inline data with media type")
	}
	return file, nil
}

func toChatTools(defs []message.ToolDefinition) []chatTool {
	items := make([]chatTool, 0, len(defs))
	for _, def := range defs {
		items = append(items, chatTool{
			Type: "function",
			Function: chatToolFunction{
				Name:        def.Name,
				Description: def.Description,
				Parameters:  def.InputSchema,
			},
		})
	}
	return items
}

// reasoningFromBudget maps a token-style budget hint onto the OpenAI
// reasoning.effort enum used by GPT-5 and o-series models. Returning nil
// means the caller opted out and the field is omitted from the request.
func reasoningFromBudget(budget int) *reasoningOptions {
	if budget <= 0 {
		return nil
	}
	switch {
	case budget < 2000:
		return &reasoningOptions{Effort: "low"}
	case budget < 10000:
		return &reasoningOptions{Effort: "medium"}
	default:
		return &reasoningOptions{Effort: "high"}
	}
}

func mapOpenAIStopReason(reason string) provider.StopReason {
	switch reason {
	case "stop":
		return provider.StopReasonComplete
	case "length":
		return provider.StopReasonLength
	case "tool_calls", "function_call":
		return provider.StopReasonToolUse
	case "content_filter":
		return provider.StopReasonContentFilter
	default:
		return provider.StopReasonUnknown
	}
}

func isEventStreamContentType(contentType string) bool {
	if strings.TrimSpace(contentType) == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	}
	return strings.EqualFold(mediaType, "text/event-stream")
}
