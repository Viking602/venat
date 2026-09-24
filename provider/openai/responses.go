package openai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

type responsesRequest struct {
	Model             string              `json:"model"`
	Input             []json.RawMessage   `json:"input"`
	Temperature       float64             `json:"temperature,omitempty"`
	TopP              float64             `json:"top_p,omitempty"`
	MaxOutputTokens   int                 `json:"max_output_tokens,omitempty"`
	Include           []string            `json:"include,omitempty"`
	Tools             []responsesTool     `json:"tools,omitempty"`
	Stream            bool                `json:"stream"`
	Store             bool                `json:"store"`
	Metadata          map[string]string   `json:"metadata,omitempty"`
	PromptCacheKey    string              `json:"prompt_cache_key,omitempty"`
	ServiceTier       string              `json:"service_tier,omitempty"`
	ParallelToolCalls *bool               `json:"parallel_tool_calls,omitempty"`
	Reasoning         *responsesReasoning `json:"reasoning,omitempty"`
	Text              *responsesText      `json:"text,omitempty"`
}

type responsesTool struct {
	Type        string             `json:"type"`
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Parameters  message.JSONSchema `json:"parameters"`
}

type responsesReasoning struct {
	Effort  string `json:"effort"`
	Summary string `json:"summary"`
}

type responsesText struct {
	Format map[string]any `json:"format"`
}

type responsesStreamEvent struct {
	Type        string              `json:"type"`
	OutputIndex int                 `json:"output_index"`
	Delta       string              `json:"delta"`
	Item        responsesOutputItem `json:"item"`
	Response    responsesResponse   `json:"response"`
	Error       *responsesAPIError  `json:"error"`
	Code        string              `json:"code"`
	Message     string              `json:"message"`
}

type responsesOutputItem struct {
	ID          string             `json:"id"`
	Type        string             `json:"type"`
	CallID      string             `json:"call_id"`
	Name        string             `json:"name"`
	Arguments   string             `json:"arguments"`
	Phase       provider.TextPhase `json:"phase"`
	Status      string             `json:"status,omitempty"`
	Action      json.RawMessage    `json:"action,omitempty"`
	ContainerID string             `json:"container_id,omitempty"`
	Code        string             `json:"code,omitempty"`
	Queries     []string           `json:"queries,omitempty"`
	Results     json.RawMessage    `json:"results,omitempty"`
	Outputs     json.RawMessage    `json:"outputs,omitempty"`
}

const (
	responsesWebSearchCall   = "web_search_call"
	responsesCodeInterpreter = "code_interpreter_call"
	responsesFileSearchCall  = "file_search_call"
)

type responsesResponse struct {
	ID                string                      `json:"id"`
	Model             string                      `json:"model"`
	Output            json.RawMessage             `json:"output"`
	Usage             responsesUsage              `json:"usage"`
	IncompleteDetails *responsesIncompleteDetails `json:"incomplete_details"`
	Error             *responsesAPIError          `json:"error"`
}

type responsesUsage struct {
	InputTokens        int `json:"input_tokens"`
	OutputTokens       int `json:"output_tokens"`
	TotalTokens        int `json:"total_tokens"`
	InputTokensDetails struct {
		CachedTokens     *int `json:"cached_tokens"`
		CacheWriteTokens *int `json:"cache_write_tokens"`
	} `json:"input_tokens_details"`
	OutputTokensDetails struct {
		ReasoningTokens *int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

type responsesIncompleteDetails struct {
	Reason string `json:"reason"`
}

type responsesAPIError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type responsesOutputState struct {
	phase             provider.TextPhase
	callID            string
	name              string
	hadArgumentDeltas bool
	hadToolCallEvent  bool
}

type responsesStream struct {
	body                 io.ReadCloser
	reader               *shared.Reader
	items                map[int]*responsesOutputState
	contextUsage         provider.ContextUsageObserver
	contextUsageReported bool
	finished             bool
}

func (d Driver) streamResponses(ctx context.Context, request provider.Request) (provider.Stream, error) {
	if len(request.StopSequences) > 0 {
		shared.WarnDrop("openai responses", "stopSequences", "does not support stop sequences")
	}
	apiKey, err := d.apiKey()
	if err != nil {
		return nil, err
	}
	input, err := toResponsesInput(request.Messages)
	if err != nil {
		return nil, err
	}
	text := responsesTextFromRequest(request.ResponseFormat)
	body, err := marshalResponsesRequest(responsesRequest{
		Model:             request.Model,
		Temperature:       request.Temperature,
		TopP:              request.TopP,
		MaxOutputTokens:   request.MaxTokens,
		Input:             input,
		Tools:             toResponsesTools(request.Tools),
		Stream:            true,
		Metadata:          request.Metadata,
		PromptCacheKey:    request.PromptCacheKey,
		ServiceTier:       request.ServiceTier,
		ParallelToolCalls: request.ParallelToolCalls,
		Reasoning:         responsesReasoningFromBudget(request.ThinkingBudget),
		Text:              text,
	}, request.ExtraBody)
	if err != nil {
		return nil, err
	}
	bodyStream, err := d.postEventStream(ctx, "/responses", body, apiKey)
	if err != nil {
		return nil, err
	}
	return &responsesStream{
		body:         bodyStream,
		reader:       shared.NewReader(bodyStream),
		items:        make(map[int]*responsesOutputState),
		contextUsage: request.ContextUsage,
	}, nil
}

const responsesEncryptedReasoningInclude = "reasoning.encrypted_content"

func marshalResponsesRequest(payload responsesRequest, extraBody map[string]any) ([]byte, error) {
	include, err := responsesIncludes(extraBody)
	if err != nil {
		return nil, err
	}
	payload.Include = include
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	merged := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &merged); err != nil {
		return nil, err
	}
	for key, value := range extraResponsesBodyFields(extraBody) {
		if _, protected := protectedResponsesModelFields[key]; protected {
			if _, set := merged[key]; set {
				continue
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal openai responses extra body field %q: %w", key, err)
		}
		merged[key] = encoded
	}
	if err := mergeResponsesBodyObject(merged, extraBody, "reasoning", "effort"); err != nil {
		return nil, err
	}
	if err := mergeResponsesBodyObject(merged, extraBody, "text", "format"); err != nil {
		return nil, err
	}
	return json.Marshal(merged)
}

func mergeResponsesBodyObject(merged map[string]json.RawMessage, extraBody map[string]any, key string, protectedKeys ...string) error {
	value, ok := extraBody[key]
	if !ok || value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal openai responses extra body field %q: %w", key, err)
	}
	extra := map[string]json.RawMessage{}
	if err := json.Unmarshal(encoded, &extra); err != nil || extra == nil {
		return fmt.Errorf("openai responses ExtraBody %s must be an object", key)
	}
	current := map[string]json.RawMessage{}
	if encodedCurrent, exists := merged[key]; exists {
		if err := json.Unmarshal(encodedCurrent, &current); err != nil {
			return fmt.Errorf("decode openai responses managed body field %q: %w", key, err)
		}
	}
	droppedProtected := map[string]struct{}{}
	for _, protectedKey := range protectedKeys {
		if _, managed := current[protectedKey]; !managed {
			continue
		}
		if _, supplied := extra[protectedKey]; supplied {
			if key != "reasoning" {
				return fmt.Errorf("openai responses ExtraBody %s.%s conflicts with a managed request field", key, protectedKey)
			}
			shared.WarnDrop("openai responses", "extraBody", "reasoning.effort conflicts with a managed request field")
			droppedProtected[protectedKey] = struct{}{}
		}
	}
	for field, fieldValue := range extra {
		if _, dropped := droppedProtected[field]; dropped {
			continue
		}
		current[field] = fieldValue
	}
	encodedCurrent, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("marshal openai responses merged body field %q: %w", key, err)
	}
	merged[key] = encodedCurrent
	return nil
}

func responsesIncludes(extraBody map[string]any) ([]string, error) {
	include := []string{responsesEncryptedReasoningInclude}
	requested, ok := extraBody["include"]
	if !ok || requested == nil {
		return include, nil
	}
	encoded, err := json.Marshal(requested)
	if err != nil {
		return nil, fmt.Errorf("marshal openai responses include: %w", err)
	}
	var extra []string
	if err := json.Unmarshal(encoded, &extra); err != nil {
		return nil, fmt.Errorf("openai responses ExtraBody include must be an array of strings: %w", err)
	}
	seen := map[string]struct{}{responsesEncryptedReasoningInclude: {}}
	for _, item := range extra {
		if _, duplicate := seen[item]; duplicate {
			continue
		}
		seen[item] = struct{}{}
		include = append(include, item)
	}
	return include, nil
}

func extraResponsesBodyFields(extraBody map[string]any) map[string]any {
	fields := make(map[string]any, len(extraBody))
	for key, value := range extraBody {
		if _, managed := managedResponsesBodyFields[key]; !managed {
			fields[key] = value
		}
	}
	return fields
}

var managedResponsesBodyFields = map[string]struct{}{
	"model":                {},
	"include":              {},
	"input":                {},
	"tools":                {},
	"stream":               {},
	"reasoning":            {},
	"text":                 {},
	"metadata":             {},
	"instructions":         {},
	"previous_response_id": {},
	"conversation":         {},
	"prompt":               {},
	"prompt_cache_key":     {},
	"service_tier":         {},
	"parallel_tool_calls":  {},
}

var protectedResponsesModelFields = map[string]struct{}{
	"temperature":       {},
	"top_p":             {},
	"max_output_tokens": {},
}

func toResponsesInput(messages []message.Message) ([]json.RawMessage, error) {
	items := make([]json.RawMessage, 0, len(messages))
	for _, msg := range messages {
		var err error
		switch msg.Role {
		case message.RoleAssistant:
			items, err = appendResponsesAssistantInput(items, msg)
		case message.RoleTool:
			items, err = appendResponsesToolOutput(items, msg.ToolResult, msg.CacheBoundary)
		default:
			items, err = appendResponsesTextMessage(items, msg.Role, msg.CanonicalContent(), msg.CacheBoundary)
		}
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func appendResponsesAssistantInput(items []json.RawMessage, msg message.Message) ([]json.RawMessage, error) {
	cacheBoundary := msg.CacheBoundary
	if cacheBoundary && len(msg.ProviderState) > 0 {
		shared.WarnDrop("openai responses", "cacheBoundary", "cannot annotate opaque provider state")
		cacheBoundary = false
	}
	if len(msg.ProviderState) > 0 {
		stateItems, err := decodeResponsesProviderState(msg.ProviderState)
		if err == nil {
			return append(items, stateItems...), nil
		}
		shared.WarnDrop("openai responses", "providerState", strings.TrimPrefix(err.Error(), "openai responses "))
	}
	var err error
	if len(msg.CanonicalContent()) > 0 || cacheBoundary {
		items, err = appendResponsesTextMessage(items, message.RoleAssistant, msg.CanonicalContent(), cacheBoundary)
		if err != nil {
			return nil, err
		}
	}
	for _, call := range msg.ToolCalls {
		items, err = appendResponsesInputItem(items, map[string]any{
			"type":      "function_call",
			"call_id":   call.ID,
			"name":      call.Name,
			"arguments": string(call.Arguments),
		})
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func appendResponsesTextMessage(
	items []json.RawMessage,
	role message.Role,
	parts []message.ContentPart,
	cacheBoundary bool,
) ([]json.RawMessage, error) {
	content, err := responsesMessageContent(parts, role, cacheBoundary)
	if err != nil {
		return nil, err
	}
	return appendResponsesInputItem(items, map[string]any{
		"role":    role,
		"content": content,
	})
}

func responsesMessageContent(parts []message.ContentPart, role message.Role, cacheBoundary bool) (any, error) {
	blocks := make([]map[string]any, 0, len(parts))
	textOnly := true
	var plain strings.Builder
	for _, part := range parts {
		block, keepText, ok := responsesContentBlock(part, role, &plain)
		if !ok {
			continue
		}
		textOnly = textOnly && keepText
		blocks = append(blocks, block)
	}
	if cacheBoundary {
		for index := len(blocks) - 1; index >= 0; index-- {
			if blocks[index]["type"] == "input_text" && blocks[index]["text"] != "" {
				blocks[index]["prompt_cache_breakpoint"] = map[string]string{"mode": "explicit"}
				return blocks, nil
			}
		}
		shared.WarnDrop("openai responses", "cacheBoundary", "requires non-empty text")
	}
	if textOnly {
		return plain.String(), nil
	}
	return blocks, nil
}

// responsesContentBlock maps one content part onto a responses input block.
// keepText reports whether the part belongs to the plain-text fast path; ok
// is false when the part was dropped with a warning.
func responsesContentBlock(part message.ContentPart, role message.Role, plain *strings.Builder) (block map[string]any, keepText bool, ok bool) {
	switch part.Kind {
	case message.ContentText, message.ContentCommentary, message.ContentFinalAnswer:
		plain.WriteString(part.Text)
		return map[string]any{"type": "input_text", "text": part.Text}, true, true
	case message.ContentReasoning, message.ContentRedactedReasoning:
		shared.WarnDrop("openai responses", "content", "requires opaque ProviderState to replay reasoning content")
	case message.ContentImage:
		if role != message.RoleUser {
			shared.WarnDrop("openai responses", "content", "image content requires user role")
			return nil, false, false
		}
		url, err := contentPartURL(part)
		if err != nil {
			shared.WarnDrop("openai responses", "content", err.Error())
			return nil, false, false
		}
		return map[string]any{"type": "input_image", "image_url": url}, false, true
	case message.ContentAudio:
		if role != message.RoleUser || len(part.Data) == 0 {
			shared.WarnDrop("openai responses", "content", "audio content requires user-role inline data")
			return nil, false, false
		}
		return map[string]any{
			"type": "input_audio",
			"input_audio": map[string]any{
				"data":   base64.StdEncoding.EncodeToString(part.Data),
				"format": audioFormat(part),
			},
		}, false, true
	case message.ContentFile:
		if role != message.RoleUser {
			shared.WarnDrop("openai responses", "content", "file content requires user role")
			return nil, false, false
		}
		file, err := chatFilePart(part)
		if err != nil {
			shared.WarnDrop("openai responses", "content", err.Error())
			return nil, false, false
		}
		block := map[string]any{"type": "input_file", "filename": file.Filename}
		if file.FileData != "" {
			block["file_data"] = file.FileData
		} else {
			block["file_id"] = file.FileID
		}
		return block, false, true
	case message.ContentSource:
		shared.WarnDrop("openai responses", "content", "cannot serialize source content")
	case message.ContentProviderData:
		shared.WarnDrop("openai responses", "content", "cannot serialize provider_data content")
	default:
		shared.WarnDrop("openai responses", "content", fmt.Sprintf("unknown content kind %q dropped", part.Kind))
	}
	return nil, false, false
}

func appendResponsesToolOutput(items []json.RawMessage, result *message.ToolResult, cacheBoundary bool) ([]json.RawMessage, error) {
	if result == nil {
		if cacheBoundary {
			shared.WarnDrop("openai responses", "cacheBoundary", "requires a tool result")
		}
		return items, nil
	}
	if cacheBoundary && result.TextContent() == "" {
		shared.WarnDrop("openai responses", "cacheBoundary", "requires non-empty tool result text")
		cacheBoundary = false
	}
	output, err := responsesMessageContent(result.CanonicalContent(), message.RoleTool, cacheBoundary)
	if err != nil {
		return nil, err
	}
	return appendResponsesInputItem(items, map[string]any{
		"type":    "function_call_output",
		"call_id": result.ToolCallID,
		"output":  output,
	})
}

func appendResponsesInputItem(items []json.RawMessage, item map[string]any) ([]json.RawMessage, error) {
	encoded, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return append(items, encoded), nil
}

func decodeResponsesProviderState(state json.RawMessage) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(state)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil, fmt.Errorf("openai responses provider state must be a JSON array")
	}
	var items []json.RawMessage
	if err := json.Unmarshal(trimmed, &items); err != nil {
		return nil, fmt.Errorf("decode openai responses provider state: %w", err)
	}
	return items, nil
}

func toResponsesTools(defs []message.ToolDefinition) []responsesTool {
	items := make([]responsesTool, 0, len(defs))
	for _, def := range defs {
		items = append(items, responsesTool{
			Type:        "function",
			Name:        def.Name,
			Description: def.Description,
			Parameters:  def.InputSchema,
		})
	}
	return items
}

func responsesReasoningFromBudget(budget int) *responsesReasoning {
	reasoning := reasoningFromBudget(budget)
	if reasoning == nil {
		return nil
	}
	return &responsesReasoning{Effort: reasoning.Effort, Summary: "auto"}
}

func responsesTextFromRequest(format *provider.ResponseFormat) *responsesText {
	if format == nil {
		return nil
	}
	switch format.Type {
	case "text", "json_object":
		return &responsesText{Format: map[string]any{"type": format.Type}}
	case "json_schema":
		payload := map[string]any{
			"type":   format.Type,
			"name":   format.Name,
			"strict": format.Strict,
		}
		if len(format.RawSchema) > 0 {
			payload["schema"] = format.RawSchema
		} else if format.Schema != nil {
			payload["schema"] = format.Schema
		} else {
			shared.WarnDrop("openai responses", "responseFormat", "requires schema")
			return nil
		}
		return &responsesText{Format: payload}
	default:
		shared.WarnDrop("openai responses", "responseFormat", fmt.Sprintf("unsupported response format type %q", format.Type))
		return nil
	}
}

func (s *responsesStream) Recv() (provider.Event, error) {
	if s.finished {
		return provider.Event{}, io.EOF
	}
	for {
		frame, err := s.reader.Next()
		if err != nil {
			if err == io.EOF {
				return provider.Event{}, io.ErrUnexpectedEOF
			}
			return provider.Event{}, err
		}
		if strings.TrimSpace(frame.Data) == "" {
			continue
		}
		var event responsesStreamEvent
		if err := json.Unmarshal([]byte(frame.Data), &event); err != nil {
			return provider.Event{}, fmt.Errorf("decode openai responses stream event: %w", err)
		}
		if event.Type == "" {
			event.Type = frame.Name
		}
		result, emit, err := s.consume(event)
		if err != nil {
			return provider.Event{}, err
		}
		if emit {
			return result, nil
		}
	}
}

func (s *responsesStream) consume(event responsesStreamEvent) (provider.Event, bool, error) {
	switch event.Type {
	case "response.output_item.added":
		s.recordOutputItem(event.OutputIndex, event.Item)
		if isResponsesBuiltInTool(event.Item.Type) {
			return s.builtInToolDelta(event.OutputIndex, event.Item), true, nil
		}
	case "response.output_text.annotation.added", "response.output_text.logprobs", "response.output_audio.delta":
		// Citations, logprobs, and audio are provider-specific metadata. The
		// terminal response output is retained verbatim in ProviderState, so
		// these events must be consumed without flattening or dropping them.
		return provider.Event{}, false, nil
	case "response.output_text.delta", "response.refusal.delta":
		return provider.Event{
			Kind:      provider.EventTextDelta,
			Text:      event.Delta,
			TextPhase: s.textPhase(event.OutputIndex),
		}, true, nil
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		return provider.Event{Kind: provider.EventThinkingDelta, Thinking: event.Delta}, true, nil
	case "response.function_call_arguments.delta":
		state := s.outputState(event.OutputIndex)
		state.hadArgumentDeltas = true
		index := event.OutputIndex
		return provider.Event{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				Index:          &index,
				ID:             state.callID,
				Name:           state.name,
				ArgumentsDelta: event.Delta,
			},
		}, true, nil
	case "response.output_item.done":
		state := s.outputState(event.OutputIndex)
		hadToolCallEvent := state.hadToolCallEvent
		result := s.outputItemDone(event.OutputIndex, event.Item)
		state = s.outputState(event.OutputIndex)
		emit := event.Item.Type == "function_call" && !state.hadArgumentDeltas
		emit = emit || (isResponsesBuiltInTool(event.Item.Type) && !hadToolCallEvent)
		return result, emit, nil
	case "response.completed":
		return s.completed(event.Response)
	case "response.incomplete":
		return s.incomplete(event.Response)
	case "response.failed":
		s.finished = true
		return provider.Event{Kind: provider.EventError, Err: responsesError(event.Response.Error)}, true, nil
	case "error":
		s.finished = true
		apiError := event.Error
		if apiError == nil {
			apiError = &responsesAPIError{Code: event.Code, Message: event.Message}
		}
		return provider.Event{Kind: provider.EventError, Err: responsesError(apiError)}, true, nil
	}
	return provider.Event{}, false, nil
}

func (s *responsesStream) outputItemDone(index int, item responsesOutputItem) provider.Event {
	state := s.outputState(index)
	hadArgumentDeltas := state.hadArgumentDeltas
	s.recordOutputItem(index, item)
	if item.Type == "function_call" {
		if hadArgumentDeltas {
			return provider.Event{}
		}
		indexCopy := index
		return provider.Event{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				Index:          &indexCopy,
				ID:             state.callID,
				Name:           state.name,
				ArgumentsDelta: item.Arguments,
			},
		}
	}
	if isResponsesBuiltInTool(item.Type) {
		return s.builtInToolDelta(index, item)
	}
	return provider.Event{}
}

func (s *responsesStream) builtInToolDelta(index int, item responsesOutputItem) provider.Event {
	state := s.outputState(index)
	state.hadToolCallEvent = true
	indexCopy := index
	return provider.Event{
		Kind: provider.EventToolCallDelta,
		ToolCallDelta: &provider.ToolCallDelta{
			Index:          &indexCopy,
			ID:             item.ID,
			Name:           item.Type,
			ArgumentsDelta: responsesBuiltInToolArguments(item),
		},
	}
}

func isResponsesBuiltInTool(toolType string) bool {
	switch toolType {
	case responsesWebSearchCall, responsesCodeInterpreter, responsesFileSearchCall:
		return true
	default:
		return false
	}
}

func responsesBuiltInToolArguments(item responsesOutputItem) string {
	raw, err := json.Marshal(item)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (s *responsesStream) completed(response responsesResponse) (provider.Event, bool, error) {
	providerState, output, err := responsesOutput(response.Output)
	if err != nil {
		return provider.Event{}, false, err
	}
	stopReason := provider.StopReasonComplete
	for _, item := range output {
		if item.Type == "function_call" || isResponsesBuiltInTool(item.Type) {
			stopReason = provider.StopReasonToolUse
			break
		}
	}
	s.reportContextUsage(response.Usage.InputTokens)
	s.finished = true
	return responsesDoneEvent(response, stopReason, providerState), true, nil
}

func (s *responsesStream) incomplete(response responsesResponse) (provider.Event, bool, error) {
	providerState, _, err := responsesOutput(response.Output)
	if err != nil {
		return provider.Event{}, false, err
	}
	stopReason := provider.StopReasonUnknown
	if response.IncompleteDetails != nil {
		switch response.IncompleteDetails.Reason {
		case "max_output_tokens":
			stopReason = provider.StopReasonLength
		case "content_filter":
			stopReason = provider.StopReasonContentFilter
		}
	}
	s.reportContextUsage(response.Usage.InputTokens)
	s.finished = true
	return responsesDoneEvent(response, stopReason, providerState), true, nil
}

func (s *responsesStream) reportContextUsage(inputTokens int) {
	if s.contextUsage == nil || s.contextUsageReported {
		return
	}
	s.contextUsageReported = true
	s.contextUsage(provider.ContextUsage{UsedTokens: inputTokens})
}

func responsesOutput(raw json.RawMessage) (json.RawMessage, []responsesOutputItem, error) {
	if len(raw) == 0 {
		return nil, nil, fmt.Errorf("openai responses terminal event omitted output")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil, nil, fmt.Errorf("openai responses terminal output must be a JSON array")
	}
	var output []responsesOutputItem
	if err := json.Unmarshal(trimmed, &output); err != nil {
		return nil, nil, fmt.Errorf("decode openai responses terminal output: %w", err)
	}
	providerState := append(json.RawMessage(nil), trimmed...)
	return providerState, output, nil
}

func responsesDoneEvent(response responsesResponse, stopReason provider.StopReason, state json.RawMessage) provider.Event {
	usage := response.Usage
	cachedTokens, cacheReported := reportedToken(usage.InputTokensDetails.CachedTokens)
	cacheWriteTokens, cacheWriteReported := reportedToken(usage.InputTokensDetails.CacheWriteTokens)
	reasoningTokens, _ := reportedToken(usage.OutputTokensDetails.ReasoningTokens)
	return provider.Event{
		Kind: provider.EventDone,
		Usage: provider.Usage{
			InputTokens:                   usage.InputTokens,
			CachedInputTokens:             cachedTokens,
			CachedInputTokensReported:     cacheReported,
			CacheWriteInputTokens:         cacheWriteTokens,
			CacheWriteInputTokensReported: cacheWriteReported,
			OutputTokens:                  usage.OutputTokens,
			ReasoningTokens:               reasoningTokens,
			TotalTokens:                   usage.TotalTokens,
		},
		StopReason:    stopReason,
		ProviderState: state,
		Response:      provider.ResponseMetadata{ID: response.ID, Model: response.Model},
	}
}

func reportedToken(value *int) (int, bool) {
	if value == nil {
		return 0, false
	}
	return max(0, *value), true
}

func responsesError(apiError *responsesAPIError) error {
	if apiError == nil {
		return &provider.Error{Provider: "openai", Kind: provider.ErrorUnknown, Message: "responses API failed"}
	}
	return &provider.Error{
		Provider: "openai",
		Kind:     openAIErrorKind(apiError.Type, apiError.Code),
		Code:     apiError.Code,
		Message:  apiError.Message,
	}
}

func openAIErrorKind(errorType, code string) provider.ErrorKind {
	switch {
	case code == "rate_limit_exceeded" || errorType == "rate_limit_error":
		return provider.ErrorRateLimit
	case code == "server_error" || code == "server_is_overloaded" || code == "model_error" ||
		errorType == "api_error" || errorType == "overloaded_error":
		return provider.ErrorServer
	case code == "stream_error":
		return provider.ErrorStream
	case code == "invalid_request_error" || code == "invalid_request" ||
		code == "unsupported_parameter" || errorType == "invalid_request_error":
		return provider.ErrorInvalidRequest
	case code == "invalid_api_key" || errorType == "authentication_error":
		return provider.ErrorAuthentication
	case errorType == "permission_error":
		return provider.ErrorPermission
	case errorType == "not_found_error":
		return provider.ErrorNotFound
	default:
		return provider.ErrorUnknown
	}
}

func (s *responsesStream) recordOutputItem(index int, item responsesOutputItem) {
	state := s.outputState(index)
	state.phase = normalizeTextPhase(item.Phase)
	if item.CallID != "" {
		state.callID = item.CallID
	}
	if item.Name != "" {
		state.name = item.Name
	}
}

func (s *responsesStream) outputState(index int) *responsesOutputState {
	state, ok := s.items[index]
	if !ok {
		state = &responsesOutputState{}
		s.items[index] = state
	}
	return state
}

func (s *responsesStream) textPhase(index int) provider.TextPhase {
	return s.outputState(index).phase
}

func normalizeTextPhase(phase provider.TextPhase) provider.TextPhase {
	switch phase {
	case provider.TextPhaseCommentary, provider.TextPhaseFinalAnswer:
		return phase
	default:
		return ""
	}
}

func (s *responsesStream) Close() error {
	return s.body.Close()
}
