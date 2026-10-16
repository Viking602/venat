package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

const (
	responseRecoveryKey       = "venat.response_recovery"
	maxResponseRecoveries     = 3
	maxToolCallCandidateBytes = 64 << 10
	maxToolCallCandidates     = 16
	maxToolCallEnvelopeDepth  = 3
)

var toolCallJSONLimits = message.JSONLimits{
	Bytes:      maxToolCallCandidateBytes,
	Depth:      16,
	Values:     512,
	Collection: 128,
}

var errIncompleteResponse = errors.New("agent response remained incomplete")

// Only completed model responses qualify. Stream failures and ambiguous effects
// never enter this path. Step observations bound retries across compaction and resume.
func markIncompleteResponse(assistant *message.Message, stop provider.StopReason, normalizationErr error) {
	reason := ""
	switch {
	case normalizationErr != nil:
		reason = "invalid_tool_arguments"
	case stop == provider.StopReasonLength:
		reason = "output_length"
	case len(assistant.ToolCalls) == 0 && strings.TrimSpace(assistant.FinalAnswerContent()) == "" &&
		(stop == provider.StopReasonComplete || stop == provider.StopReasonToolUse):
		reason = "empty_output"
	}
	if reason == "" {
		return
	}
	assistant.ToolCalls = nil
	setResponseRecoveryReason(assistant, reason)
	if strings.TrimSpace(assistant.Text) == "" {
		assistant.Content = append(assistant.Content, message.TextPart("[The model returned no usable final output. No tool calls from this response were executed.]"))
		assistant.SyncLegacyContent()
	}
}

func markTextToolCallResponse(assistant *message.Message, stop provider.StopReason, bus *tool.Bus, input LoopInput) {
	if assistant == nil || len(assistant.ToolCalls) > 0 || assistant.Metadata[responseRecoveryKey] != "" {
		return
	}
	if stop != provider.StopReasonComplete && stop != provider.StopReasonToolUse {
		return
	}
	content := assistant.FinalAnswerContent()
	if satisfiesRequestedStructuredOutput(content, input) {
		return
	}
	if !hasPlainTextToolCall(content, bus) {
		return
	}
	setResponseRecoveryReason(assistant, "text_tool_call")
}

func satisfiesRequestedStructuredOutput(text string, input LoopInput) bool {
	if len(input.continuationPolicy.Schema) > 0 {
		if schema, err := parseOutputPolicySchema(input.continuationPolicy.Schema); err == nil {
			if _, err := validateStructuredOutputAgainstSchema(schema, text); err == nil {
				return true
			}
		}
	}
	if input.ResponseFormat == nil {
		return false
	}
	if len(input.ResponseFormat.RawSchema) > 0 {
		if schema, err := parseOutputPolicySchema(input.ResponseFormat.RawSchema); err == nil {
			if _, err := validateStructuredOutputAgainstSchema(schema, text); err == nil {
				return true
			}
		}
	} else if input.ResponseFormat.Schema != nil {
		if raw, err := json.Marshal(input.ResponseFormat.Schema); err == nil {
			if schema, err := parseOutputPolicySchema(raw); err == nil {
				if _, err := validateStructuredOutputAgainstSchema(schema, text); err == nil {
					return true
				}
			}
		}
	} else if input.ResponseFormat.Type == "json_object" {
		trimmed := strings.TrimSpace(text)
		if strings.HasPrefix(trimmed, "{") && json.Valid([]byte(trimmed)) {
			return true
		}
	}
	return false
}

func setResponseRecoveryReason(assistant *message.Message, reason string) {
	metadata := make(map[string]string, len(assistant.Metadata)+1)
	for key, value := range assistant.Metadata {
		metadata[key] = value
	}
	metadata[responseRecoveryKey] = reason
	assistant.Metadata = metadata
}

func responseRecovery(steps []Step, assistant message.Message, continuation string) (message.Message, error) {
	reason := assistant.Metadata[responseRecoveryKey]
	if reason == "" {
		return message.Message{}, nil
	}
	attempts := responseRecoveryCount(steps)
	if attempts >= maxResponseRecoveries {
		return message.Message{}, fmt.Errorf("%w after %d corrections: %s", errIncompleteResponse, attempts, reason)
	}
	instruction := "The previous response was incomplete. Continue the task and provide a complete usable final answer, preserving the requested level of detail. Do not repeat content that was already completed."
	switch reason {
	case "invalid_tool_arguments":
		instruction = "The previous tool arguments were not valid JSON. No tools from that response were executed. Correct the arguments and issue complete tool calls with valid JSON."
	case "text_tool_call":
		instruction = "The previous response wrote a tool call in plain text instead of invoking a tool. No tools from that response were executed. Invoke the required tool using a structured tool call, or provide the final answer without raw tool-call markup."
	case "output_length":
		if strings.TrimSpace(continuation) != "" {
			instruction = continuation
		}
	}
	return message.NewText(message.RoleUser, instruction), nil
}

func responseRecoveryCount(steps []Step) int {
	attempts := 0
	for _, step := range steps {
		for _, observation := range step.Observations {
			if observation.Kind == "response_recovery" {
				attempts++
			}
		}
	}
	return attempts
}

func hasPlainTextToolCall(text string, bus *tool.Bus) bool {
	if bus == nil || len(bus.Definitions()) == 0 {
		return false
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	if hasExplicitToolCallMarkup(trimmed, bus) {
		return true
	}
	for _, candidate := range jsonToolCallCandidates(trimmed) {
		if isJSONToolCallPayload(candidate, bus) {
			return true
		}
	}
	return false
}

func hasExplicitToolCallMarkup(text string, bus *tool.Bus) bool {
	defs := bus.Definitions()
	for _, def := range defs {
		if def.Name == "" {
			continue
		}
		if hasRegisteredFunctionTag(text, def.Name) {
			return true
		}
	}
	for _, token := range []string{
		"<tool_call>",
		"<｜tool▁calls▁begin｜>",
		"<|tool_calls_begin|>",
		"<|tool_call|>",
		"<|python_tag|>",
		"[TOOL_CALLS]",
		"<function_calls>",
	} {
		if hasMarkerInvocationPayload(text, token, defs, bus) {
			return true
		}
	}
	return false
}

func hasRegisteredFunctionTag(text, name string) bool {
	for _, item := range []struct {
		open  string
		close string
	}{
		{open: "<function=" + name + ">", close: "</function>"},
		{open: "<function=" + name + " ", close: "</function>"},
		{open: `<invoke name="` + name + `"`, close: "</invoke>"},
	} {
		remaining := text
		for {
			idx := strings.Index(remaining, item.open)
			if idx < 0 {
				break
			}
			remaining = remaining[idx+len(item.open):]
			if strings.Contains(remaining, item.close) {
				return true
			}
		}
	}
	return false
}

func hasMarkerInvocationPayload(text, marker string, defs []message.ToolDefinition, bus *tool.Bus) bool {
	remaining := text
	for {
		idx := strings.Index(remaining, marker)
		if idx < 0 {
			return false
		}
		rest := remaining[idx+len(marker):]
		for _, candidate := range extractBalancedJSONObjects(rest) {
			if isJSONToolCallPayload(candidate, bus) {
				return true
			}
		}
		trimmedRest := strings.TrimSpace(rest)
		for _, def := range defs {
			if def.Name == "" {
				continue
			}
			if strings.HasPrefix(trimmedRest, def.Name+"(") ||
				strings.Contains(rest, "<｜tool▁sep｜>"+def.Name) ||
				strings.Contains(rest, "<|tool_sep|>"+def.Name) {
				return true
			}
		}
		remaining = rest
	}
}

func jsonToolCallCandidates(text string) []string {
	var candidates []string
	addCandidate := func(candidate string) {
		if candidate == "" || len(candidate) > maxToolCallCandidateBytes || len(candidates) >= maxToolCallCandidates {
			return
		}
		candidates = append(candidates, candidate)
	}
	addCandidate(text)
	remaining := text
	for len(candidates) < maxToolCallCandidates {
		open := strings.Index(remaining, "```")
		if open < 0 {
			break
		}
		remaining = remaining[open+3:]
		closeIdx := strings.Index(remaining, "```")
		if closeIdx < 0 {
			break
		}
		block := remaining[:closeIdx]
		if newline := strings.IndexByte(block, '\n'); newline >= 0 && !strings.ContainsAny(strings.TrimSpace(block[:newline]), "{[") {
			block = block[newline+1:]
		}
		addCandidate(strings.TrimSpace(block))
		remaining = remaining[closeIdx+3:]
	}
	for _, object := range extractBalancedJSONObjects(text) {
		addCandidate(object)
	}
	return candidates
}

func extractBalancedJSONObjects(text string) []string {
	var objects []string
	inString := false
	escaped := false
	depth := 0
	start := -1
	for i := range len(text) {
		ch := text[i]
		if inString {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			if depth > 0 {
				inString = true
			}
		case '{':
			if depth == 0 {
				start = i
			}
			depth++
		case '}':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					if i+1-start <= maxToolCallCandidateBytes {
						objects = append(objects, text[start:i+1])
					}
					start = -1
					if len(objects) >= maxToolCallCandidates {
						return objects
					}
				}
			}
		}
	}
	return objects
}

func isJSONToolCallPayload(candidate string, bus *tool.Bus) bool {
	if len(candidate) < 2 || len(candidate) > maxToolCallCandidateBytes {
		return false
	}
	if candidate[0] != '{' && candidate[0] != '[' {
		return false
	}
	if err := message.CheckJSON([]byte(candidate), toolCallJSONLimits); err != nil {
		return false
	}
	var decoded any
	if err := json.Unmarshal([]byte(candidate), &decoded); err != nil {
		return false
	}
	return isDecodedToolCallValue(decoded, bus, 0)
}

func isDecodedToolCallValue(value any, bus *tool.Bus, depth int) bool {
	if depth > maxToolCallEnvelopeDepth || value == nil {
		return false
	}
	switch typed := value.(type) {
	case map[string]any:
		return isDecodedToolCallObject(typed, bus, depth)
	case []any:
		if len(typed) == 0 {
			return false
		}
		for _, item := range typed {
			obj, ok := item.(map[string]any)
			if !ok || !isDecodedToolCallObject(obj, bus, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isDecodedToolCallObject(obj map[string]any, bus *tool.Bus, depth int) bool {
	if depth > maxToolCallEnvelopeDepth || !hasOnlyToolCallKeys(obj) {
		return false
	}
	if rawCalls, ok := obj["tool_calls"]; ok {
		return isDecodedToolCallValue(rawCalls, bus, depth+1)
	}
	if rawFn, ok := obj["function"]; ok {
		if fnObj, ok := rawFn.(map[string]any); ok {
			if isDecodedToolCallObject(fnObj, bus, depth+1) {
				return true
			}
			if hasOnlyToolCallKeys(fnObj) {
				if name := extractDecodedStringField(fnObj, "name", "tool", "tool_name"); name != "" {
					if _, exists := bus.Driver(name); exists {
						return true
					}
				}
			}
		}
	}
	name := extractDecodedStringField(obj, "name", "tool", "tool_name")
	if name == "" {
		return false
	}
	if _, exists := bus.Driver(name); !exists {
		return false
	}
	for _, indicator := range []string{"arguments", "parameters", "input"} {
		if _, ok := obj[indicator]; ok {
			return true
		}
	}
	switch extractDecodedStringField(obj, "type") {
	case "function", "tool_use", "tool_call":
		return true
	}
	return false
}

func hasOnlyToolCallKeys(obj map[string]any) bool {
	if len(obj) == 0 {
		return false
	}
	for key := range obj {
		switch key {
		case "id", "call_id", "type", "name", "tool", "tool_name",
			"arguments", "parameters", "input", "function", "tool_calls":
		default:
			return false
		}
	}
	return true
}

func extractDecodedStringField(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := obj[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}
