package agent

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

const responseRecoveryKey = "venat.response_recovery"
const maxResponseRecoveries = 3

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
	metadata := make(map[string]string, len(assistant.Metadata)+1)
	for key, value := range assistant.Metadata {
		metadata[key] = value
	}
	metadata[responseRecoveryKey] = reason
	assistant.Metadata = metadata
	if strings.TrimSpace(assistant.Text) == "" {
		assistant.Content = append(assistant.Content, message.TextPart("[The model returned no usable final output. No tool calls from this response were executed.]"))
		assistant.SyncLegacyContent()
	}
}

func responseRecovery(steps []Step, assistant message.Message) (message.Message, error) {
	reason := assistant.Metadata[responseRecoveryKey]
	if reason == "" {
		return message.Message{}, nil
	}
	attempts := responseRecoveryCount(steps)
	if attempts >= maxResponseRecoveries {
		return message.Message{}, fmt.Errorf("%w after %d corrections: %s", errIncompleteResponse, attempts, reason)
	}
	instruction := "The previous response was incomplete. Continue the task and provide a usable final answer."
	if reason == "invalid_tool_arguments" {
		instruction = "The previous tool arguments were not valid JSON. No tools from that response were executed. Correct the arguments and issue complete tool calls with valid JSON."
	} else if reason == "output_length" {
		instruction = "The previous response reached the output limit. No tools from that response were executed. Continue with a shorter complete answer or complete tool calls."
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
