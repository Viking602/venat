package message

import (
	"errors"
	"fmt"
)

// ErrIncompleteToolTurn indicates that a message history contains a malformed
// or incomplete assistant tool-call exchange.
var ErrIncompleteToolTurn = errors.New("message: incomplete tool turn")

// ValidateCompleteTurns verifies that every assistant tool-call message is
// immediately followed by exactly one matching tool result per call.
func ValidateCompleteTurns(messages []Message) error {
	for index := 0; index < len(messages); {
		msg := messages[index]
		if len(msg.ToolCalls) > 0 && msg.Role != RoleAssistant {
			return incompleteToolTurn(index, "tool calls require the assistant role")
		}
		if msg.ToolResult != nil && msg.Role != RoleTool {
			return incompleteToolTurn(index, "tool results require the tool role")
		}
		if msg.Role == RoleTool {
			if msg.ToolResult == nil {
				return incompleteToolTurn(index, "tool message has no result")
			}
			return incompleteToolTurn(index, "orphan tool result")
		}
		if len(msg.ToolCalls) == 0 {
			index++
			continue
		}

		callIndexes := make(map[string]int, len(msg.ToolCalls))
		for callIndex, call := range msg.ToolCalls {
			if call.ID == "" {
				continue
			}
			if _, exists := callIndexes[call.ID]; exists {
				return incompleteToolTurn(index, fmt.Sprintf("duplicate tool call ID %q", call.ID))
			}
			callIndexes[call.ID] = callIndex
		}

		matched := make([]bool, len(msg.ToolCalls))
		for resultOffset := range msg.ToolCalls {
			resultIndex := index + resultOffset + 1
			if resultIndex >= len(messages) {
				return incompleteToolTurn(index, "tool-call group has an incomplete suffix")
			}

			resultMessage := messages[resultIndex]
			if len(resultMessage.ToolCalls) > 0 {
				return incompleteToolTurn(resultIndex, "tool calls require the assistant role")
			}
			if resultMessage.Role != RoleTool {
				return incompleteToolTurn(resultIndex, "non-tool message before tool-call group completed")
			}
			if resultMessage.ToolResult == nil {
				return incompleteToolTurn(resultIndex, "tool message has no result")
			}

			callIndex := unmatchedCallIndex(msg.ToolCalls, matched, callIndexes, resultMessage.ToolResult.ToolCallID)
			if callIndex < 0 {
				return incompleteToolTurn(resultIndex, "duplicate or unmatched tool result")
			}
			matched[callIndex] = true
		}

		index += len(msg.ToolCalls) + 1
	}
	return nil
}

// CompleteTurnBoundary returns a valid cut point near preferredStart without
// splitting an assistant tool-call exchange. It validates the full history
// before choosing the boundary.
func CompleteTurnBoundary(messages []Message, preferredStart int) (start int, err error) {
	if err := ValidateCompleteTurns(messages); err != nil {
		return 0, err
	}

	start = preferredStart
	if start < 0 {
		start = 0
	} else if start > len(messages) {
		start = len(messages)
	}

	for index := 0; index < len(messages); {
		callCount := len(messages[index].ToolCalls)
		if callCount == 0 {
			index++
			continue
		}

		groupEnd := index + callCount + 1
		if start > index && start < groupEnd {
			if groupEnd < len(messages) {
				return groupEnd, nil
			}
			return index, nil
		}
		index = groupEnd
	}
	return start, nil
}

// CachePrefixBoundary returns the number of leading messages protected by the
// last CacheBoundary marker. If the marker falls inside a tool exchange, the
// whole exchange is protected. The full history is validated first.
func CachePrefixBoundary(messages []Message) (int, error) {
	if err := ValidateCompleteTurns(messages); err != nil {
		return 0, err
	}
	boundary := 0
	for index, msg := range messages {
		if msg.CacheBoundary {
			boundary = index + 1
		}
	}
	if boundary == 0 {
		return 0, nil
	}
	for index := 0; index < len(messages); {
		callCount := len(messages[index].ToolCalls)
		if callCount == 0 {
			index++
			continue
		}
		groupEnd := index + callCount + 1
		if boundary > index && boundary < groupEnd {
			return groupEnd, nil
		}
		index = groupEnd
	}
	return boundary, nil
}

func unmatchedCallIndex(calls []ToolCall, matched []bool, callIndexes map[string]int, resultID string) int {
	if resultID != "" {
		if index, exists := callIndexes[resultID]; exists && !matched[index] {
			return index
		}
		for index, call := range calls {
			if !matched[index] && call.ID == "" {
				return index
			}
		}
		return -1
	}

	for index := range calls {
		if !matched[index] {
			return index
		}
	}
	return -1
}

func incompleteToolTurn(index int, reason string) error {
	return fmt.Errorf("%w: message %d: %s", ErrIncompleteToolTurn, index, reason)
}

// ContextView returns the visible model context without deleting transcript
// evidence. Message values are borrowed; use CloneMessages before mutating them.
// Histories without archived entries reuse the original slice.
func ContextView(history []Message) []Message {
	visible := 0
	for _, current := range history {
		if !current.ContextArchived {
			visible++
		}
	}
	if visible == len(history) {
		return history
	}
	view := make([]Message, 0, visible)
	for _, current := range history {
		if !current.ContextArchived {
			view = append(view, current)
		}
	}
	return view
}
