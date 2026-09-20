package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Viking602/venat/message"
)

var errContextLimit = errors.New("agent context cannot fit the configured target")

type contextGroup struct {
	start, end int
	cost       int
	protected  bool
}

// fitContext bounds only the provider-facing view. The execution transcript and
// its step/effect evidence remain intact for checkpoints and recovery. The
// conservative estimate charges one token per serialized byte plus framing;
// it is not a model tokenizer. Media needs an explicit model-aware CompactTo.
// ponytail: byte estimates over-trim; use CompactTo when model token counts matter.
func fitContext(ctx context.Context, history []message.Message, target int) ([]message.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := message.ValidateCompleteTurns(history); err != nil {
		return nil, err
	}
	prefix, err := message.CachePrefixBoundary(history)
	if err != nil {
		return nil, err
	}
	groups := make([]contextGroup, 0, len(history))
	total := 0
	for index := 0; index < len(history); {
		group := contextGroup{start: index, end: index + 1 + len(history[index].ToolCalls)}
		for _, current := range history[group.start:group.end] {
			cost, err := contextMessageCost(current)
			if err != nil {
				return nil, err
			}
			group.cost += cost
			group.protected = group.protected || protectedContextMessage(current)
		}
		group.protected = group.protected || group.start < prefix || group.end == len(history)
		total += group.cost
		groups = append(groups, group)
		index = group.end
	}
	kept := make([]message.Message, 0, len(history))
	for _, group := range groups {
		if !group.protected && total > target {
			total -= group.cost
			continue
		}
		kept = append(kept, history[group.start:group.end]...)
	}
	kept = message.CloneMessages(kept)
	if total > target {
		var trimErr error
		total, trimErr = trimContextToolOutput(kept, total, target)
		if trimErr != nil {
			return nil, trimErr
		}
	}
	if total > target {
		return nil, fmt.Errorf("%w: protected history estimate %d exceeds %d", errContextLimit, total, target)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return kept, nil
}

func contextMessageCost(current message.Message) (int, error) {
	contents := [][]message.ContentPart{current.CanonicalContent()}
	if current.ToolResult != nil {
		contents = append(contents, current.ToolResult.CanonicalContent())
	}
	for _, parts := range contents {
		for _, part := range parts {
			switch part.Kind {
			case message.ContentImage, message.ContentAudio, message.ContentFile:
				return 0, fmt.Errorf("%w: media requires a model-aware CompactTo", errContextLimit)
			}
		}
	}
	// Canonical parts supersede legacy scalar mirrors. Charge each representation
	// once; Structured may differ and remains charged separately.
	current.Content = current.CanonicalContent()
	current.Text, current.Thinking, current.ThinkingSignature, current.RedactedThinking = "", "", "", ""
	if current.ToolResult != nil {
		result := *current.ToolResult
		result.Parts = result.CanonicalContent()
		if strings.TrimSpace(result.TextContent()) == strings.TrimSpace(string(result.Structured)) {
			result.Structured = nil
		}
		result.Content = ""
		current.ToolResult = &result
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return 0, fmt.Errorf("estimate context: %w", err)
	}
	return len(encoded) + 16, nil
}

// Trim only oversized plain tool output after dropping disposable old groups.
// Instructions, signed content, media, cache prefixes and skill bodies stay intact.
func trimContextToolOutput(history []message.Message, total, target int) (int, error) {
	prefix, err := message.CachePrefixBoundary(history)
	if err != nil {
		return total, err
	}
	for index := prefix; index < len(history) && total > target; index++ {
		current := &history[index]
		result := current.ToolResult
		if result == nil || result.Name == activateSkillToolName || len(current.ProviderState) > 0 || len(current.Content) > 0 {
			continue
		}
		parts := result.CanonicalContent()
		plain := true
		for _, part := range parts {
			if part.Kind != message.ContentText {
				plain = false
			}
		}
		if !plain {
			continue
		}
		text := result.TextContent()
		if len(result.Structured) > 0 && strings.TrimSpace(text) != strings.TrimSpace(string(result.Structured)) {
			text += "\nStructured: " + string(result.Structured)
		}
		if len(text) <= 512 {
			continue
		}
		before, err := contextMessageCost(*current)
		if err != nil {
			return total, err
		}
		limit := max(256, before-(total-target))
		original := *result
		set := func(size int) {
			*result = original
			result.Structured = nil
			result.Parts = []message.ContentPart{message.TextPart(contextHeadTail(text, size))}
			result.SyncLegacyContent()
		}
		// Exact serialized-byte comparison also bounds escaped control characters.
		low, high := 128, len(text)-1
		for low < high {
			mid := low + (high-low+1)/2
			set(mid)
			cost, err := contextMessageCost(*current)
			if err != nil {
				return total, err
			}
			if cost <= limit {
				low = mid
			} else {
				high = mid - 1
			}
		}
		set(low)
		after, err := contextMessageCost(*current)
		if err != nil {
			return total, err
		}
		if after >= before {
			*result = original
			continue
		}
		total -= before - after
	}
	return total, nil
}

func contextHeadTail(text string, size int) string {
	head, tail := size/2, len(text)-(size-size/2)
	for head > 0 && !utf8.RuneStart(text[head]) {
		head--
	}
	for tail < len(text) && !utf8.RuneStart(text[tail]) {
		tail++
	}
	return text[:head] + "\n[... tool output omitted from model context; full result retained in transcript ...]\n" + text[tail:]
}

func protectedContextMessage(current message.Message) bool {
	if current.Role == message.RoleSystem || current.Role == message.RoleUser {
		return true
	}
	result := current.ToolResult
	return result != nil && (result.IsError || result.Name == activateSkillToolName)
}
