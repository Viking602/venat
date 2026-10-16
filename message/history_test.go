package message

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestValidateCompleteTurns(t *testing.T) {
	tests := []struct {
		name       string
		messages   []Message
		wantErr    bool
		wantIndex  int
		wantReason string
	}{
		{
			name: "empty history",
		},
		{
			name: "standalone text history",
			messages: []Message{
				{Role: RoleSystem, Text: "system"},
				{Role: RoleUser, Text: "user"},
				{Role: RoleAssistant, Text: "assistant"},
				{Role: RoleCustom, Kind: KindCustom, Text: "custom"},
				{Role: RoleSystem, Kind: KindCompactionSummary, Text: "summary"},
			},
		},
		{
			name: "reasoning fields remain part of assistant turn",
			messages: []Message{
				{
					Role:              RoleAssistant,
					Thinking:          "reasoning",
					ThinkingSignature: "signature",
					RedactedThinking:  "redacted",
					ToolCalls:         []ToolCall{{ID: "call-1", Name: "lookup"}},
				},
				toolResultMessage("call-1", "lookup"),
			},
		},
		{
			name: "one tool call",
			messages: []Message{
				assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"}),
				toolResultMessage("call-1", "lookup"),
			},
		},
		{
			name: "multiple tool calls",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-1", "first"),
				toolResultMessage("call-2", "second"),
			},
		},
		{
			name: "out of order results match by ID not name",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-2", "unrelated-name"),
				toolResultMessage("call-1", "another-name"),
			},
		},
		{
			name: "empty result ID consumes first unmatched call",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("", "second"),
				toolResultMessage("call-2", "first"),
			},
		},
		{
			name: "non-empty result ID falls back to empty call ID",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("driver-generated-id", "different-name"),
				toolResultMessage("call-2", "also-different"),
			},
		},
		{
			name: "multiple empty call IDs use positional fallback",
			messages: []Message{
				assistantToolMessage(
					ToolCall{Name: "first"},
					ToolCall{Name: "second"},
				),
				toolResultMessage("", "second"),
				toolResultMessage("some-id", "first"),
			},
		},
		{
			name: "duplicate non-empty call IDs",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "duplicate", Name: "first"},
					ToolCall{ID: "duplicate", Name: "second"},
				),
				toolResultMessage("duplicate", "first"),
				toolResultMessage("duplicate", "second"),
			},
			wantErr:    true,
			wantIndex:  0,
			wantReason: "duplicate tool call ID",
		},
		{
			name: "duplicate result",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-1", "first"),
				toolResultMessage("call-1", "first"),
			},
			wantErr:    true,
			wantIndex:  2,
			wantReason: "duplicate or unmatched tool result",
		},
		{
			name: "duplicate known result ID does not consume empty call ID",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-2", "second"),
				toolResultMessage("call-2", "second"),
			},
			wantErr:    true,
			wantIndex:  2,
			wantReason: "duplicate or unmatched tool result",
		},
		{
			name: "missing result before non-tool message",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-1", "first"),
				{Role: RoleUser, Text: "interrupt"},
			},
			wantErr:    true,
			wantIndex:  2,
			wantReason: "non-tool message",
		},
		{
			name: "incomplete suffix",
			messages: []Message{
				assistantToolMessage(
					ToolCall{ID: "call-1", Name: "first"},
					ToolCall{ID: "call-2", Name: "second"},
				),
				toolResultMessage("call-1", "first"),
			},
			wantErr:    true,
			wantIndex:  0,
			wantReason: "incomplete suffix",
		},
		{
			name:       "orphan result",
			messages:   []Message{toolResultMessage("call-1", "lookup")},
			wantErr:    true,
			wantIndex:  0,
			wantReason: "orphan tool result",
		},
		{
			name: "extra result",
			messages: []Message{
				assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"}),
				toolResultMessage("call-1", "lookup"),
				toolResultMessage("call-1", "lookup"),
			},
			wantErr:    true,
			wantIndex:  2,
			wantReason: "orphan tool result",
		},
		{
			name: "tool calls on non-assistant message",
			messages: []Message{
				{Role: RoleUser, ToolCalls: []ToolCall{{ID: "call-1", Name: "lookup"}}},
			},
			wantErr:    true,
			wantIndex:  0,
			wantReason: "tool calls require the assistant role",
		},
		{
			name: "tool result on non-tool message",
			messages: []Message{
				{Role: RoleAssistant, ToolResult: &ToolResult{ToolCallID: "call-1"}},
			},
			wantErr:    true,
			wantIndex:  0,
			wantReason: "tool results require the tool role",
		},
		{
			name: "tool message without result",
			messages: []Message{
				assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"}),
				{Role: RoleTool},
			},
			wantErr:    true,
			wantIndex:  1,
			wantReason: "tool message has no result",
		},
		{
			name: "tool message carrying calls",
			messages: []Message{
				assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"}),
				{
					Role:       RoleTool,
					ToolCalls:  []ToolCall{{ID: "nested", Name: "invalid"}},
					ToolResult: &ToolResult{ToolCallID: "call-1"},
				},
			},
			wantErr:    true,
			wantIndex:  1,
			wantReason: "tool calls require the assistant role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCompleteTurns(tt.messages)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateCompleteTurns() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrIncompleteToolTurn) {
				t.Fatalf("ValidateCompleteTurns() error = %v, want ErrIncompleteToolTurn", err)
			}
			if want := "message " + strconv.Itoa(tt.wantIndex); !strings.Contains(err.Error(), want) {
				t.Errorf("ValidateCompleteTurns() error = %q, want index marker %q", err, want)
			}
			if !strings.Contains(err.Error(), tt.wantReason) {
				t.Errorf("ValidateCompleteTurns() error = %q, want reason containing %q", err, tt.wantReason)
			}
		})
	}
}

func TestCachePrefixBoundary(t *testing.T) {
	stable := NewText(RoleSystem, "stable")
	stable.CacheBoundary = true
	toolTurn := assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"})
	toolTurn.Text = "checking"
	toolTurn.CacheBoundary = true

	tests := []struct {
		name     string
		messages []Message
		want     int
	}{
		{name: "no marker", messages: []Message{NewText(RoleUser, "task")}},
		{
			name:     "last marker wins",
			messages: []Message{stable, NewText(RoleUser, "task"), {Role: RoleAssistant, Text: "answer", CacheBoundary: true}},
			want:     3,
		},
		{
			name:     "tool exchange remains atomic",
			messages: []Message{NewText(RoleSystem, "system"), toolTurn, toolResultMessage("call-1", "lookup"), NewText(RoleUser, "next")},
			want:     3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CachePrefixBoundary(test.messages)
			if err != nil {
				t.Fatalf("CachePrefixBoundary() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("CachePrefixBoundary() = %d, want %d", got, test.want)
			}
		})
	}

	if _, err := CachePrefixBoundary([]Message{assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"})}); err == nil {
		t.Fatal("CachePrefixBoundary() accepted an incomplete tool turn")
	}
}

func TestCompleteTurnBoundary(t *testing.T) {
	olderGroupHistory := []Message{
		{Role: RoleSystem, Text: "system"},
		assistantToolMessage(
			ToolCall{ID: "call-1", Name: "first"},
			ToolCall{ID: "call-2", Name: "second"},
		),
		toolResultMessage("call-1", "first"),
		toolResultMessage("call-2", "second"),
		{Role: RoleUser, Text: "newer"},
		{Role: RoleAssistant, Text: "newer response"},
	}
	newestGroupHistory := []Message{
		{Role: RoleSystem, Text: "system"},
		{Role: RoleUser, Text: "request"},
		assistantToolMessage(
			ToolCall{ID: "call-1", Name: "first"},
			ToolCall{ID: "call-2", Name: "second"},
		),
		toolResultMessage("call-1", "first"),
		toolResultMessage("call-2", "second"),
	}

	tests := []struct {
		name      string
		messages  []Message
		preferred int
		want      int
		wantErr   bool
	}{
		{name: "empty history clamps negative cut", preferred: -3, want: 0},
		{name: "empty history clamps oversized cut", preferred: 3, want: 0},
		{
			name:      "text history preserves safe cut",
			messages:  []Message{{Role: RoleSystem}, {Role: RoleUser}, {Role: RoleAssistant}},
			preferred: 2,
			want:      2,
		},
		{
			name:      "safe cut at tool group start",
			messages:  olderGroupHistory,
			preferred: 1,
			want:      1,
		},
		{
			name:      "safe cut at tool group end",
			messages:  olderGroupHistory,
			preferred: 4,
			want:      4,
		},
		{
			name:      "cut moves right past older group after assistant",
			messages:  olderGroupHistory,
			preferred: 2,
			want:      4,
		},
		{
			name:      "cut moves right past older group between results",
			messages:  olderGroupHistory,
			preferred: 3,
			want:      4,
		},
		{
			name:      "cut moves left to retain newest group",
			messages:  newestGroupHistory,
			preferred: 4,
			want:      2,
		},
		{
			name:      "cut after newest assistant moves left",
			messages:  newestGroupHistory,
			preferred: 3,
			want:      2,
		},
		{
			name: "invalid history is rejected before clamping",
			messages: []Message{
				assistantToolMessage(ToolCall{ID: "call-1", Name: "lookup"}),
			},
			preferred: -1,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompleteTurnBoundary(tt.messages, tt.preferred)
			if tt.wantErr {
				if !errors.Is(err, ErrIncompleteToolTurn) {
					t.Fatalf("CompleteTurnBoundary() error = %v, want ErrIncompleteToolTurn", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CompleteTurnBoundary() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("CompleteTurnBoundary() = %d, want %d", got, tt.want)
			}
		})
	}
}

func assistantToolMessage(calls ...ToolCall) Message {
	return Message{Role: RoleAssistant, ToolCalls: calls}
}

func toolResultMessage(callID, name string) Message {
	return Message{
		Role:       RoleTool,
		ToolResult: &ToolResult{ToolCallID: callID, Name: name},
	}
}
