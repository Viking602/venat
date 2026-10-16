package tool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Viking602/venat/message"
)

func TestBus_RejectsOversizedNamesBeforeHintsOrExecution(t *testing.T) {
	driver := &safetyDriver{name: "safe"}
	bus := NewBus(driver)
	name := strings.Repeat("x", 1<<20)
	result, err := bus.Execute(context.Background(), Call{ID: "call", Name: name}, ExecuteOptions{})
	if err != nil || !result.IsError || result.ToolCallID != "call" || result.Name != name || driver.calls != 0 {
		t.Fatalf("oversized rejection failed: error=%v calls=%d", err, driver.calls)
	}
	if len(result.Content) > 512 || strings.Contains(result.Content, "available:") {
		t.Fatal("oversized name entered diagnostic ranking or echoed unbounded text")
	}
	if err := NewBus().Register(&safetyDriver{name: strings.Repeat("a", message.MaxToolNameBytes+1)}); !errors.Is(err, ErrInvalidToolDefinition) {
		t.Fatalf("Register() = %v", err)
	}
	boundary := &safetyDriver{name: strings.Repeat("a", message.MaxToolNameBytes)}
	if _, err := NewBus(boundary).Execute(context.Background(), Call{Name: boundary.name}, ExecuteOptions{}); err != nil || boundary.calls != 1 {
		t.Fatalf("boundary call = %v, calls=%d", err, boundary.calls)
	}
}

func TestBus_RejectedOversizedNameKeepsValidUTF8(t *testing.T) {
	// "é" is two bytes; an odd prefix makes the byte limit fall inside a rune.
	name := "a" + strings.Repeat("é", message.MaxToolNameBytes)
	result, err := NewBus().Execute(context.Background(), Call{ID: "call", Name: name}, ExecuteOptions{})
	if err != nil || !result.IsError {
		t.Fatalf("Execute() result=%#v error=%v, want rejected result", result, err)
	}
	if !utf8.ValidString(result.Content) {
		t.Fatalf("rejection content is not valid UTF-8: %q", result.Content[:64])
	}
}
