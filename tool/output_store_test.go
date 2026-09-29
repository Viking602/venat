package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
)

func TestOutputStore_ProcessAndReadRecoversOmittedMiddle(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(48), WithReadLimit(128))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := "BEGIN-" + strings.Repeat("middle-", 20) + "-END"
	input := Result{ToolCallID: "call-7", Name: "lookup", Content: body, IsError: true}
	processed, err := store.Process(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if processed.ToolCallID != input.ToolCallID || processed.Name != input.Name || !processed.IsError {
		t.Fatalf("identity/error changed: %#v", processed)
	}
	if processed.Content == body || !strings.Contains(processed.Content, "artifact://") {
		t.Fatalf("expected bounded preview with reference: %q", processed.Content)
	}
	refStart := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[refStart : refStart+len("artifact://")+64]
	bus := NewBus(store.Tools()...)
	read, err := bus.Execute(context.Background(), Call{ID: "read-1", Name: "tool_output_read", Arguments: json.RawMessage(`{"reference":"` + ref + `","offset":6,"limit":32}`)}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if read.IsError || read.ToolCallID != "read-1" || read.Name != "tool_output_read" || !strings.Contains(read.Content, "middle-") {
		t.Fatalf("read omitted content = %#v", read)
	}
}

func TestOutputStore_InvalidReferenceDenied(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bus := NewBus(store.Tools()...)
	result, err := bus.Execute(context.Background(), Call{ID: "bad", Name: "tool_output_read", Arguments: json.RawMessage(`{"reference":"artifact://../../etc/passwd","offset":0,"limit":10}`)}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError || !strings.Contains(result.Content, "invalid artifact reference") {
		t.Fatalf("invalid reference result = %#v", result)
	}
}

func TestOutputStore_StructuredAndMediaPreserved(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(24))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	media := message.ContentPart{Kind: message.ContentImage, Data: []byte{1, 2, 3}, MediaType: "image/png"}
	input := Result{ToolCallID: "structured", Name: "inspect", Parts: []message.ContentPart{media}, Structured: json.RawMessage(`{"items":"` + strings.Repeat("x", 80) + `"}`)}
	processed, err := store.Process(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(processed.Parts) != 2 || processed.Parts[0].Kind != message.ContentImage || string(processed.Parts[0].Data) != string(media.Data) {
		t.Fatalf("media changed: %#v", processed.Parts)
	}
	if processed.Parts[1].Kind != message.ContentText || !strings.Contains(processed.Content, "artifact://") || processed.Structured != nil {
		t.Fatalf("structured artifact missing: %#v", processed)
	}
}

func TestOutputStore_DeduplicatesBodies(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts")
	store, err := NewOutputStore(root, WithPreviewThreshold(8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	input := Result{Content: strings.Repeat("same", 20)}
	first, err := store.Process(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Process(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !strings.Contains(first.Content, "artifact://") {
		t.Fatalf("dedup entries=%d preview=%q", len(entries), first.Content)
	}
}

func TestOutputStore_ProcessStorageFailureHasNoReference(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.root.Close(); err != nil {
		t.Fatal(err)
	}
	input := Result{Content: strings.Repeat("x", 20)}
	processed, processErr := store.Process(context.Background(), input)
	if processErr == nil {
		t.Fatalf("expected honest storage failure, result=%#v", processed)
	}
	if strings.Contains(processed.Content, "artifact://") || processed.Content != input.Content {
		t.Fatalf("failure exposed misleading reference: %#v", processed)
	}
}

func TestOutputStore_SearchesLargeArtifact(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(8), WithSearchLimit(512))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := strings.Repeat("filler\n", 20000) + "needle-middle\n"
	processed, err := store.Process(context.Background(), Result{Content: body})
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[start : start+len("artifact://")+64]
	bus := NewBus(store.Tools()...)
	args := json.RawMessage(`{"reference":"` + ref + `","query":"needle-middle"}`)
	result, err := bus.Execute(context.Background(), Call{ID: "search", Name: "tool_output_search", Arguments: args}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || !strings.Contains(result.Content, "needle-middle") {
		t.Fatalf("search=%#v", result)
	}
}

func TestOutputStore_RejectsCorruptArtifact(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts")
	store, err := NewOutputStore(root, WithPreviewThreshold(8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := strings.Repeat("same", 20)
	processed, err := store.Process(context.Background(), Result{Content: body})
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[start : start+len("artifact://")+64]
	name := strings.TrimPrefix(ref, "artifact://")
	if err := os.WriteFile(filepath.Join(root, name), []byte(strings.Repeat("x", len(body))), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Process(context.Background(), Result{Content: body}); err == nil {
		t.Fatal("corrupt artifact reused")
	}
}

func TestOutputStore_RejectsSymlinkArtifact(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts")
	store, err := NewOutputStore(root, WithPreviewThreshold(8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := strings.Repeat("safe", 20)
	processed, err := store.Process(context.Background(), Result{Content: body})
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[start : start+len("artifact://")+64]
	name := strings.TrimPrefix(ref, "artifact://")
	if err := os.Remove(filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(target, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	bus := NewBus(store.Tools()...)
	result, err := bus.Execute(context.Background(), Call{ID: "symlink", Name: "tool_output_read", Arguments: json.RawMessage("{\"reference\":\"" + ref + "\",\"offset\":0,\"limit\":10}")}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("symlink artifact was read: %#v", result)
	}
}

func TestOutputStore_SearchesLongLineAndReadOffset(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(8), WithSearchLimit(512))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	prefix := strings.Repeat("prefix", 20000)
	body := prefix + "needle" + strings.Repeat("suffix", 20000)
	processed, err := store.Process(context.Background(), Result{Content: body})
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[start : start+len("artifact://")+64]
	bus := NewBus(store.Tools()...)
	args := json.RawMessage(`{"reference":"` + ref + `","query":"needle","context":4}`)
	result, err := bus.Execute(context.Background(), Call{ID: "search-long", Name: "tool_output_search", Arguments: args}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || !strings.Contains(result.Content, "needle") {
		t.Fatalf("search=%#v", result)
	}
	marker := strings.TrimPrefix(strings.SplitN(result.Content, ":", 2)[0], "offset=")
	offset, err := strconv.ParseInt(marker, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	readArgs := json.RawMessage(`{"reference":"` + ref + `","offset":` + strconv.FormatInt(offset, 10) + `,"limit":6}`)
	read, err := bus.Execute(context.Background(), Call{ID: "read-long", Name: "tool_output_read", Arguments: readArgs}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if read.IsError || !strings.Contains(read.Content, "needle") {
		t.Fatalf("offset read=%#v", read)
	}
}

func TestOutputStore_AggregatesTextFragmentsWithoutInsertedBytes(t *testing.T) {
	store, err := NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), WithPreviewThreshold(5))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	input := Result{Parts: []message.ContentPart{message.TextPart("abc"), message.CommentaryPart("def"), {Kind: message.ContentImage, Data: []byte{1}}, message.FinalAnswerPart("ghi")}}
	processed, err := store.Process(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	ref := processed.Content[start : start+len("artifact://")+64]
	data, _, err := store.read(ref, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdefghi" {
		t.Fatalf("stored fragments=%q", data)
	}
	if len(processed.Parts) != 2 || processed.Parts[1].Kind != message.ContentImage {
		t.Fatalf("media/fragments=%#v", processed.Parts)
	}
}
