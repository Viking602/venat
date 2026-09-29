// This credential-free example stores an ordinary tool result, sends only a
// bounded preview onward, then uses the registered read tool to recover omitted bytes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Viking602/venat/tool"
)

type reportTool struct{}

func (reportTool) Definition() tool.Definition {
	return tool.Definition{Name: "report", Description: "Produce a long local report.", InputSchema: tool.Schema{Type: "object", AdditionalProperties: boolPtr(false)}}
}

func (reportTool) Execute(context.Context, tool.Call, tool.UpdateSink) (tool.Result, error) {
	return tool.Result{Content: "header\n" + strings.Repeat("omitted-middle-line\n", 20) + "footer"}, nil
}

func boolPtr(value bool) *bool { return &value }

func main() {
	root, err := os.MkdirTemp("", "venat-output-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	store, err := tool.NewOutputStore(root, tool.WithPreviewThreshold(72), tool.WithReadLimit(256))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	bus := tool.NewBus(append([]tool.Driver{reportTool{}}, store.Tools()...)...)
	original, err := bus.Execute(context.Background(), tool.Call{ID: "report-1", Name: "report", Arguments: json.RawMessage(`{}`)}, tool.ExecuteOptions{})
	if err != nil {
		panic(err)
	}
	processed, err := store.Process(context.Background(), original)
	if err != nil {
		panic(err)
	}
	refStart := strings.Index(processed.Content, "artifact://")
	if refStart < 0 {
		panic("preview did not include an artifact reference")
	}
	ref := processed.Content[refStart : refStart+len("artifact://")+64]
	read, err := bus.Execute(context.Background(), tool.Call{ID: "read-1", Name: "tool_output_read", Arguments: mustJSON(map[string]any{
		"reference": ref, "offset": 7, "limit": 96,
	})}, tool.ExecuteOptions{})
	if err != nil {
		panic(err)
	}
	if read.IsError || !strings.Contains(read.Content, "omitted-middle-line") {
		panic(fmt.Sprintf("read failed: %#v", read))
	}
	fmt.Printf("preview=%s\nrecovered=%s", processed.Content, read.Content)
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
