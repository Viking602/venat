package kit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Viking602/venat/tool"
)

func TestHTTPTool(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	driver := HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{URL: ts.URL}, Description("remote"))
	result, err := driver.Execute(context.Background(), tool.Call{
		ID:        "call-1",
		Name:      "remote",
		Arguments: json.RawMessage(`{"query":"venat"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Content != `{"status":"ok"}` {
		t.Fatalf("unexpected result: %q", result.Content)
	}
}

func TestHTTPToolReturnsOversizedResponseFeedback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write(bytes.Repeat([]byte("x"), 1<<20+1))
	}))
	defer ts.Close()

	driver := HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{URL: ts.URL})
	result, err := driver.Execute(context.Background(), tool.Call{
		ID:        "call-oversized",
		Name:      "remote",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if err != nil || !result.IsError || !strings.Contains(result.Content, "HTTP 200") || !strings.Contains(result.Content, "body truncated") || result.Structured != nil {
		t.Fatalf("expected bounded HTTP feedback: size=%d err=%v", len(result.Content), err)
	}
}

func TestProcessToolTruncatesAndDrainsOversizedOutput(t *testing.T) {
	if os.Getenv("VENAT_PROCESS_OVERSIZED_OUTPUT_HELPER") == "1" {
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 2<<20))
		_, _ = os.Stdout.WriteString("last-diagnostic")
		if err := os.WriteFile(os.Getenv("VENAT_PROCESS_DONE_FILE"), []byte("completed"), 0600); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	doneFile := t.TempDir() + "/done"
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolTruncatesAndDrainsOversizedOutput$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_OVERSIZED_OUTPUT_HELPER=1", "VENAT_PROCESS_DONE_FILE="+doneFile),
	})
	var streamed strings.Builder
	result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{
		ID:        "call-process-oversized",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, tool.ExecuteOptions{Sink: func(update tool.Update) error {
		for _, part := range update.Parts {
			streamed.WriteString(part.Text)
		}
		return nil
	}})
	if err != nil || result.IsError || !strings.HasSuffix(result.Content, "[Output truncated at 1048576 bytes.]") || len(result.Content) > defaultMaxProcessOutputBytes+100 || result.Content != streamed.String() || !strings.Contains(result.Content, "last-diagnostic") {
		t.Fatalf("size=%d error=%v is_error=%v", len(result.Content), err, result.IsError)
	}
	if content, err := os.ReadFile(doneFile); err != nil || string(content) != "completed" {
		t.Fatalf("child was interrupted: %q, %v", content, err)
	}
}

func TestProcessToolCapturesStdoutAndStderr(t *testing.T) {
	if os.Getenv("VENAT_PROCESS_CAPTURE_HELPER") == "1" {
		_, _ = os.Stdout.WriteString("stdout-body")
		_, _ = os.Stderr.WriteString("stderr-body")
		os.Exit(0)
	}

	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolCapturesStdoutAndStderr$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_CAPTURE_HELPER=1"),
	})
	result, err := driver.Execute(context.Background(), tool.Call{
		ID:        "call-process-capture",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !bytes.Contains([]byte(result.Content), []byte("stdout-body")) {
		t.Fatalf("missing stdout in %#q", result.Content)
	}
	if !bytes.Contains([]byte(result.Content), []byte("stderr-body")) {
		t.Fatalf("missing stderr in %#q", result.Content)
	}
}

func TestProcessTool_NonzeroExitIsCompletedFeedback(t *testing.T) {
	if code := os.Getenv("VENAT_PROCESS_EXIT_HELPER"); code != "" {
		_, _ = os.Stderr.WriteString("assertion failed")
		value, _ := strconv.Atoi(code)
		os.Exit(value)
	}
	for _, code := range []int{1, 2, 7, 126, 127, 128, 137, 255} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
				Command: os.Args[0], Args: []string{"-test.run=^TestProcessTool_NonzeroExitIsCompletedFeedback$"},
				Env: append(os.Environ(), "VENAT_PROCESS_EXIT_HELPER="+strconv.Itoa(code)),
			})
			var streamed strings.Builder
			result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{Name: "run", Arguments: json.RawMessage(`{}`)}, tool.ExecuteOptions{
				Sink: func(update tool.Update) error {
					for _, part := range update.Parts {
						streamed.WriteString(part.Text)
					}
					return nil
				},
			})
			if err != nil || !result.IsError || result.Content != streamed.String() || !strings.Contains(result.Content, fmt.Sprintf("assertion failed\nProcess exited with code %d.", code)) {
				t.Fatalf("result=%+v stream=%q error=%v", result, streamed.String(), err)
			}
		})
	}
}

func TestProcessTool_MissingExecutableIsFeedbackButCancellationIsFatal(t *testing.T) {
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{Command: t.TempDir() + "/missing"})
	result, err := driver.Execute(context.Background(), tool.Call{Name: "run"}, nil)
	if err != nil || !result.IsError || !strings.Contains(result.Content, "did not start") {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = driver.Execute(ctx, tool.Call{Name: "run"}, nil)
	if !errors.Is(err, tool.ErrNotExecuted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled launch error=%v", err)
	}
}

func TestProcessTool_ConfirmedSignalExitIsFeedback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX signal scenario")
	}
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{Command: "/bin/sh", Args: []string{"-c", "printf failed; kill -TERM $$"}})
	result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{Name: "run"}, tool.ExecuteOptions{})
	if err != nil || !result.IsError || !strings.Contains(result.Content, "failed") || !strings.Contains(result.Content, "signal:") {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestHTTPTool_RejectionIncludesStatusAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"invalid path"}`))
	}))
	defer server.Close()
	driver := HTTPTool("request", tool.Schema{Type: "object"}, HTTPToolConfig{URL: server.URL})
	result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{Name: "request"}, tool.ExecuteOptions{})
	if err != nil || !result.IsError || !strings.Contains(result.Content, "422") || string(result.Structured) != `{"error":"invalid path"}` {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestProcessToolStreamsOutputBeforeReturning(t *testing.T) {
	if os.Getenv("VENAT_PROCESS_STREAM_HELPER") == "1" {
		_, _ = os.Stdout.WriteString("streamed")
		os.Exit(0)
	}

	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolStreamsOutputBeforeReturning$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_STREAM_HELPER=1"),
	})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	updates := make(chan tool.Update, 1)
	type outcome struct {
		result tool.Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{
			ID:          "call-process-stream",
			Name:        "run",
			OperationID: "turn:0:call:0",
			Arguments:   json.RawMessage(`{}`),
		}, tool.ExecuteOptions{Sink: func(update tool.Update) error {
			updates <- tool.CloneUpdate(update)
			<-release
			return nil
		}})
		done <- outcome{result: result, err: err}
	}()

	var update tool.Update
	select {
	case update = <-updates:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for process output update")
	}
	if update.Kind != tool.UpdateOutput || update.ToolCallID != "call-process-stream" ||
		update.OperationID != "turn:0:call:0" || update.Sequence != 1 ||
		len(update.Parts) != 1 || update.Parts[0].Text != "streamed" {
		t.Fatalf("process update = %#v", update)
	}
	select {
	case current := <-done:
		t.Fatalf("Execute() returned before sink released: %#v", current)
	default:
	}
	close(release)
	current := <-done
	if current.err != nil {
		t.Fatalf("Execute() error = %v", current.err)
	}
	if current.result.Content != "streamed" || len(current.result.Parts) != 1 || current.result.Parts[0].Text != "streamed" {
		t.Fatalf("process result = %#v", current.result)
	}
}

func TestProcessToolDrainsPipeBeforeWait(t *testing.T) {
	const payloadSize = 256 << 10
	if os.Getenv("VENAT_PROCESS_DRAIN_HELPER") == "1" {
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("a"), payloadSize))
		_, _ = os.Stderr.Write(bytes.Repeat([]byte("b"), payloadSize))
		os.Exit(0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolDrainsPipeBeforeWait$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_DRAIN_HELPER=1"),
	})
	result, err := driver.Execute(ctx, tool.Call{
		ID:        "call-process-drain",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(result.Content); got != payloadSize*2 {
		t.Fatalf("captured %d bytes, want %d", got, payloadSize*2)
	}
}

func TestProcessToolForwardsStdinJSON(t *testing.T) {
	if os.Getenv("VENAT_PROCESS_STDIN_HELPER") == "1" {
		payload, err := io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(2)
		}
		_, _ = os.Stdout.Write(payload)
		os.Exit(0)
	}

	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command:   os.Args[0],
		Args:      []string{"-test.run=^TestProcessToolForwardsStdinJSON$"},
		Env:       append(os.Environ(), "VENAT_PROCESS_STDIN_HELPER=1"),
		StdinJSON: true,
	})
	result, err := driver.Execute(context.Background(), tool.Call{
		ID:        "call-process-stdin",
		Name:      "run",
		Arguments: json.RawMessage(`{"query":"venat"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Content != `{"query":"venat"}` {
		t.Fatalf("stdin round-trip = %#q", result.Content)
	}
}

func TestProcessToolCapturesOutputAfterLongRunningChild(t *testing.T) {
	const payloadSize = 64 << 10
	if os.Getenv("VENAT_PROCESS_LONG_HELPER") == "1" {
		time.Sleep(250 * time.Millisecond)
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("z"), payloadSize))
		os.Exit(0)
	}

	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolCapturesOutputAfterLongRunningChild$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_LONG_HELPER=1"),
	})
	result, err := driver.Execute(context.Background(), tool.Call{
		ID:        "call-process-long",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(result.Content); got != payloadSize {
		t.Fatalf("captured %d bytes after long-running child, want %d", got, payloadSize)
	}
}

func TestProcessToolReturnsAfterChildExitsWithInheritedPipes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inherited-pipe orphan test uses a Unix shell")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: "sh",
		Args:    []string{"-c", "printf ready; sleep 8 &"},
	})
	started := time.Now()
	result, err := driver.Execute(ctx, tool.Call{
		ID:        "call-process-orphan",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(result.Content, "ready") {
		t.Fatalf("missing child output in %#q", result.Content)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked on inherited pipe for %s", elapsed)
	}
}

func TestProcessToolPreservesCommandContextCancel(t *testing.T) {
	if os.Getenv("VENAT_PROCESS_CANCEL_HELPER") == "1" {
		_, _ = io.Copy(io.Discard, os.Stdin)
		time.Sleep(time.Hour)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestProcessToolPreservesCommandContextCancel$"},
		Env:     append(os.Environ(), "VENAT_PROCESS_CANCEL_HELPER=1"),
	})
	_, err := driver.Execute(ctx, tool.Call{
		ID:        "call-process-cancel",
		Name:      "run",
		Arguments: json.RawMessage(`{}`),
	}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
}

func TestAdapterToolsCarryExecutionSettings(t *testing.T) {
	drivers := []tool.Driver{
		HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{URL: "https://example.test/tool"},
			Timeout(5*time.Second),
			Concurrency(tool.ConcurrencySequential),
			ConcurrencyGroup("adapters"),
			MaxConcurrency(1),
		),
		ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{Command: "printf"},
			Timeout(5*time.Second),
			Concurrency(tool.ConcurrencySequential),
			ConcurrencyGroup("adapters"),
			MaxConcurrency(1),
		),
	}
	for _, driver := range drivers {
		def := driver.Definition()
		if def.Timeout != 5*time.Second {
			t.Fatalf("%s timeout = %s, want 5s", def.Name, def.Timeout)
		}
		if def.Concurrency != tool.ConcurrencySequential || def.ConcurrencyGroup != "adapters" || def.MaxConcurrency != 1 {
			t.Fatalf("%s concurrency settings = %#v", def.Name, def)
		}
	}
}

func TestProcessToolLocalDeadlineReturnsFeedback(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("managed process groups are supported on Darwin and Linux")
	}
	if os.Getenv("VENAT_LOCAL_DEADLINE_HELPER") == "1" {
		_, _ = os.Stdout.WriteString("started")
		time.Sleep(time.Hour)
		os.Exit(0)
	}
	driver := ProcessTool("run", tool.Schema{Type: "object"}, ProcessToolConfig{
		Command: os.Args[0], Args: []string{"-test.run=^TestProcessToolLocalDeadlineReturnsFeedback$"}, Env: append(os.Environ(), "VENAT_LOCAL_DEADLINE_HELPER=1"),
	}, Timeout(300*time.Millisecond))
	var output strings.Builder
	result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{ID: "local", Name: "run", Arguments: json.RawMessage(`{}`)}, tool.ExecuteOptions{Sink: func(update tool.Update) error {
		for _, part := range update.Parts {
			output.WriteString(part.Text)
		}
		return nil
	}})
	if err != nil || !result.IsError || !strings.Contains(result.Content, "tool-local deadline") || !strings.Contains(result.Content, "started") || result.Content != output.String() {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
