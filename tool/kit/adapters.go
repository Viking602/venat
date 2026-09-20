package kit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/tool"
)

const (
	defaultHTTPTimeout           = 30 * time.Second
	defaultProcessTimeout        = 30 * time.Second
	defaultMaxResponseBytes      = 1 << 20
	defaultMaxProcessOutputBytes = 1 << 20
)

type HTTPToolConfig struct {
	Method  string
	URL     string
	Headers map[string]string
	Client  *http.Client
}

func HTTPTool(name string, schema tool.Schema, cfg HTTPToolConfig, options ...ToolOption) tool.Driver {
	config := toolConfig{}
	for _, option := range options {
		option(&config)
	}
	driver := staticDriver{
		definition: definitionFromConfig(name, schema, config),
		execute: func(ctx context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
			if _, hasDeadline := ctx.Deadline(); !hasDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, defaultHTTPTimeout)
				defer cancel()
			}
			client := cfg.Client
			if client == nil {
				client = &http.Client{Timeout: defaultHTTPTimeout}
			}
			method := cfg.Method
			if method == "" {
				method = http.MethodPost
			}
			request, err := http.NewRequestWithContext(ctx, method, cfg.URL, bytes.NewReader(call.Arguments))
			if err != nil {
				return tool.Result{}, errors.Join(tool.ErrNotExecuted, err)
			}
			request.Header.Set("Content-Type", "application/json")
			for key, value := range cfg.Headers {
				request.Header.Set(key, value)
			}
			response, err := client.Do(request)
			if err != nil {
				return tool.Result{}, err
			}
			defer func() { _ = response.Body.Close() }()
			body, truncated, err := readLimited(response.Body, defaultMaxResponseBytes)
			if err != nil {
				return tool.Result{}, err
			}
			result := resultFromPayload(call, body, response.StatusCode >= 400 || truncated)
			if truncated {
				result.Structured = nil
				result.Content += "\n[Response body truncated; request was sent. Use a smaller response or pagination; do not assume the operation was not executed.]"
			}
			if result.IsError {
				result.Content += fmt.Sprintf("\nHTTP %d %s.", response.StatusCode, http.StatusText(response.StatusCode))
			}
			return result, nil
		},
	}
	return driver
}

// ProcessToolConfig describes an unsandboxed local process. This helper does
// not apply an argv allowlist. When Env is empty the child receives a minimal
// PATH/HOME/LANG environment instead of the parent process environment.
type ProcessToolConfig struct {
	Command   string
	Args      []string
	Dir       string
	Env       []string
	StdinJSON bool
}

// ProcessTool streams bounded output and drains excess logs until the process
// exits. Confirmed unsuccessful exits and ordinary launch rejections produce
// IsError feedback. Cancellation, I/O, and sink failures remain Go errors.
func ProcessTool(name string, schema tool.Schema, cfg ProcessToolConfig, options ...ToolOption) tool.Driver {
	config := toolConfig{}
	for _, option := range options {
		option(&config)
	}
	return staticDriver{
		definition: definitionFromConfig(name, schema, config),
		execute: func(ctx context.Context, call tool.Call, sink tool.UpdateSink) (tool.Result, error) {
			output, failed, err := runProcess(ctx, cfg, call.Arguments, sink)
			if err != nil {
				return tool.Result{}, err
			}
			return resultFromPayload(call, output, failed), nil
		},
	}
}

func minimalProcessEnv() []string {
	out := make([]string, 0, 6)
	for _, key := range []string{"PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "TMP"} {
		if value, ok := os.LookupEnv(key); ok && value != "" {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func readLimited(reader io.Reader, maxBytes int64) ([]byte, bool, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > maxBytes {
		return body[:maxBytes], true, nil
	}
	return body, false, nil
}

func runProcess(ctx context.Context, cfg ProcessToolConfig, input []byte, sink tool.UpdateSink) ([]byte, bool, error) {
	var commandCtx context.Context
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		commandCtx, cancel = context.WithCancel(ctx)
	} else {
		commandCtx, cancel = context.WithTimeoutCause(ctx, defaultProcessTimeout, tool.ErrToolTimeout)
	}
	defer cancel()

	command := exec.CommandContext(commandCtx, cfg.Command, cfg.Args...)
	groupCancellation := configureProcessCancellation(command)
	command.Dir = cfg.Dir
	if len(cfg.Env) > 0 {
		command.Env = append([]string{}, cfg.Env...)
	} else {
		command.Env = minimalProcessEnv()
	}
	if cfg.StdinJSON {
		command.Stdin = bytes.NewReader(input)
	}

	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return nil, false, errors.Join(tool.ErrNotExecuted, err)
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		return nil, false, errors.Join(tool.ErrNotExecuted, err, closePipe(stdoutRead), closePipe(stdoutWrite))
	}
	command.Stdout = stdoutWrite
	command.Stderr = stderrWrite

	if err := command.Start(); err != nil {
		closeErr := errors.Join(closePipe(stdoutRead), closePipe(stdoutWrite), closePipe(stderrRead), closePipe(stderrWrite))
		if ctxErr := commandCtx.Err(); ctxErr != nil {
			return nil, false, errors.Join(tool.ErrNotExecuted, ctxErr, closeErr)
		}
		if closeErr == nil && (errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission)) {
			return []byte(fmt.Sprintf("Process did not start: %v", err)), true, nil
		}
		return nil, false, errors.Join(tool.ErrNotExecuted, err, closeErr)
	}
	// Close the parent write ends so Copy sees EOF when the child exits.
	// Wait() must not own these pipes: StdoutPipe/StderrPipe close them under
	// the readers and can drop output or wake Copy with a spurious error.
	if err := errors.Join(closePipe(stdoutWrite), closePipe(stderrWrite)); err != nil {
		cancel()
		_ = stdoutRead.Close()
		_ = stderrRead.Close()
		return nil, false, errors.Join(err, command.Wait())
	}

	output := &limitedOutputBuffer{max: defaultMaxProcessOutputBytes, sink: sink}
	copyErrs := make(chan error, 2)
	copyOutput := func(reader io.ReadCloser) {
		_, copyErr := io.Copy(output, reader)
		if ignoredCopyError(copyErr) {
			copyErr = nil
		}
		closeErr := closePipe(reader)
		if copyErr != nil || closeErr != nil {
			cancel()
		}
		copyErrs <- errors.Join(copyErr, closeErr)
	}
	go copyOutput(stdoutRead)
	go copyOutput(stderrRead)

	copyDone := make(chan struct{})
	var stdoutErr, stderrErr error
	go func() {
		stdoutErr = <-copyErrs
		stderrErr = <-copyErrs
		close(copyDone)
	}()
	waitErr := waitAndUnblockPipes(commandCtx, command, copyDone, stdoutRead, stderrRead)
	return finishProcessOutput(commandCtx, output, waitErr, errors.Join(stdoutErr, stderrErr), sink, groupCancellation)
}

// Finalize feedback only after Wait and both output readers have completed.
func finishProcessOutput(ctx context.Context, output *limitedOutputBuffer, waitErr, copyErr error, sink tool.UpdateSink, managedGroup bool) ([]byte, bool, error) {
	localTimeout := managedGroup && errors.Is(context.Cause(ctx), tool.ErrToolTimeout) && completedProcessExit(waitErr) != nil
	contextErr := ctx.Err()
	if localTimeout {
		contextErr = nil
	} // child reaped; partial side effects are still possible
	if err := errors.Join(contextErr, copyErr); err != nil {
		return output.Bytes(), false, err
	}
	if err := output.Flush(); err != nil {
		return output.Bytes(), false, err
	}
	var suffix string
	if waitErr != nil {
		exited := completedProcessExit(waitErr)
		if exited == nil {
			return output.Bytes(), false, waitErr
		}
		suffix = "\n" + processExitDescription(exited)
	}
	if localTimeout {
		suffix += "\n[Process exceeded its tool-local deadline and was terminated. Partial side effects may have occurred.]"
	}
	if output.TooLarge() {
		suffix += fmt.Sprintf("\n[Output truncated at %d bytes.]", defaultMaxProcessOutputBytes)
	}
	if sink != nil && suffix != "" {
		if err := sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart(suffix)}}); err != nil {
			return output.Bytes(), false, err
		}
	}
	return append(output.Bytes(), suffix...), waitErr != nil, nil
}

// Only a single error chain ending in an observed process exit qualifies.
// Joined errors may also contain an infrastructure failure and stay fatal.
func completedProcessExit(err error) *exec.ExitError {
	for err != nil {
		if exited, ok := err.(*exec.ExitError); ok && exited.ProcessState != nil {
			return exited
		}
		err = errors.Unwrap(err)
	}
	return nil
}

func processExitDescription(exited *exec.ExitError) string {
	if code := exited.ExitCode(); code >= 0 {
		return fmt.Sprintf("Process exited with code %d.", code)
	}
	return "Process terminated: " + exited.Error() + "."
}

func waitAndUnblockPipes(ctx context.Context, command *exec.Cmd, copyDone <-chan struct{}, stdout, stderr *os.File) error {
	waitErrs := make(chan error, 1)
	go func() { waitErrs <- command.Wait() }()

	select {
	case waitErr := <-waitErrs:
		select {
		case <-copyDone:
			return waitErr
		case <-time.After(100 * time.Millisecond):
			// Windows anonymous pipes do not interrupt a blocked Read
			// with SetReadDeadline. Close the parent read ends instead.
			_ = stdout.Close()
			_ = stderr.Close()
			<-copyDone
			return waitErr
		}
	case <-ctx.Done():
		_ = stdout.Close()
		_ = stderr.Close()
		var waitErr error
		select {
		case waitErr = <-waitErrs:
		case <-time.After(time.Second):
			waitErr = ctx.Err()
		}
		<-copyDone
		return waitErr
	}
}

func ignoredCopyError(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, os.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe)
}

func closePipe(closer io.Closer) error {
	if closer == nil {
		return nil
	}
	err := closer.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

type limitedOutputBuffer struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	max      int64
	tail     []byte
	total    int64
	sink     tool.UpdateSink
	tooLarge bool
}

func (b *limitedOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := len(p)
	b.total += int64(size)
	b.tooLarge = b.total > b.max
	headSize := int(b.max) / 2
	take := min(len(p), max(0, headSize-b.buf.Len()))
	if take > 0 {
		_, _ = b.buf.Write(p[:take])
		if err := b.emit(p[:take]); err != nil {
			return take, err
		}
	}
	p = p[take:]
	tailSize := int(b.max) - headSize
	if len(p) >= tailSize {
		b.tail = append(b.tail[:0], p[len(p)-tailSize:]...)
	} else {
		drop := max(0, len(b.tail)+len(p)-tailSize)
		b.tail = append(b.tail[drop:], p...)
	}
	return size, nil
}

// The stable head streams immediately; the rolling tail is emitted only after
// completion so the final result exactly matches the accumulated stream.
func (b *limitedOutputBuffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.emit(b.pending())
}

func (b *limitedOutputBuffer) pending() []byte {
	var result []byte
	if b.tooLarge {
		result = []byte("\n[... middle output omitted ...]\n")
	}
	return append(result, b.tail...)
}

func (b *limitedOutputBuffer) emit(chunk []byte) error {
	if b.sink == nil || len(chunk) == 0 {
		return nil
	}
	return b.sink(tool.Update{
		Kind:  tool.UpdateOutput,
		Parts: []message.ContentPart{message.TextPart(string(chunk))},
	})
}

func (b *limitedOutputBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append(append([]byte{}, b.buf.Bytes()...), b.pending()...)
}

func (b *limitedOutputBuffer) TooLarge() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tooLarge
}

type staticDriver struct {
	definition tool.Definition
	execute    func(ctx context.Context, call tool.Call, sink tool.UpdateSink) (tool.Result, error)
}

func (d staticDriver) Definition() tool.Definition {
	return d.definition
}

func (d staticDriver) Execute(ctx context.Context, call tool.Call, sink tool.UpdateSink) (tool.Result, error) {
	return d.execute(ctx, call, sink)
}

func resultFromPayload(call tool.Call, payload []byte, isError bool) tool.Result {
	result := tool.Result{
		ToolCallID: call.ID,
		Name:       call.Name,
		Content:    string(payload),
		IsError:    isError,
	}
	if json.Valid(payload) {
		result.Structured = append([]byte{}, payload...)
	}
	return result
}
