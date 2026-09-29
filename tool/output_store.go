package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Viking602/venat/message"
)

const (
	// DefaultOutputPreviewThreshold is the default inline body preview limit.
	DefaultOutputPreviewThreshold = 4096
	defaultOutputReadLimit        = 32 * 1024
	defaultOutputSearchLimit      = 64 * 1024
	maxConfiguredOutputRead       = 1 << 20
	maxConfiguredOutputSearch     = 1 << 20
	maxOutputArtifactBytes        = 64 << 20
	artifactScheme                = "artifact://"
)

var artifactReferencePattern = regexp.MustCompile(`^artifact://[0-9a-f]{64}$`)

// OutputStoreOption configures a local OutputStore.
type OutputStoreOption func(*OutputStore)

// WithPreviewThreshold bounds the inline preview of each stored body. Values
// less than one are rejected by NewOutputStore.
func WithPreviewThreshold(bytes int) OutputStoreOption {
	return func(store *OutputStore) { store.previewThreshold = bytes }
}

// WithReadLimit bounds one model-requested artifact read.
func WithReadLimit(bytes int) OutputStoreOption {
	return func(store *OutputStore) { store.readLimit = bytes }
}

// WithSearchLimit bounds the bytes returned by one model search. Searching
// still streams through larger artifacts; this limit only bounds the response.
func WithSearchLimit(bytes int) OutputStoreOption {
	return func(store *OutputStore) { store.searchLimit = bytes }
}

// OutputStore keeps oversized tool output in private, content-addressed files.
// References contain no filesystem information and remain stable for identical
// bodies in the same store.
type OutputStore struct {
	root             *os.Root
	previewThreshold int
	readLimit        int
	searchLimit      int
	mu               sync.Mutex
}

// NewOutputStore creates a private local artifact directory. The directory is
// resolved before use so a symlink supplied as root cannot redirect artifacts.
func NewOutputStore(root string, options ...OutputStoreOption) (*OutputStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("output store root is empty")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create output store: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect output store: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("output store root is not a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("output store root must be private")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve output store: %w", err)
	}
	rootFS, err := os.OpenRoot(resolved)
	if err != nil {
		return nil, fmt.Errorf("open output store: %w", err)
	}
	store := &OutputStore{
		root:             rootFS,
		previewThreshold: DefaultOutputPreviewThreshold,
		readLimit:        defaultOutputReadLimit,
		searchLimit:      defaultOutputSearchLimit,
	}
	for _, option := range options {
		if option != nil {
			option(store)
		}
	}
	if store.previewThreshold < 1 || store.readLimit < 1 || store.searchLimit < 1 || store.readLimit > maxConfiguredOutputRead || store.searchLimit > maxConfiguredOutputSearch {
		_ = rootFS.Close()
		return nil, errors.New("output store limits are outside safe bounds")
	}
	return store, nil
}

// Process stores oversized textual or structured bodies and returns a bounded
// result containing an opaque reference. On storage failure it returns the
// cloned original result and a Go error, never a reference to an uncertain file.
func (store *OutputStore) Process(ctx context.Context, input Result) (Result, error) {
	if store == nil || store.root == nil {
		return input, errors.New("output store is nil")
	}
	if err := ctx.Err(); err != nil {
		return message.CloneToolResult(input), err
	}
	result := message.CloneToolResult(input)
	changed, err := store.processText(ctx, &result)
	if err != nil {
		return message.CloneToolResult(input), err
	}
	structuredChanged, err := store.processStructured(ctx, &result)
	if err != nil {
		return message.CloneToolResult(input), err
	}
	if (changed || structuredChanged) && len(result.Parts) == 0 {
		result.Parts = []message.ContentPart{message.TextPart(result.Content)}
	}
	return result, nil
}

func (store *OutputStore) processText(ctx context.Context, result *Result) (bool, error) {
	if len(result.Parts) == 0 {
		return store.processContent(ctx, result)
	}
	textIndexes, total := textPartIndexes(result.Parts)
	if total > store.previewThreshold {
		body := make([]byte, 0, total)
		for _, index := range textIndexes {
			body = append(body, result.Parts[index].Text...)
		}
		ref, err := store.save(ctx, body)
		if err != nil {
			return false, err
		}
		preview := store.preview(body, ref)
		preserved := make([]message.ContentPart, 0, len(result.Parts)-len(textIndexes)+1)
		inserted := false
		for _, part := range result.Parts {
			if isTextPart(part.Kind) {
				if !inserted {
					preserved = append(preserved, message.TextPart(preview))
					inserted = true
				}
				continue
			}
			preserved = append(preserved, part)
		}
		result.Parts, result.Content = preserved, message.ToolResult{Parts: preserved}.TextContent()
		return true, nil
	}
	if len(result.Content) > store.previewThreshold && result.Content != result.TextContent() {
		return store.processContent(ctx, result)
	}
	return false, nil
}

func textPartIndexes(parts []message.ContentPart) ([]int, int) {
	indexes := make([]int, 0, len(parts))
	total := 0
	for index, part := range parts {
		if isTextPart(part.Kind) {
			indexes = append(indexes, index)
			total += len(part.Text)
		}
	}
	return indexes, total
}

func (store *OutputStore) processContent(ctx context.Context, result *Result) (bool, error) {
	if len(result.Content) <= store.previewThreshold {
		return false, nil
	}
	ref, err := store.save(ctx, []byte(result.Content))
	if err != nil {
		return false, err
	}
	marker := store.preview([]byte(result.Content), ref)
	if len(result.Parts) > 0 {
		result.Parts = append(result.Parts, message.TextPart(marker))
		result.Content = result.TextContent()
	} else {
		result.Content = marker
	}
	return true, nil
}

func (store *OutputStore) processStructured(ctx context.Context, result *Result) (bool, error) {
	if len(result.Structured) <= store.previewThreshold {
		return false, nil
	}
	ref, err := store.save(ctx, result.Structured)
	if err != nil {
		return false, err
	}
	marker := store.preview(nil, ref)
	result.Structured = nil
	if len(result.Parts) > 0 {
		result.Parts = append(result.Parts, message.TextPart(marker))
		result.Content = result.TextContent()
	} else if result.Content == "" {
		result.Content = marker
	} else {
		result.Content += "\n" + marker
	}
	return true, nil
}

func isTextPart(kind message.ContentKind) bool {
	return kind == message.ContentText || kind == message.ContentCommentary || kind == message.ContentFinalAnswer
}

func (store *OutputStore) reference(data []byte) string {
	digest := sha256.Sum256(data)
	return artifactScheme + hex.EncodeToString(digest[:])
}

func (store *OutputStore) pathFor(reference string) (string, error) {
	if !artifactReferencePattern.MatchString(reference) {
		return "", errors.New("invalid artifact reference")
	}
	name := strings.TrimPrefix(reference, artifactScheme)
	if name == "" || filepath.IsAbs(name) || name == "." || strings.Contains(name, string(filepath.Separator)) || strings.Contains(name, "/") {
		return "", errors.New("artifact reference escapes store")
	}
	return name, nil
}

func (store *OutputStore) save(ctx context.Context, data []byte) (string, error) {
	if len(data) > maxOutputArtifactBytes {
		return "", fmt.Errorf("tool output exceeds artifact limit of %d bytes", maxOutputArtifactBytes)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ref := store.reference(data)
	name, err := store.pathFor(ref)
	if err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if exists, err := store.matchesExisting(name, data); err != nil {
		return "", err
	} else if exists {
		return ref, nil
	}
	temp := fmt.Sprintf(".tmp-%x-%d", sha256.Sum256(data), time.Now().UnixNano())
	file, err := store.root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create artifact: %w", err)
	}
	written, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil || written != len(data) {
		_ = store.root.Remove(temp)
		if writeErr != nil {
			return "", fmt.Errorf("write artifact: %w", writeErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close artifact: %w", closeErr)
		}
		return "", io.ErrShortWrite
	}
	if err := ctx.Err(); err != nil {
		_ = store.root.Remove(temp)
		return "", err
	}
	if err := store.root.Rename(temp, name); err != nil {
		_ = store.root.Remove(temp)
		if exists, checkErr := store.matchesExisting(name, data); checkErr == nil && exists {
			return ref, nil
		}
		return "", fmt.Errorf("publish artifact: %w", err)
	}
	return ref, nil
}

func (store *OutputStore) matchesExisting(name string, data []byte) (bool, error) {
	info, err := store.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect artifact: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != int64(len(data)) {
		return false, errors.New("existing artifact content does not match reference")
	}
	file, err := store.root.Open(name)
	if err != nil {
		return false, fmt.Errorf("open existing artifact: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.CopyN(digest, file, int64(len(data))+1); err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("hash existing artifact: %w", err)
	}
	if hex.EncodeToString(digest.Sum(nil)) != name {
		return false, errors.New("existing artifact content does not match reference")
	}
	return true, nil
}

func (store *OutputStore) preview(data []byte, ref string) string {
	marker := "[stored: " + ref + "]"
	if len(data) == 0 {
		return marker
	}
	omitted := "\n[… omitted …]\n"
	budget := store.previewThreshold - len(marker) - len(omitted)
	if budget < 1 {
		return marker
	}
	if len(data) <= budget {
		return string(data) + omitted[:1] + marker
	}
	headLen := budget / 2
	tailLen := budget - headLen
	headLen = utf8PrefixBoundary(data, headLen)
	tailStart := utf8SuffixBoundary(data, len(data)-tailLen)
	if tailStart < headLen {
		tailStart = headLen
	}
	return string(data[:headLen]) + omitted + string(data[tailStart:]) + "\n" + marker
}

func utf8PrefixBoundary(data []byte, end int) int {
	for end > 0 && end < len(data) && !utf8.RuneStart(data[end]) {
		end--
	}
	return end
}

func utf8SuffixBoundary(data []byte, start int) int {
	for start < len(data) && start > 0 && !utf8.RuneStart(data[start]) {
		start++
	}
	return start
}

func (store *OutputStore) read(reference string, offset, limit int) ([]byte, int64, error) {
	name, err := store.pathFor(reference)
	if err != nil {
		return nil, 0, err
	}
	if offset < 0 || limit < 1 || limit > store.readLimit {
		return nil, 0, fmt.Errorf("offset must be nonnegative and limit must be 1..%d", store.readLimit)
	}
	file, size, err := store.openArtifact(name)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek artifact: %w", err)
	}
	chunk := make([]byte, limit)
	n, err := io.ReadFull(file, chunk)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, 0, fmt.Errorf("read artifact: %w", err)
	}
	return chunk[:n], size, nil
}

func (store *OutputStore) search(ctx context.Context, reference, query string, maxMatches, contextBytes int) (string, error) {
	if err := validateSearchRequest(query, maxMatches, contextBytes, store.searchLimit); err != nil {
		return "", err
	}
	name, err := store.pathFor(reference)
	if err != nil {
		return "", err
	}
	file, _, err := store.openArtifact(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return scanSearch(ctx, file, []byte(query), maxMatches, contextBytes, store.searchLimit)
}

func validateSearchRequest(query string, maxMatches, contextBytes, responseLimit int) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("query is required")
	}
	if maxMatches < 1 || maxMatches > 100 || contextBytes < 0 || contextBytes > responseLimit/2 {
		return fmt.Errorf("maxMatches must be 1..100 and context must be 0..%d", responseLimit/2)
	}
	return nil
}

func (store *OutputStore) openArtifact(name string) (*os.File, int64, error) {
	linkInfo, linkErr := store.root.Lstat(name)
	if linkErr != nil {
		return nil, 0, fmt.Errorf("artifact unavailable: %w", linkErr)
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 {
		return nil, 0, errors.New("artifact is a symlink")
	}
	file, err := store.root.Open(name)
	if err != nil {
		return nil, 0, fmt.Errorf("artifact unavailable: %w", err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxOutputArtifactBytes {
		_ = file.Close()
		return nil, 0, errors.New("artifact is not a safe regular file")
	}
	return file, info.Size(), nil
}

func scanSearch(ctx context.Context, file *os.File, query []byte, maxMatches, contextBytes, responseLimit int) (string, error) {
	overlap := max(len(query)-1, contextBytes)
	carry := make([]byte, 0, overlap)
	chunk := make([]byte, 32*1024)
	var output strings.Builder
	matches, lastMatch := 0, int64(-1)
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, readErr := file.Read(chunk)
		scan := append(append(make([]byte, 0, len(carry)+n), carry...), chunk[:n]...)
		base, position := offset-int64(len(carry)), 0
		for position <= len(scan)-len(query) && matches < maxMatches {
			relative := bytes.Index(scan[position:], query)
			if relative < 0 {
				break
			}
			position += relative
			absolute := base + int64(position)
			if absolute+int64(len(query)) <= offset {
				position++
				continue
			}
			if absolute != lastMatch {
				entry := formatSearchMatch(absolute, scan, position, len(query), contextBytes)
				if output.Len()+len(entry) > responseLimit {
					if output.Len() == 0 {
						return "", fmt.Errorf("search response limit %d is too small to report a match", responseLimit)
					}
					return output.String(), nil
				}
				output.WriteString(entry)
				matches++
				lastMatch = absolute
			}
			position++
		}
		if overlap > 0 {
			keep := min(overlap, len(scan))
			carry = append(carry[:0], scan[len(scan)-keep:]...)
		} else {
			carry = carry[:0]
		}
		offset += int64(n)
		if readErr == io.EOF {
			return outputOrNoMatches(&output), nil
		}
		if readErr != nil {
			return "", fmt.Errorf("search artifact: %w", readErr)
		}
	}
}

func formatSearchMatch(offset int64, data []byte, position, queryLength, contextBytes int) string {
	start := max(0, position-contextBytes)
	end := min(len(data), position+queryLength+contextBytes)
	start = utf8SuffixBoundary(data, start)
	end = utf8PrefixBoundary(data, end)
	if end < position+queryLength {
		end = position + queryLength
	}
	if start > position {
		start = position
	}
	snippet := string(data[start:end])
	if start > 0 {
		snippet = "…" + snippet
	}
	if end < len(data) {
		snippet += "…"
	}
	return fmt.Sprintf("offset=%d: %s\n", offset, snippet)
}

func outputOrNoMatches(output *strings.Builder) string {
	if output.Len() == 0 {
		return "no matches"
	}
	return output.String()
}

// Close releases the store directory handle. Callers that share a store
// with an Engine remain responsible for its lifetime.
func (store *OutputStore) Close() error {
	if store == nil || store.root == nil {
		return nil
	}
	return store.root.Close()
}

// Tools returns the model-callable bounded read and search drivers.
func (store *OutputStore) Tools() []Driver {
	if store == nil {
		return nil
	}
	return []Driver{artifactReadDriver{store: store}, artifactSearchDriver{store: store}}
}

type artifactReadDriver struct{ store *OutputStore }

func (driver artifactReadDriver) Definition() Definition {
	return Definition{Name: "tool_output_read", Description: "Read a bounded byte range from a stored tool output. Use the exact artifact:// reference from the tool result.", InputSchema: definitionSchema(map[string]Schema{
		"reference": {Type: "string"}, "offset": {Type: "integer"}, "limit": {Type: "integer"},
	}, []string{"reference", "offset", "limit"})}
}

func (driver artifactReadDriver) Execute(ctx context.Context, call Call, _ UpdateSink) (Result, error) {
	var input struct {
		Reference string `json:"reference"`
		Offset    int    `json:"offset"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(call.Arguments, &input); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	data, size, err := driver.store.read(input.Reference, input.Offset, input.Limit)
	if err != nil {
		return Result{Content: err.Error(), IsError: true}, nil
	}
	return Result{Content: fmt.Sprintf("offset=%d size=%d\n%s", input.Offset, size, data)}, nil
}

type artifactSearchDriver struct{ store *OutputStore }

func (driver artifactSearchDriver) Definition() Definition {
	return Definition{Name: "tool_output_search", Description: "Search a stored tool output for a query and return bounded matching lines.", InputSchema: definitionSchema(map[string]Schema{
		"reference": {Type: "string"}, "query": {Type: "string"}, "maxMatches": {Type: "integer"}, "context": {Type: "integer"},
	}, []string{"reference", "query"})}
}

func (driver artifactSearchDriver) Execute(ctx context.Context, call Call, _ UpdateSink) (Result, error) {
	var input struct {
		Reference  string `json:"reference"`
		Query      string `json:"query"`
		MaxMatches int    `json:"maxMatches"`
		Context    int    `json:"context"`
	}
	if err := json.Unmarshal(call.Arguments, &input); err != nil {
		return Result{}, err
	}
	if input.MaxMatches == 0 {
		input.MaxMatches = 20
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	content, err := driver.store.search(ctx, input.Reference, input.Query, input.MaxMatches, input.Context)
	if err != nil {
		return Result{Content: err.Error(), IsError: true}, nil
	}
	return Result{Content: content}, nil
}

func definitionSchema(properties map[string]Schema, required []string) Schema {
	additional := false
	return Schema{Type: "object", Properties: properties, Required: required, AdditionalProperties: &additional}
}
