package shared

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

const (
	// MaxSSELineBytes caps one physical line.
	MaxSSELineBytes = 8 << 20
	// MaxSSEFrameBytes and MaxSSEFrameLines bound aggregate multi-line events.
	MaxSSEFrameBytes = 16 << 20
	MaxSSEFrameLines = 4096
)

// Event represents a parsed Server-Sent Event frame.
type Event struct {
	Name    string
	Data    string
	ID      string
	Comment string
}

// Reader incrementally parses SSE frames from a text stream.
type Reader struct {
	reader *bufio.Reader
	bytes  int
}

func NewReader(body io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(body)}
}

// Next reads and returns the next SSE frame.
// It properly handles:
//   - multi-line data: concatenated with '\n'
//   - event: and id: fields
//   - comment lines starting with ':' (used for keepalive heartbeats)
//   - incremental/incomplete frames buffered across scanner reads
func (r *Reader) Next() (Event, error) {
	var (
		name    string
		data    []string
		id      string
		comment string
	)
	frameBytes := 0
	frameLines := 0
	for {
		line, err := r.readLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return Event{}, err
		}
		frameBytes += len(line) + 1
		frameLines++
		if frameBytes > MaxSSEFrameBytes || frameLines > MaxSSEFrameLines {
			return Event{}, fmt.Errorf(
				"sse: frame exceeds %d bytes or %d lines",
				MaxSSEFrameBytes,
				MaxSSEFrameLines,
			)
		}
		if strings.TrimSpace(line) == "" {
			// Empty line terminates a frame. Skip if frame is entirely empty.
			if len(data) == 0 && name == "" && id == "" && comment == "" {
				continue
			}
			return Event{
				Name:    name,
				Data:    strings.Join(data, "\n"),
				ID:      id,
				Comment: comment,
			}, nil
		}
		if strings.HasPrefix(line, ":") {
			// SSE comment (e.g., :keepalive)
			comment = strings.TrimSpace(strings.TrimPrefix(line, ":"))
			continue
		}
		field, value := parseSSELine(line)
		switch field {
		case "event":
			name = value
		case "data":
			data = append(data, value)
		case "id":
			id = value
			// retry: is intentionally ignored
		}
	}
	// A frame in progress at EOF means the stream was truncated mid-event:
	// surface it as an error rather than a valid final event, so the
	// provider Recv path reports a transport fault instead of silently
	// accepting a partial JSON payload (which could parse to a wrong
	// result against a permissive struct).
	if len(data) > 0 || name != "" || id != "" || comment != "" {
		return Event{
			Name:    name,
			Data:    strings.Join(data, "\n"),
			ID:      id,
			Comment: comment,
		}, io.ErrUnexpectedEOF
	}
	return Event{}, io.EOF
}

func (r *Reader) readLine() (string, error) {
	var buf []byte
	for {
		chunk, err := r.reader.ReadSlice('\n')
		if len(chunk) > MaxStreamBytes-r.bytes {
			return "", fmt.Errorf("sse: stream exceeds %d wire bytes", MaxStreamBytes)
		}
		r.bytes += len(chunk)
		if len(buf)+len(chunk) > MaxSSELineBytes {
			return "", fmt.Errorf("sse: line exceeds %d bytes", MaxSSELineBytes)
		}
		if err == bufio.ErrBufferFull {
			buf = append(buf, chunk...)
			continue
		}
		if err != nil && err != io.EOF {
			return "", err
		}
		if err == io.EOF && len(chunk) == 0 && len(buf) == 0 {
			return "", io.EOF
		}
		buf = append(buf, chunk...)
		line := string(buf)
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
		return line, nil
	}
}

// parseSSELine splits an SSE field line into (field, value).
// Per spec, strip one leading space after the colon if present.
func parseSSELine(line string) (field, value string) {
	colonIdx := strings.Index(line, ":")
	if colonIdx == -1 {
		return "", ""
	}
	field = line[:colonIdx]
	if colonIdx+1 < len(line) && line[colonIdx+1] == ' ' {
		value = line[colonIdx+2:]
	} else if colonIdx+1 < len(line) {
		value = line[colonIdx+1:]
	}
	return field, value
}
