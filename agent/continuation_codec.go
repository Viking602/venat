package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

// Version 1's nested request and output-policy exclusions are validated before
// decoding this shared shape. Auxiliary context usage was added to version 2.
type continuationWireV1 struct {
	SchemaVersion     int               `json:"schemaVersion"`
	Request           Request           `json:"request"`
	OutputPolicy      OutputPolicy      `json:"outputPolicy"`
	Messages          []message.Message `json:"messages"`
	Usage             provider.Usage    `json:"usage"`
	Steps             []Step            `json:"steps"`
	ToolCallsUsed     int               `json:"toolCallsUsed"`
	RepairCount       int               `json:"repairCount"`
	ActiveElapsed     time.Duration     `json:"activeElapsed"`
	NextOperationTurn int               `json:"nextOperationTurn"`
	Phase             ContinuationPhase `json:"phase"`
}

type continuationWireV2 struct {
	SchemaVersion     int               `json:"schemaVersion"`
	Request           Request           `json:"request"`
	OutputPolicy      OutputPolicy      `json:"outputPolicy"`
	Messages          []message.Message `json:"messages"`
	Usage             provider.Usage    `json:"usage"`
	ContextUsage      provider.Usage    `json:"contextUsage,omitzero"`
	Steps             []Step            `json:"steps"`
	ToolCallsUsed     int               `json:"toolCallsUsed"`
	RepairCount       int               `json:"repairCount"`
	ActiveElapsed     time.Duration     `json:"activeElapsed"`
	NextOperationTurn int               `json:"nextOperationTurn"`
	Phase             ContinuationPhase `json:"phase"`
}

var continuationV1Fields = []string{
	"schemaVersion",
	"request",
	"outputPolicy",
	"messages",
	"usage",
	"steps",
	"toolCallsUsed",
	"repairCount",
	"activeElapsed",
	"nextOperationTurn",
	"phase",
}

// EncodeContinuation validates continuation and returns its canonical JSON
// representation. Every top-level recovery field is emitted.
func EncodeContinuation(continuation Continuation) ([]byte, error) {
	if err := ValidateContinuation(continuation); err != nil {
		return nil, err
	}
	var wire any = continuationWireV2(continuation)
	if continuation.SchemaVersion == 1 {
		wire = continuationWireV1{
			SchemaVersion: continuation.SchemaVersion, Request: continuation.Request,
			OutputPolicy: continuation.OutputPolicy, Messages: continuation.Messages,
			Usage: continuation.Usage, Steps: continuation.Steps,
			ToolCallsUsed: continuation.ToolCallsUsed, RepairCount: continuation.RepairCount,
			ActiveElapsed: continuation.ActiveElapsed, NextOperationTurn: continuation.NextOperationTurn,
			Phase: continuation.Phase,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, continuationError("encode continuation: %v", err)
	}
	canonical, err := canonicalizeContinuationDocument(encoded)
	if err != nil {
		return nil, continuationError("canonicalize continuation: %v", err)
	}
	return canonical, nil
}

// DecodeContinuation decodes one strict continuation JSON object. Missing,
// unknown, duplicate, trailing, or unsupported-version state fails closed.
func DecodeContinuation(data []byte) (Continuation, error) {
	if err := validateContinuationJSONDocument(data); err != nil {
		return Continuation{}, continuationError("decode JSON document: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := decodeContinuationJSON(data, &fields, false); err != nil {
		return Continuation{}, continuationError("decode top-level object: %v", err)
	}
	if fields == nil {
		return Continuation{}, continuationError("top-level value must be an object")
	}
	versionRaw, ok := fields["schemaVersion"]
	if !ok {
		return Continuation{}, continuationError("missing top-level field %q", "schemaVersion")
	}
	if isJSONNull(versionRaw) {
		return Continuation{}, continuationError("schemaVersion must be an integer")
	}
	var version int
	if err := json.Unmarshal(versionRaw, &version); err != nil {
		return Continuation{}, continuationError("schemaVersion must be an integer: %v", err)
	}
	if version != 1 && version != ContinuationSchemaVersion {
		return Continuation{}, continuationError("unsupported schema version %d", version)
	}
	if version == 1 {
		var requestFields map[string]json.RawMessage
		if err := json.Unmarshal(fields["request"], &requestFields); err != nil {
			return Continuation{}, continuationError("decode v1 request: %v", err)
		}
		if _, exists := requestFields["content"]; exists {
			return Continuation{}, continuationError("request content requires schema version 2")
		}
		var policyFields map[string]json.RawMessage
		if err := json.Unmarshal(fields["outputPolicy"], &policyFields); err != nil {
			return Continuation{}, continuationError("decode v1 output policy: %v", err)
		}
		if _, exists := policyFields["native"]; exists {
			return Continuation{}, continuationError("native output requires schema version 2")
		}
		return decodeContinuationV1(data, fields)
	}
	return decodeContinuationV2(data, fields)
}

// MarshalJSON enforces the same strict, canonical wire contract used by
// persistence backends.
func (continuation Continuation) MarshalJSON() ([]byte, error) {
	return EncodeContinuation(continuation)
}

// UnmarshalJSON enforces the same strict wire contract used by
// DecodeContinuation.
func (continuation *Continuation) UnmarshalJSON(data []byte) error {
	if continuation == nil {
		return continuationError("decode into nil continuation")
	}
	decoded, err := DecodeContinuation(data)
	if err != nil {
		return err
	}
	*continuation = decoded
	return nil
}

func decodeContinuationV1(data []byte, fields map[string]json.RawMessage) (Continuation, error) {
	if err := validateContinuationV1Fields(fields); err != nil {
		return Continuation{}, err
	}
	for _, name := range []string{
		"schemaVersion",
		"request",
		"outputPolicy",
		"usage",
		"toolCallsUsed",
		"repairCount",
		"activeElapsed",
		"nextOperationTurn",
		"phase",
	} {
		if isJSONNull(fields[name]) {
			return Continuation{}, continuationError("top-level field %q must not be null", name)
		}
	}
	if raw, ok := fields["steps"]; ok && !isJSONNull(raw) {
		var steps []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &steps); err != nil {
			return Continuation{}, continuationError("decode steps: %v", err)
		}
		for index, step := range steps {
			if _, exists := step["contextUsage"]; exists {
				return Continuation{}, continuationError("version 1 step %d does not support contextUsage", index)
			}
		}
	}

	var wire continuationWireV1
	if err := decodeContinuationJSON(data, &wire, true); err != nil {
		return Continuation{}, continuationError("decode continuation: %v", err)
	}
	continuation := Continuation{
		SchemaVersion: wire.SchemaVersion, Request: wire.Request, OutputPolicy: wire.OutputPolicy,
		Messages: wire.Messages, Usage: wire.Usage, Steps: wire.Steps,
		ToolCallsUsed: wire.ToolCallsUsed, RepairCount: wire.RepairCount,
		ActiveElapsed: wire.ActiveElapsed, NextOperationTurn: wire.NextOperationTurn, Phase: wire.Phase,
	}
	normalizeContinuationContent(&continuation)
	if err := ValidateContinuation(continuation); err != nil {
		return Continuation{}, err
	}
	return cloneContinuation(continuation), nil
}
func decodeContinuationV2(data []byte, fields map[string]json.RawMessage) (Continuation, error) {
	if err := validateContinuationV2Fields(fields); err != nil {
		return Continuation{}, err
	}
	for _, name := range []string{
		"schemaVersion",
		"request",
		"outputPolicy",
		"usage",
		"toolCallsUsed",
		"repairCount",
		"activeElapsed",
		"nextOperationTurn",
		"phase",
	} {
		if isJSONNull(fields[name]) {
			return Continuation{}, continuationError("top-level field %q must not be null", name)
		}
	}
	if raw, ok := fields["contextUsage"]; ok && isJSONNull(raw) {
		return Continuation{}, continuationError("top-level field %q must not be null", "contextUsage")
	}
	if raw, ok := fields["steps"]; ok && !isJSONNull(raw) {
		var steps []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &steps); err != nil {
			return Continuation{}, continuationError("decode steps: %v", err)
		}
		for index, step := range steps {
			if contextRaw, exists := step["contextUsage"]; exists && isJSONNull(contextRaw) {
				return Continuation{}, continuationError("step %d contextUsage must not be null", index)
			}
		}
	}

	var wire continuationWireV2
	if err := decodeContinuationJSON(data, &wire, true); err != nil {
		return Continuation{}, continuationError("decode continuation: %v", err)
	}
	continuation := Continuation(wire)
	normalizeContinuationContent(&continuation)
	if err := ValidateContinuation(continuation); err != nil {
		return Continuation{}, err
	}
	return cloneContinuation(continuation), nil
}

func normalizeContinuationContent(continuation *Continuation) {
	for index := range continuation.Messages {
		current := &continuation.Messages[index]
		if current.Content == nil &&
			(current.Text != "" || current.Thinking != "" || current.RedactedThinking != "" ||
				(current.Role == message.RoleAssistant && len(current.ToolCalls) > 0)) {
			current.Content = current.CanonicalContent()
		}
		if current.ToolResult != nil && current.ToolResult.Parts == nil {
			current.ToolResult.Parts = current.ToolResult.CanonicalContent()
		}
	}
}

func validateContinuationV1Fields(fields map[string]json.RawMessage) error {
	required := make(map[string]struct{}, len(continuationV1Fields))
	for _, name := range continuationV1Fields {
		required[name] = struct{}{}
		if _, ok := fields[name]; !ok {
			return continuationError("missing top-level field %q", name)
		}
	}
	var unknown []string
	for name := range fields {
		if _, ok := required[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return continuationError("unknown top-level field %q", unknown[0])
	}
	return nil
}
func validateContinuationV2Fields(fields map[string]json.RawMessage) error {
	allowed := make(map[string]struct{}, len(continuationV1Fields)+1)
	for _, name := range continuationV1Fields {
		allowed[name] = struct{}{}
		if _, ok := fields[name]; !ok {
			return continuationError("missing top-level field %q", name)
		}
	}
	allowed["contextUsage"] = struct{}{}
	var unknown []string
	for name := range fields {
		if _, ok := allowed[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return continuationError("unknown top-level field %q", unknown[0])
	}
	return nil
}

func decodeContinuationJSON(data []byte, target any, disallowUnknown bool) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireContinuationJSONEnd(decoder)
}

func canonicalizeContinuationDocument(data []byte) ([]byte, error) {
	if err := validateContinuationJSONDocument(data); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := requireContinuationJSONEnd(decoder); err != nil {
		return nil, err
	}
	return json.Marshal(document)
}

const (
	maxContinuationBytes      = 64 << 20
	maxContinuationDepth      = 128
	maxContinuationValues     = 524288
	maxContinuationCollection = 65536
)

func validateContinuationJSONDocument(data []byte) error {
	if err := message.CheckJSON(data, message.JSONLimits{
		Bytes: maxContinuationBytes, Depth: maxContinuationDepth,
		Values: maxContinuationValues, Collection: maxContinuationCollection,
	}); err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("top-level value must be an object")
	}
	return nil
}

func requireContinuationJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func isJSONNull(data []byte) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}
