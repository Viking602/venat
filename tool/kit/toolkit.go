package kit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/tool"
)

type ToolOption func(*toolConfig)

type toolConfig struct {
	description      string
	terminal         bool
	timeout          time.Duration
	concurrency      tool.ConcurrencyMode
	concurrencyGroup string
	maxConcurrency   int
}

func Description(description string) ToolOption {
	return func(cfg *toolConfig) {
		cfg.description = description
	}
}

func Timeout(timeout time.Duration) ToolOption {
	return func(cfg *toolConfig) {
		cfg.timeout = timeout
	}
}

func Terminal() ToolOption {
	return func(cfg *toolConfig) {
		cfg.terminal = true
	}
}

func Concurrency(mode tool.ConcurrencyMode) ToolOption {
	return func(cfg *toolConfig) {
		cfg.concurrency = mode
	}
}

func ConcurrencyGroup(group string) ToolOption {
	return func(cfg *toolConfig) {
		cfg.concurrencyGroup = group
	}
}

func MaxConcurrency(limit int) ToolOption {
	return func(cfg *toolConfig) {
		cfg.maxConcurrency = limit
	}
}

type BundleSpec struct {
	Tools []tool.Driver
}

func Bundle(items ...any) BundleSpec {
	spec := BundleSpec{}
	for _, item := range items {
		switch current := item.(type) {
		case tool.Driver:
			spec.Tools = append(spec.Tools, current)
		case BundleSpec:
			spec.Tools = append(spec.Tools, current.Tools...)
		}
	}
	return spec
}

type Middleware func(tool.Driver) tool.Driver

func ChainMiddlewares(driver tool.Driver, middlewares ...Middleware) tool.Driver {
	current := driver
	for idx := len(middlewares) - 1; idx >= 0; idx-- {
		current = middlewares[idx](current)
	}
	return current
}

// Tool wraps a typed function. Return tool.Result with IsError for a completed
// domain failure the model can correct. Ordinary Go errors remain fatal, except
// a confirmed exec.ExitError with an active context becomes process feedback.
func Tool(name string, fn any, options ...ToolOption) (tool.Driver, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("tool name is required")
	}
	cfg := toolConfig{}
	for _, option := range options {
		option(&cfg)
	}
	builder, err := newFunctionTool(name, fn, cfg)
	if err != nil {
		return nil, err
	}
	return builder, nil
}

func definitionFromConfig(name string, schema tool.Schema, cfg toolConfig) tool.Definition {
	return tool.Definition{
		Name:             name,
		Description:      cfg.description,
		InputSchema:      schema,
		Terminal:         cfg.terminal,
		Timeout:          cfg.timeout,
		Concurrency:      cfg.concurrency,
		ConcurrencyGroup: cfg.concurrencyGroup,
		MaxConcurrency:   cfg.maxConcurrency,
	}
}

type functionTool struct {
	definition tool.Definition
	fn         reflect.Value
	inputType  reflect.Type
	outputType reflect.Type
	wantsCtx   bool
	wantsSink  bool
}

func newFunctionTool(name string, fn any, cfg toolConfig) (*functionTool, error) {
	value := reflect.ValueOf(fn)
	if value.Kind() != reflect.Func {
		return nil, fmt.Errorf("tool %s expects a function", name)
	}
	signature := value.Type()
	wantsCtx := false
	wantsSink := false
	index := 0
	if signature.NumIn() > 0 && signature.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem() {
		wantsCtx = true
		index++
	}
	if signature.NumIn()-index < 1 || signature.NumIn()-index > 2 {
		return nil, fmt.Errorf("tool %s expects func([context.Context], In[, tool.UpdateSink]) (Out, error)", name)
	}
	inputType := signature.In(index)
	if signature.NumIn()-index == 2 {
		if signature.In(index+1) != reflect.TypeOf((tool.UpdateSink)(nil)) {
			return nil, fmt.Errorf("tool %s received unsupported second argument", name)
		}
		wantsSink = true
	}
	if signature.NumOut() != 2 || !signature.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil, fmt.Errorf("tool %s expects (Out, error) return values", name)
	}
	outputType := signature.Out(0)
	schema, err := schemaFor(inputType)
	if err != nil {
		return nil, err
	}
	return &functionTool{
		definition: definitionFromConfig(name, schema, cfg),
		fn:         value,
		inputType:  inputType,
		outputType: outputType,
		wantsCtx:   wantsCtx,
		wantsSink:  wantsSink,
	}, nil
}

func (t *functionTool) Definition() tool.Definition {
	return t.definition
}

func (t *functionTool) Execute(ctx context.Context, call tool.Call, sink tool.UpdateSink) (tool.Result, error) {
	if err := ctx.Err(); err != nil {
		return tool.Result{}, errors.Join(tool.ErrNotExecuted, err)
	}
	inputValue, err := decodeInput(t.inputType, call.Arguments)
	if err != nil {
		return resultFromPayload(call, []byte(fmt.Sprintf("%s rejected: %v", call.Name, err)), true), nil
	}
	args := make([]reflect.Value, 0, 3)
	if t.wantsCtx {
		args = append(args, reflect.ValueOf(ctx))
	}
	args = append(args, inputValue)
	var outputStreamed atomic.Bool
	if t.wantsSink {
		forward := tool.UpdateSink(func(update tool.Update) error {
			if update.Kind == tool.UpdateOutput {
				outputStreamed.Store(true)
			}
			if sink != nil {
				return sink(update)
			}
			return nil
		})
		args = append(args, reflect.ValueOf(forward))
	}
	values := t.fn.Call(args)
	var exited *exec.ExitError
	if errValue := values[1].Interface(); errValue != nil {
		executionErr := errValue.(error)
		exited = completedProcessExit(executionErr)
		if ctx.Err() != nil || exited == nil {
			return tool.Result{}, errors.Join(executionErr, ctx.Err())
		}
	}
	output := values[0].Interface()
	result, explicit := output.(tool.Result)
	if explicit {
		result = message.CloneToolResult(result)
	} else {
		structured, err := json.Marshal(output)
		if err != nil {
			return tool.Result{}, err
		}
		result = tool.Result{Content: string(structured), Structured: structured}
		if t.outputType.Kind() == reflect.String {
			result.Content = values[0].String()
		}
	}
	result.ToolCallID, result.Name = call.ID, call.Name
	if exited != nil {
		suffix := "\n" + processExitDescription(exited)
		if len(exited.Stderr) > 0 {
			suffix += "\n" + string(exited.Stderr)
		}
		if sink != nil && outputStreamed.Load() {
			if err := sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart(suffix)}}); err != nil {
				return tool.Result{}, err
			}
		}
		if len(result.Parts) > 0 {
			result.Parts = append(result.Parts, message.TextPart(suffix))
			result.SyncLegacyContent()
		} else {
			result.Content += suffix
		}
		result.IsError = true
	}
	return result, nil
}

func decodeInput(target reflect.Type, payload json.RawMessage) (reflect.Value, error) {
	value := reflect.New(target)
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	if target.Kind() != reflect.Struct {
		value = reflect.New(target)
		if err := json.Unmarshal(payload, value.Interface()); err != nil {
			return reflect.Value{}, err
		}
		return value.Elem(), nil
	}
	if err := requireJSONFields(target, payload); err != nil {
		return reflect.Value{}, err
	}
	if err := json.Unmarshal(payload, value.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return value.Elem(), nil
}

func requireJSONFields(target reflect.Type, payload json.RawMessage) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return err
	}
	for idx := 0; idx < target.NumField(); idx++ {
		field := target.Field(idx)
		if !field.IsExported() {
			continue
		}
		name := field.Tag.Get("json")
		name = strings.Split(name, ",")[0]
		if name == "" {
			name = lowerCamel(field.Name)
		}
		if name == "-" || strings.Contains(field.Tag.Get("json"), "omitempty") {
			continue
		}
		if _, ok := raw[name]; !ok {
			return fmt.Errorf("missing required field %q", name)
		}
	}
	return nil
}

func schemaFor(current reflect.Type) (message.JSONSchema, error) {
	return schemaForVisited(current, map[reflect.Type]struct{}{})
}

func schemaForVisited(current reflect.Type, visiting map[reflect.Type]struct{}) (message.JSONSchema, error) {
	for current.Kind() == reflect.Pointer {
		current = current.Elem()
	}
	switch current.Kind() {
	case reflect.Struct:
		if _, seen := visiting[current]; seen {
			additional := true
			return message.JSONSchema{Type: "object", AdditionalProperties: &additional}, nil
		}
		visiting[current] = struct{}{}
		defer delete(visiting, current)
		properties := map[string]message.JSONSchema{}
		required := make([]string, 0, current.NumField())
		for idx := 0; idx < current.NumField(); idx++ {
			field := current.Field(idx)
			if !field.IsExported() {
				continue
			}
			name := field.Tag.Get("json")
			name = strings.Split(name, ",")[0]
			if name == "" {
				name = lowerCamel(field.Name)
			}
			if name == "-" {
				continue
			}
			child, err := schemaForVisited(field.Type, visiting)
			if err != nil {
				return message.JSONSchema{}, err
			}
			if description := field.Tag.Get("description"); description != "" {
				child.Description = description
			}
			properties[name] = child
			if !strings.Contains(field.Tag.Get("json"), "omitempty") {
				required = append(required, name)
			}
		}
		additional := false
		return message.JSONSchema{
			Type:                 "object",
			Properties:           properties,
			Required:             required,
			AdditionalProperties: &additional,
		}, nil
	case reflect.Slice, reflect.Array:
		items, err := schemaForVisited(current.Elem(), visiting)
		if err != nil {
			return message.JSONSchema{}, err
		}
		return message.JSONSchema{
			Type:  "array",
			Items: &items,
		}, nil
	case reflect.Bool:
		return message.JSONSchema{Type: "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return message.JSONSchema{Type: "integer"}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return message.JSONSchema{Type: "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return message.JSONSchema{Type: "number"}, nil
	case reflect.Map:
		additional := true
		return message.JSONSchema{Type: "object", AdditionalProperties: &additional}, nil
	case reflect.String:
		return message.JSONSchema{Type: "string"}, nil
	default:
		return message.JSONSchema{}, fmt.Errorf("unsupported schema type: %s", current.Kind())
	}
}

func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	return strings.ToLower(value[:1]) + value[1:]
}
