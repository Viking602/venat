package provider

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Viking602/venat/message"
)

type interceptorTestDriver struct {
	calls    int
	requests []Request
}

func (*interceptorTestDriver) Metadata() Metadata { return Metadata{Name: "test"} }

func (driver *interceptorTestDriver) Stream(_ context.Context, request Request) (Stream, error) {
	driver.calls++
	driver.requests = append(driver.requests, request)
	return NewSliceStream([]Event{{Kind: EventDone, StopReason: StopReasonComplete}}), nil
}

func TestChainStreamInterceptors_OrderAndSingleExecution(t *testing.T) {
	driver := &interceptorTestDriver{}
	var order []string
	outer := StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
		order = append(order, "outer-before")
		stream, err := next.Stream(ctx, request)
		order = append(order, "outer-after")
		return stream, err
	})
	inner := StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
		order = append(order, "inner-before")
		stream, err := next.Stream(ctx, request)
		order = append(order, "inner-after")
		return stream, err
	})

	stream, err := ChainStreamInterceptors(outer, nil, inner).Stream(context.Background(), driver, Request{Model: "model", OperationID: "turn:0:model"})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if stream == nil {
		t.Fatal("Stream() returned nil stream")
	}
	if driver.calls != 1 {
		t.Fatalf("underlying calls = %d, want 1", driver.calls)
	}
	wantOrder := []string{"outer-before", "inner-before", "inner-after", "outer-after"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order = %v, want %v", order, wantOrder)
	}
}

func TestStreamInterceptorIsolatesMutableRequestMembers(t *testing.T) {
	newRequest := func() Request {
		additionalProperties := false
		parallelToolCalls := true
		current := message.NewText(message.RoleUser, "work")
		current.Metadata = map[string]string{"source": "caller"}
		return Request{
			Model:       "model",
			OperationID: "turn:0:model",
			Messages:    []message.Message{current},
			Tools: []message.ToolDefinition{{
				Name: "lookup",
				InputSchema: message.JSONSchema{
					Type:       "object",
					Required:   []string{"query"},
					Properties: map[string]message.JSONSchema{"query": {Type: "string"}},
				},
			}},
			Metadata:      map[string]string{"scope": "caller"},
			StopSequences: []string{"STOP"},
			ResponseFormat: &ResponseFormat{
				Type:      "json_schema",
				RawSchema: []byte(`{"type":"object"}`),
				Schema: &message.JSONSchema{
					Type:                 "object",
					Required:             []string{"answer"},
					AdditionalProperties: &additionalProperties,
				},
			},
			ParallelToolCalls: &parallelToolCalls,
			ExtraBody: map[string]any{
				"nested":  map[string]any{"enabled": true},
				"include": []string{"reasoning.encrypted_content"},
			},
		}
	}
	request := newRequest()
	want := newRequest()
	driver := &interceptorTestDriver{}
	interceptor := ChainStreamInterceptors(StreamInterceptorFunc(func(ctx context.Context, next Driver, current Request) (Stream, error) {
		stream, err := next.Stream(ctx, current)
		current.Messages[0].Metadata["source"] = "interceptor"
		current.Tools[0].InputSchema.Required[0] = "changed"
		current.Metadata["scope"] = "interceptor"
		current.StopSequences[0] = "CHANGED"
		current.ResponseFormat.Schema.Required[0] = "changed"
		current.ResponseFormat.RawSchema[0] = '['
		*current.ResponseFormat.Schema.AdditionalProperties = true
		*current.ParallelToolCalls = false
		current.ExtraBody["nested"].(map[string]any)["enabled"] = false
		current.ExtraBody["include"].([]string)[0] = "changed"
		return stream, err
	}))

	if _, err := interceptor.Stream(context.Background(), driver, request); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if !reflect.DeepEqual(request, want) {
		t.Fatalf("caller request mutated:\n got: %#v\nwant: %#v", request, want)
	}
	if len(driver.requests) != 1 || !reflect.DeepEqual(driver.requests[0], want) {
		t.Fatalf("driver request aliases interceptor input:\n got: %#v\nwant: %#v", driver.requests, want)
	}
}

func TestStreamInterceptorRejectsContextUsageMutation(t *testing.T) {
	driver := &interceptorTestDriver{}
	original := func(ContextUsage) {}
	replacement := func(ContextUsage) {}
	interceptor := ChainStreamInterceptors(StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
		request.ContextUsage = replacement
		return next.Stream(ctx, request)
	}))

	_, err := interceptor.Stream(context.Background(), driver, Request{
		Model:        "model",
		OperationID:  "turn:0:model",
		ContextUsage: original,
	})
	if !errors.Is(err, ErrStreamInterceptorProtocol) || !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Stream() error = %v, want protocol and not-started errors", err)
	}
	if driver.calls != 0 {
		t.Fatalf("underlying calls = %d, want 0", driver.calls)
	}
}

func TestStreamInterceptorRejectsMutationBeforeEffect(t *testing.T) {
	driver := &interceptorTestDriver{}
	interceptor := ChainStreamInterceptors(StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
		request.Model = "changed"
		return next.Stream(ctx, request)
	}))

	_, err := interceptor.Stream(context.Background(), driver, Request{Model: "model", OperationID: "turn:0:model"})
	if !errors.Is(err, ErrStreamInterceptorProtocol) || !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Stream() error = %v, want protocol and not-started errors", err)
	}
	if driver.calls != 0 {
		t.Fatalf("underlying calls = %d, want 0", driver.calls)
	}
}

func TestStreamInterceptorRejectsSecondEffect(t *testing.T) {
	driver := &interceptorTestDriver{}
	interceptor := ChainStreamInterceptors(StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
		if _, err := next.Stream(ctx, request); err != nil {
			return nil, err
		}
		return next.Stream(ctx, request)
	}))

	_, err := interceptor.Stream(context.Background(), driver, Request{Model: "model", OperationID: "turn:0:model"})
	if !errors.Is(err, ErrStreamInterceptorProtocol) {
		t.Fatalf("Stream() error = %v, want protocol error", err)
	}
	if errors.Is(err, ErrNotStarted) {
		t.Fatalf("Stream() error = %v, must not claim the first effect was not started", err)
	}
	if driver.calls != 1 {
		t.Fatalf("underlying calls = %d, want 1", driver.calls)
	}
}

func TestStreamInterceptorPanicPreservesStartCertainty(t *testing.T) {
	tests := []struct {
		name           string
		interceptor    StreamInterceptor
		wantCalls      int
		wantNotStarted bool
	}{
		{
			name: "before next",
			interceptor: StreamInterceptorFunc(func(context.Context, Driver, Request) (Stream, error) {
				panic("before")
			}),
			wantNotStarted: true,
		},
		{
			name: "after next",
			interceptor: StreamInterceptorFunc(func(ctx context.Context, next Driver, request Request) (Stream, error) {
				_, _ = next.Stream(ctx, request)
				panic("after")
			}),
			wantCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &interceptorTestDriver{}
			_, err := ChainStreamInterceptors(test.interceptor).Stream(context.Background(), driver, Request{Model: "model", OperationID: "turn:0:model"})
			if !errors.Is(err, ErrStreamInterceptorProtocol) {
				t.Fatalf("Stream() error = %v, want protocol error", err)
			}
			if errors.Is(err, ErrNotStarted) != test.wantNotStarted {
				t.Fatalf("errors.Is(ErrNotStarted) = %v, want %v", errors.Is(err, ErrNotStarted), test.wantNotStarted)
			}
			if driver.calls != test.wantCalls {
				t.Fatalf("underlying calls = %d, want %d", driver.calls, test.wantCalls)
			}
		})
	}
}

func TestStreamInterceptorRejectsLateEffect(t *testing.T) {
	driver := &interceptorTestDriver{}
	var late Driver
	interceptor := ChainStreamInterceptors(StreamInterceptorFunc(func(_ context.Context, next Driver, _ Request) (Stream, error) {
		late = next
		return NewSliceStream([]Event{{Kind: EventDone, StopReason: StopReasonComplete}}), nil
	}))
	request := Request{Model: "model", OperationID: "turn:0:model"}
	if _, err := interceptor.Stream(context.Background(), driver, request); err != nil {
		t.Fatalf("synthetic Stream() error = %v", err)
	}
	_, err := late.Stream(context.Background(), request)
	if !errors.Is(err, ErrStreamInterceptorProtocol) || !errors.Is(err, ErrNotStarted) {
		t.Fatalf("late Stream() error = %v, want protocol and not-started errors", err)
	}
	if driver.calls != 0 {
		t.Fatalf("underlying calls = %d, want 0", driver.calls)
	}
}
