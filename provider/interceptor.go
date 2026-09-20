package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"

	"github.com/Viking602/venat/message"
)

// ErrNotStarted proves that a provider request was not issued.
var ErrNotStarted = errors.New("provider request was not started")

// ErrStreamInterceptorProtocol reports an invalid interceptor implementation.
var ErrStreamInterceptorProtocol = errors.New("provider stream interceptor protocol violation")

// StreamInterceptor surrounds one provider request. It may call the supplied
// Driver zero or one time and must pass the Request through unchanged.
type StreamInterceptor interface {
	Stream(context.Context, Driver, Request) (Stream, error)
}

// StreamInterceptorFunc adapts a function to StreamInterceptor.
type StreamInterceptorFunc func(context.Context, Driver, Request) (Stream, error)

// Stream delegates to f.
func (f StreamInterceptorFunc) Stream(ctx context.Context, next Driver, request Request) (Stream, error) {
	return f(ctx, next, request)
}

// StreamInterceptorProtocolError identifies the interceptor stage that broke
// the zero-or-one-call or immutable-request contract.
type StreamInterceptorProtocolError struct {
	Index int
	Err   error
}

func (failure *StreamInterceptorProtocolError) Error() string {
	if failure == nil {
		return ErrStreamInterceptorProtocol.Error()
	}
	return fmt.Sprintf("provider stream interceptor %d: %v", failure.Index, failure.Err)
}

func (failure *StreamInterceptorProtocolError) Unwrap() []error {
	if failure == nil || failure.Err == nil {
		return []error{ErrStreamInterceptorProtocol}
	}
	return []error{ErrStreamInterceptorProtocol, failure.Err}
}

// ChainStreamInterceptors composes interceptors outermost-first. Nil entries
// are ignored; an empty chain returns nil.
func ChainStreamInterceptors(interceptors ...StreamInterceptor) StreamInterceptor {
	filtered := make([]StreamInterceptor, 0, len(interceptors))
	for _, interceptor := range interceptors {
		if interceptor != nil {
			filtered = append(filtered, interceptor)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return StreamInterceptorFunc(func(ctx context.Context, driver Driver, request Request) (Stream, error) {
		next := driver
		for index := len(filtered) - 1; index >= 0; index-- {
			next = streamInterceptorDriver{
				metadata: driver.Metadata,
				stream:   wrapStreamInterceptor(index, filtered[index], next),
			}
		}
		return next.Stream(ctx, cloneRequest(request))
	})
}

type streamInterceptorDriver struct {
	metadata func() Metadata
	stream   func(context.Context, Request) (Stream, error)
}

func (driver streamInterceptorDriver) Metadata() Metadata { return driver.metadata() }

func (driver streamInterceptorDriver) Stream(ctx context.Context, request Request) (Stream, error) {
	return driver.stream(ctx, request)
}

func wrapStreamInterceptor(index int, interceptor StreamInterceptor, next Driver) func(context.Context, Request) (stream Stream, err error) {
	return func(ctx context.Context, request Request) (stream Stream, err error) {
		expected, fingerprintErr := requestFingerprint(request)
		if fingerprintErr != nil {
			return nil, &StreamInterceptorProtocolError{Index: index, Err: fingerprintErr}
		}
		expectedContextUsage := request.ContextUsage

		var mu sync.Mutex
		calls := 0
		closed := false
		var protocolErr error
		guarded := streamInterceptorDriver{
			metadata: next.Metadata,
			stream: func(nextCtx context.Context, candidate Request) (Stream, error) {
				mu.Lock()
				if closed {
					mu.Unlock()
					return nil, errors.Join(ErrNotStarted, &StreamInterceptorProtocolError{Index: index, Err: errors.New("next called after interceptor returned")})
				}
				calls++
				callNumber := calls
				mu.Unlock()
				if callNumber > 1 {
					failure := &StreamInterceptorProtocolError{Index: index, Err: errors.New("next called more than once")}
					mu.Lock()
					protocolErr = failure
					mu.Unlock()
					return nil, failure
				}
				candidateFingerprint, candidateErr := requestFingerprint(candidate)
				if candidateErr != nil ||
					candidate.OperationID != request.OperationID ||
					!sameContextUsage(candidate.ContextUsage, expectedContextUsage) ||
					!bytes.Equal(candidateFingerprint, expected) {
					cause := candidateErr
					if cause == nil {
						cause = errors.New("request or operation ID was modified")
					}
					failure := &StreamInterceptorProtocolError{Index: index, Err: cause}
					mu.Lock()
					protocolErr = errors.Join(ErrNotStarted, failure)
					mu.Unlock()
					return nil, errors.Join(ErrNotStarted, failure)
				}
				candidate.ContextUsage = expectedContextUsage
				return next.Stream(nextCtx, cloneRequest(candidate))
			},
		}

		defer func() {
			mu.Lock()
			closed = true
			called := calls
			violation := protocolErr
			mu.Unlock()
			if recovered := recover(); recovered != nil {
				panicErr := &StreamInterceptorProtocolError{Index: index, Err: fmt.Errorf("panic: %v", recovered)}
				if called == 0 {
					panicErr.Err = errors.Join(ErrNotStarted, panicErr.Err)
				}
				stream = nil
				err = panicErr
				return
			}
			if violation != nil {
				stream = nil
				err = errors.Join(err, violation)
				return
			}
			if called == 0 && err != nil && !errors.Is(err, ErrNotStarted) {
				err = errors.Join(ErrNotStarted, err)
			}
		}()
		return interceptor.Stream(ctx, guarded, request)
	}
}

func requestFingerprint(request Request) ([]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode request for interceptor validation: %w", err)
	}
	return encoded, nil
}

func cloneRequest(request Request) Request {
	request.Messages = message.CloneMessages(request.Messages)
	request.Tools = slices.Clone(request.Tools)
	for index := range request.Tools {
		request.Tools[index].InputSchema = cloneJSONSchema(request.Tools[index].InputSchema)
	}
	request.Metadata = maps.Clone(request.Metadata)
	request.StopSequences = slices.Clone(request.StopSequences)
	if request.ResponseFormat != nil {
		responseFormat := *request.ResponseFormat
		responseFormat.RawSchema = append(json.RawMessage(nil), request.ResponseFormat.RawSchema...)
		if request.ResponseFormat.Schema != nil {
			schema := cloneJSONSchema(*request.ResponseFormat.Schema)
			responseFormat.Schema = &schema
		}
		request.ResponseFormat = &responseFormat
	}
	if request.ParallelToolCalls != nil {
		parallelToolCalls := *request.ParallelToolCalls
		request.ParallelToolCalls = &parallelToolCalls
	}
	request.ExtraBody = cloneExtraBody(request.ExtraBody)
	return request
}

func cloneJSONSchema(schema message.JSONSchema) message.JSONSchema {
	schema.Required = slices.Clone(schema.Required)
	schema.Enum = slices.Clone(schema.Enum)
	if schema.Properties != nil {
		schema.Properties = maps.Clone(schema.Properties)
		for name, property := range schema.Properties {
			schema.Properties[name] = cloneJSONSchema(property)
		}
	}
	if schema.Items != nil {
		items := cloneJSONSchema(*schema.Items)
		schema.Items = &items
	}
	if schema.AdditionalProperties != nil {
		additionalProperties := *schema.AdditionalProperties
		schema.AdditionalProperties = &additionalProperties
	}
	return schema
}

func sameContextUsage(left, right ContextUsageObserver) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return reflect.ValueOf(left).Pointer() == reflect.ValueOf(right).Pointer()
}
