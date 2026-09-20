package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

const attemptEnvelopeVersion = 1

type modelAttemptEnvelope struct {
	Version int                   `json:"version"`
	Events  []storedProviderEvent `json:"events,omitempty"`
	Failure *FailureRecord        `json:"failure,omitempty"`
}

type storedProviderEvent struct {
	Event            provider.Event `json:"event"`
	Failure          *FailureRecord `json:"failure,omitempty"`
	InvalidArguments *string        `json:"invalidArguments,omitempty"`
}

type toolAttemptEnvelope struct {
	Version int            `json:"version"`
	Result  tool.Result    `json:"result"`
	Failure *FailureRecord `json:"failure,omitempty"`
}

func cloneProviderEvent(event provider.Event) provider.Event {
	event.ProviderState = append(json.RawMessage(nil), event.ProviderState...)
	if event.ToolCall != nil {
		call := *event.ToolCall
		call.Arguments = append(json.RawMessage(nil), event.ToolCall.Arguments...)
		event.ToolCall = &call
	}
	if event.ToolCallDelta != nil {
		delta := *event.ToolCallDelta
		if event.ToolCallDelta.Index != nil {
			index := *event.ToolCallDelta.Index
			delta.Index = &index
		}
		event.ToolCallDelta = &delta
	}
	if event.Response.Headers != nil {
		headers := make(map[string]string, len(event.Response.Headers))
		for key, value := range event.Response.Headers {
			headers[key] = value
		}
		event.Response.Headers = headers
	}
	return event
}

func encodeModelAttempt(events []provider.Event, failure *FailureRecord) ([]byte, error) {
	stored := make([]storedProviderEvent, len(events))
	version := attemptEnvelopeVersion
	for index, event := range events {
		stored[index] = storedProviderEvent{Event: cloneProviderEvent(event), Failure: failureFromError(event.Err)}
		stored[index].Event.Err = nil
		if event.ToolCall != nil && len(event.ToolCall.Arguments) > 0 && !json.Valid(event.ToolCall.Arguments) {
			// Preserve the exact event kind and raw text. Converting a full call to
			// a delta could join earlier fragments into an executable call on replay.
			arguments := string(event.ToolCall.Arguments)
			stored[index].InvalidArguments = &arguments
			stored[index].Event.ToolCall.Arguments = nil
			version = 2 // old workers fail closed rather than replay empty arguments
		}
	}
	encoded, err := json.Marshal(modelAttemptEnvelope{Version: version, Events: stored, Failure: cloneFailureRecord(failure)})
	if err != nil {
		return nil, fmt.Errorf("encode model attempt: %w", err)
	}
	return encoded, nil
}

func decodeModelAttempt(payload []byte) ([]provider.Event, *FailureRecord, error) {
	var envelope modelAttemptEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode model attempt: %w", err)
	}
	if envelope.Version != attemptEnvelopeVersion && envelope.Version != 2 {
		return nil, nil, fmt.Errorf("unsupported model attempt envelope version %d", envelope.Version)
	}
	events := make([]provider.Event, len(envelope.Events))
	for index, stored := range envelope.Events {
		events[index] = stored.Event
		if stored.InvalidArguments != nil {
			if envelope.Version != 2 || stored.Event.Kind != provider.EventToolCall || stored.Event.ToolCall == nil || len(stored.Event.ToolCall.Arguments) != 0 || *stored.InvalidArguments == "" || json.Valid([]byte(*stored.InvalidArguments)) {
				return nil, nil, errors.New("invalid stored tool arguments envelope")
			}
			events[index].ToolCall.Arguments = []byte(*stored.InvalidArguments)
		}
		if stored.Failure != nil {
			events[index].Err = recordedFailureError(*stored.Failure)
		}
	}
	return events, cloneFailureRecord(envelope.Failure), nil
}

func encodeToolAttempt(result tool.Result, failure *FailureRecord) ([]byte, error) {
	encoded, err := json.Marshal(toolAttemptEnvelope{
		Version: attemptEnvelopeVersion,
		Result:  result,
		Failure: cloneFailureRecord(failure),
	})
	if err != nil {
		return nil, fmt.Errorf("encode tool attempt: %w", err)
	}
	return encoded, nil
}

func decodeToolAttempt(payload []byte) (tool.Result, *FailureRecord, error) {
	var envelope toolAttemptEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return tool.Result{}, nil, fmt.Errorf("decode tool attempt: %w", err)
	}
	if envelope.Version != attemptEnvelopeVersion {
		return tool.Result{}, nil, fmt.Errorf("unsupported tool attempt envelope version %d", envelope.Version)
	}
	return envelope.Result, cloneFailureRecord(envelope.Failure), nil
}

func failureFromError(err error) *FailureRecord {
	if err == nil {
		return nil
	}
	code := "error"
	switch {
	case errors.Is(err, provider.ErrNotStarted):
		code = "provider_not_started"
	case errors.Is(err, tool.ErrNotExecuted):
		code = "tool_not_executed"
	case errors.Is(err, context.Canceled):
		code = "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code = "deadline_exceeded"
	}
	return &FailureRecord{Code: code, Message: err.Error()}
}

func cloneFailureRecord(failure *FailureRecord) *FailureRecord {
	if failure == nil {
		return nil
	}
	cloned := *failure
	return &cloned
}

type recordedFailureError FailureRecord

func (failure recordedFailureError) Error() string {
	if failure.Message != "" {
		return failure.Message
	}
	return failure.Code
}

func (failure recordedFailureError) Unwrap() error {
	switch failure.Code {
	case "provider_not_started":
		return provider.ErrNotStarted
	case "tool_not_executed":
		return tool.ErrNotExecuted
	case "context_canceled":
		return context.Canceled
	case "deadline_exceeded":
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func validateSuccessfulModelEvents(events []provider.Event) error {
	if len(events) == 0 || events[len(events)-1].Kind != provider.EventDone {
		return fmt.Errorf("successful model reconciliation requires a final done event")
	}
	_, err := provider.NormalizeEvents(events)
	if errors.Is(err, provider.ErrInvalidToolCallArguments) {
		return nil // a completed, rejected model turn is safe to replay for correction
	}
	return err
}

func validateFailedModelEvents(events []provider.Event) error {
	if len(events) == 0 {
		return nil
	}
	for index, event := range events {
		if event.Kind != provider.EventDone && event.Kind != provider.EventError {
			continue
		}
		if index != len(events)-1 {
			return provider.ErrEventAfterTerminal
		}
		if event.Kind == provider.EventDone {
			return errors.New("failed model reconciliation cannot contain a done event")
		}
		_, err := provider.NormalizePartialEvents(events[:index])
		return err
	}
	_, err := provider.NormalizePartialEvents(events)
	return err
}
