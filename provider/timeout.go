package provider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrModelConnectTimeout identifies a provider stream that did not open
	// before its configured connection deadline.
	ErrModelConnectTimeout = errors.New("provider model connection timeout")
	// ErrModelRequestTimeout identifies a model request that exceeded its
	// configured total-request deadline.
	ErrModelRequestTimeout = errors.New("provider model request timeout")
	// ErrStreamIdleTimeout identifies a stream with no event in its idle window.
	ErrStreamIdleTimeout = errors.New("provider stream idle timeout")
)

// WithStreamIdleTimeout wraps a provider stream with a bounded gap between
// Recv results. Close is forwarded on timeout so HTTP-backed streams can stop
// their read and release the underlying connection.
func WithStreamIdleTimeout(ctx context.Context, stream Stream, timeout time.Duration) Stream {
	if timeout <= 0 {
		return stream
	}
	return &idleTimeoutStream{ctx: ctx, stream: stream, timeout: timeout}
}

type idleTimeoutStream struct {
	ctx     context.Context
	stream  Stream
	timeout time.Duration

	closeOnce sync.Once
	closeErr  error
}

type recvResult struct {
	event Event
	err   error
}

func (stream *idleTimeoutStream) Recv() (Event, error) {
	results := make(chan recvResult, 1)
	go func() {
		event, err := stream.stream.Recv()
		results <- recvResult{event: event, err: err}
	}()

	timer := time.NewTimer(stream.timeout)
	defer timer.Stop()
	select {
	case result := <-results:
		return result.event, result.err
	case <-stream.ctx.Done():
		_ = stream.Close()
		cause := context.Cause(stream.ctx)
		if cause == nil {
			cause = stream.ctx.Err()
		}
		if errors.Is(stream.ctx.Err(), context.DeadlineExceeded) && !errors.Is(cause, context.DeadlineExceeded) {
			return Event{}, errors.Join(cause, context.DeadlineExceeded)
		}
		return Event{}, cause
	case <-timer.C:
		_ = stream.Close()
		return Event{}, fmt.Errorf("%w: no event within %s", ErrStreamIdleTimeout, stream.timeout)
	}
}

// Identity preserves composite-driver attribution through the timeout layer.
func (stream *idleTimeoutStream) Identity() StreamIdentity {
	if identified, ok := stream.stream.(IdentifiedStream); ok {
		return identified.Identity()
	}
	return StreamIdentity{}
}

func (stream *idleTimeoutStream) Close() error {
	stream.closeOnce.Do(func() {
		stream.closeErr = stream.stream.Close()
	})
	return stream.closeErr
}
