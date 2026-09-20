package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Viking602/venat/provider"
)

func TestEngineModelConnectTimeout(t *testing.T) {
	engine := Engine{
		Provider: connectBlockingProvider{},
		LoopPolicy: LoopPolicy{ModelTimeouts: &ModelTimeoutPolicy{
			DisableDefaults: true,
			ConnectTimeout:  10 * time.Millisecond,
		}},
	}

	result := engine.Run(context.Background(), Request{Prompt: "connect"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, provider.ErrModelConnectTimeout) {
		t.Fatalf("failure = %#v, want model connect timeout", result.Failure)
	}
}

func TestEngineModelRequestTimeout(t *testing.T) {
	engine := Engine{
		Provider: requestBlockingProvider{},
		LoopPolicy: LoopPolicy{ModelTimeouts: &ModelTimeoutPolicy{
			DisableDefaults: true,
			RequestTimeout:  10 * time.Millisecond,
		}},
	}

	result := engine.Run(context.Background(), Request{Prompt: "request"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, provider.ErrModelRequestTimeout) {
		t.Fatalf("failure = %#v, want model request timeout", result.Failure)
	}
}

func TestEngineModelStreamIdleTimeout(t *testing.T) {
	engine := Engine{
		Provider: idleBlockingProvider{},
		LoopPolicy: LoopPolicy{ModelTimeouts: &ModelTimeoutPolicy{
			DisableDefaults:   true,
			StreamIdleTimeout: 10 * time.Millisecond,
		}},
	}

	result := engine.Run(context.Background(), Request{Prompt: "idle"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, provider.ErrStreamIdleTimeout) {
		t.Fatalf("failure = %#v, want stream idle timeout", result.Failure)
	}
}

type connectBlockingProvider struct{}

func (connectBlockingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "connect-blocking"}
}

func (connectBlockingProvider) Stream(ctx context.Context, _ provider.Request) (provider.Stream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type requestBlockingProvider struct{}

func (requestBlockingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "request-blocking"}
}

func (requestBlockingProvider) Stream(ctx context.Context, _ provider.Request) (provider.Stream, error) {
	return &requestBlockingStream{ctx: ctx}, nil
}

type requestBlockingStream struct {
	ctx context.Context
}

func (stream *requestBlockingStream) Recv() (provider.Event, error) {
	<-stream.ctx.Done()
	return provider.Event{}, stream.ctx.Err()
}

func (*requestBlockingStream) Close() error { return nil }

type idleBlockingProvider struct{}

func (idleBlockingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "idle-blocking"}
}

func (idleBlockingProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	return &idleBlockingStream{release: make(chan struct{})}, nil
}

type idleBlockingStream struct {
	release   chan struct{}
	closeOnce sync.Once
}

func (stream *idleBlockingStream) Recv() (provider.Event, error) {
	<-stream.release
	return provider.Event{}, context.Canceled
}

func (stream *idleBlockingStream) Close() error {
	stream.closeOnce.Do(func() { close(stream.release) })
	return nil
}
