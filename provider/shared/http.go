package shared

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// ErrCrossOriginRedirect is returned when an HTTP client refuses to follow
// a redirect to a different host or scheme. Custom headers such as
// x-api-key are not stripped by net/http on cross-origin redirects.
var ErrCrossOriginRedirect = errors.New("http: cross-origin redirect rejected")

// NewHTTPClient returns a streaming-safe client: no overall Timeout (the
// stream may be long-lived), a response-header timeout on the transport,
// and a CheckRedirect that refuses cross-origin hops so API keys cannot
// follow a 3xx off-origin.
func NewHTTPClient(headerTimeout time.Duration) *http.Client {
	if headerTimeout <= 0 {
		headerTimeout = 30 * time.Second
	}
	return &http.Client{
		Transport:     CloneHTTPTransport(headerTimeout),
		CheckRedirect: RejectCrossOriginRedirect,
	}
}

// ClientOrDefault returns a streaming-safe default when client is nil.
// Otherwise it returns a shallow copy of client whose redirects stay on the
// original scheme and host before and after the caller's own CheckRedirect
// runs; the caller's client is never mutated. Zero-value drivers that bypass
// New() must not fall back to http.DefaultClient (no timeouts).
func ClientOrDefault(client *http.Client, headerTimeout time.Duration) *http.Client {
	if client == nil {
		return NewHTTPClient(headerTimeout)
	}
	confined := *client
	check := client.CheckRedirect
	confined.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := RejectCrossOriginRedirect(req, via); err != nil {
			return err
		}
		scheme, host := via[0].URL.Scheme, via[0].URL.Host
		if check != nil {
			if err := check(req, via); err != nil {
				return err
			}
		}
		return sameOriginRedirect(req, scheme, host)
	}
	return &confined
}

// maxRedirects matches net/http's default redirect ceiling, which a custom
// CheckRedirect otherwise replaces.
const maxRedirects = 10

// CloneHTTPTransport copies http.DefaultTransport when it is a
// *http.Transport; otherwise it builds a conservative default. A custom
// DefaultTransport that is not a *http.Transport is used as-is so New
// cannot panic at startup.
func CloneHTTPTransport(headerTimeout time.Duration) http.RoundTripper {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		clone := transport.Clone()
		clone.ResponseHeaderTimeout = headerTimeout
		return clone
	}
	if http.DefaultTransport != nil {
		return http.DefaultTransport
	}
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: headerTimeout,
	}
	return transport
}

// RejectCrossOriginRedirect is an http.Client.CheckRedirect that allows
// same-origin hops and rejects everything else.
func RejectCrossOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("http: stopped after %d redirects", maxRedirects)
	}
	if req == nil || req.URL == nil || len(via) == 0 || via[0] == nil || via[0].URL == nil {
		return fmt.Errorf("%w: invalid redirect", ErrCrossOriginRedirect)
	}
	return sameOriginRedirect(req, via[0].URL.Scheme, via[0].URL.Host)
}

func sameOriginRedirect(req *http.Request, scheme, host string) error {
	if req == nil || req.URL == nil || !strings.EqualFold(scheme, req.URL.Scheme) || !strings.EqualFold(host, req.URL.Host) {
		toHost := ""
		if req != nil && req.URL != nil {
			toHost = req.URL.Host
		}
		return fmt.Errorf("%w: %s -> %s", ErrCrossOriginRedirect, host, toHost)
	}
	return nil
}

// IsEventStreamContentType reports whether contentType is text/event-stream.
func IsEventStreamContentType(contentType string) bool {
	if strings.TrimSpace(contentType) == "" {
		return false
	}
	mediaType := strings.TrimSpace(strings.Split(contentType, ";")[0])
	return strings.EqualFold(mediaType, "text/event-stream")
}
