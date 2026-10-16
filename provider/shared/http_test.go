package shared

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestClientOrDefault_ConfinesCallerClientRedirects(t *testing.T) {
	leaked := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		leaked <- request.Header.Get("x-api-key")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	tests := []struct {
		name  string
		check func(*http.Request, []*http.Request) error
	}{
		{name: "caller client without redirect policy"},
		{name: "caller policy that permits every hop", check: func(*http.Request, []*http.Request) error { return nil }},
		{name: "caller policy that rewrites the hop off-origin", check: func(request *http.Request, _ []*http.Request) error {
			request.URL, _ = url.Parse(target.URL)
			return nil
		}},
		{name: "caller policy that mutates via origin to match off-origin hop", check: func(request *http.Request, via []*http.Request) error {
			request.URL, _ = url.Parse(target.URL)
			via[0].URL, _ = url.Parse(target.URL)
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := &http.Client{CheckRedirect: tt.check}
			request, err := http.NewRequest(http.MethodPost, origin.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("x-api-key", "secret")
			response, err := ClientOrDefault(caller, time.Second).Do(request)
			if response != nil {
				_ = response.Body.Close()
			}
			if !errors.Is(err, ErrCrossOriginRedirect) {
				t.Fatalf("Do error = %v, want ErrCrossOriginRedirect", err)
			}
			select {
			case key := <-leaked:
				t.Fatalf("cross-origin target received x-api-key %q", key)
			default:
			}
			if (caller.CheckRedirect == nil) != (tt.check == nil) {
				t.Fatal("caller client was mutated")
			}
		})
	}
}
