package kit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Viking602/venat/tool"
)

func TestHTTPTool_RejectsCrossOriginRedirectBeforeSendingHeaders(t *testing.T) {
	var received atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if r.Header.Get("x-api-key") != "" {
			t.Error("credentials reached redirect target")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	for _, custom := range []bool{false, true} {
		var client *http.Client
		var checks int
		if custom {
			client = origin.Client()
			client.CheckRedirect = func(*http.Request, []*http.Request) error { checks++; return nil }
		}
		driver := HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{
			URL: origin.URL, Client: client, Headers: map[string]string{"x-api-key": "secret"},
		})
		_, err := driver.Execute(context.Background(), tool.Call{Name: "remote", Arguments: json.RawMessage(`{}`)}, nil)
		if err == nil || !strings.Contains(err.Error(), "cross-origin") || checks != 0 {
			t.Fatalf("custom=%v error=%v custom checks=%d", custom, err, checks)
		}
		if errors.Is(err, tool.ErrNotExecuted) {
			t.Fatal("redirect rejection incorrectly proves original request was not executed")
		}
	}
	if received.Load() != 0 {
		t.Fatalf("redirect target received %d requests", received.Load())
	}
}

func TestHTTPTool_PreservesSameOriginPolicyAndOwnsHeaders(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "original" {
			t.Error("retained headers changed")
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/final", http.StatusTemporaryRedirect)
			return
		}
		_, _ = w.Write([]byte("done"))
	}))
	defer origin.Close()
	client := origin.Client()
	checks := 0
	client.CheckRedirect = func(*http.Request, []*http.Request) error { checks++; return nil }
	headers := map[string]string{"x-api-key": "original"}
	driver := HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{URL: origin.URL, Client: client, Headers: headers})
	headers["x-api-key"] = "changed"
	result, err := driver.Execute(context.Background(), tool.Call{Name: "remote", Arguments: json.RawMessage(`{}`)}, nil)
	if err != nil || result.Content != "done" || checks != 1 {
		t.Fatalf("result=%+v error=%v checks=%d", result, err, checks)
	}
	// The caller can still use its own policy independently of the wrapper.
	if err := client.CheckRedirect(nil, nil); err != nil || checks != 2 {
		t.Fatal("caller client was mutated")
	}
}

func TestHTTPTool_RejectsOriginChangesByCustomRedirectHook(t *testing.T) {
	var received atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/local", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client := origin.Client()
	client.CheckRedirect = func(request *http.Request, _ []*http.Request) error {
		request.URL, _ = url.Parse(target.URL)
		return nil
	}
	driver := HTTPTool("remote", tool.Schema{Type: "object"}, HTTPToolConfig{URL: origin.URL, Client: client, Headers: map[string]string{"x-api-key": "secret"}})
	_, err := driver.Execute(context.Background(), tool.Call{Name: "remote", Arguments: json.RawMessage(`{}`)}, nil)
	if err == nil || received.Load() != 0 {
		t.Fatalf("custom hook escaped confinement: error=%v requests=%d", err, received.Load())
	}
}
