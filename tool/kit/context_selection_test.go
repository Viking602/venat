package kit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Viking602/venat/tool"
)

func TestContextSelection_ExplicitProtocolAndTypedScores(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/v1/systemone" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer caller-secret" || strings.Contains(string(body), "caller-secret") {
			t.Error("incorrect endpoint, auth or payload")
		}
		var request struct {
			Model     string `json:"model"`
			Questions map[string]struct {
				Type         string          `json:"type"`
				Instructions json.RawMessage `json:"instructions"`
			} `json:"questions"`
		}
		if err := json.Unmarshal(body, &request); err != nil || request.Model != "jev-version" || len(request.Questions) != 1 || request.Questions["old"].Type != "noul" || !strings.Contains(string(request.Questions["old"].Instructions), `"candidateId":"old"`) {
			t.Errorf("request=%s err=%v", body, err)
		}
		_, _ = io.WriteString(w, `{"model":"jev-version","answers":{"old":{"type":"noul","noul":0.12}},"usage":{"input_tokens":25,"output_tokens":2}}`)
	}))
	defer server.Close()
	config := ContextSelectionConfig{Protocol: "typesafe-system-one", BaseURL: server.URL + "/v1", APIKey: "caller-secret", Model: "jev-version"}
	for _, field := range []string{"protocol", "url", "key", "model"} {
		bad := config
		switch field {
		case "protocol":
			bad.Protocol = "openai"
		case "url":
			bad.BaseURL = ""
		case "key":
			bad.APIKey = ""
		case "model":
			bad.Model = ""
		}
		if _, err := ContextSelectionTool("select_context", bad); err == nil {
			t.Fatalf("accepted missing/invalid %s", field)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("construction contacted network")
	}
	driver, err := ContextSelectionTool("select_context", config)
	if err != nil {
		t.Fatal(err)
	}
	bus := tool.NewBus(driver)
	input := `{"task":"fix build","candidates":[{"id":"rule","text":"do not deploy","protected":true},{"id":"old","text":"obsolete log","protected":false}]}`
	result, err := bus.Execute(context.Background(), tool.Call{ID: "c", Name: "select_context", Arguments: json.RawMessage(input)}, tool.ExecuteOptions{})
	if err != nil || result.IsError || calls.Load() != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	var output contextSelectionResult
	if err := json.Unmarshal(result.Structured, &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Candidates) != 2 || !output.Candidates[0].MustKeep || output.Candidates[0].RetainProbability != nil || *output.Candidates[1].RetainProbability != 0.12 || len(output.Candidates[1].SHA256) != 64 || output.TotalTokens != 27 {
		t.Fatalf("scores=%+v", output)
	}
	for _, args := range []string{
		strings.Replace(input, `"old"`, `"rule"`, 1),
		strings.Replace(input, "obsolete log", strings.Repeat("x", 17<<10), 1),
	} {
		rejected, err := bus.Execute(context.Background(), tool.Call{ID: "invalid", Name: "select_context", Arguments: json.RawMessage(args)}, tool.ExecuteOptions{})
		if err != nil || !rejected.IsError || calls.Load() != 1 {
			t.Fatal("invalid input contacted remote or aborted loop")
		}
	}
	encoded, _ := json.Marshal(config)
	if strings.Contains(string(encoded), config.APIKey) {
		t.Fatal("serialized credentials")
	}
}

func TestContextSelection_RejectsUnexpectedModelAndOversizedWireRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"model":"unexpected","answers":{"candidate":{"type":"noul","noul":0.5}},"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer server.Close()
	driver, err := ContextSelectionTool("select_context", ContextSelectionConfig{Protocol: "typesafe-system-one", BaseURL: server.URL, APIKey: "caller-secret", Model: "jev-version"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.Execute(context.Background(), tool.Call{Name: "select_context", Arguments: json.RawMessage(`{"task":"fix","candidates":[{"id":"candidate","text":"log","protected":false}]}`)}, nil)
	if err == nil || result.IsError || !strings.Contains(err.Error(), "model identity") {
		t.Fatalf("unexpected model result=%+v err=%v", result, err)
	}

	// The input JSON stays below 16 KiB, while the final wire request exceeds it
	// after model identity and Noul instructions are included.
	largeText := strings.Repeat("x", 16000)
	result, err = driver.Execute(context.Background(), tool.Call{Name: "select_context", Arguments: json.RawMessage(`{"task":"fix","candidates":[{"id":"candidate","text":"` + largeText + `","protected":false}]}`)}, nil)
	if err != nil || !result.IsError {
		t.Fatalf("oversized wire request result=%+v err=%v", result, err)
	}
}

func TestContextSelection_RemoteFailureAndRedirectAreNotRetriedOrLeaked(t *testing.T) {
	for _, response := range []string{"invalid", "unauthorized", "redirect"} {
		t.Run(response, func(t *testing.T) {
			var calls, leaked atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch response {
				case "unauthorized":
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, "caller-secret")
				case "redirect":
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
				default:
					_, _ = io.WriteString(w, `{"model":"jev","answers":{"a":{"type":"noul","noul":2}},"usage":{"input_tokens":1,"output_tokens":1}}`)
				}
			}))
			defer server.Close()
			driver, err := ContextSelectionTool("select_context", ContextSelectionConfig{Protocol: "typesafe-system-one", BaseURL: server.URL, APIKey: "caller-secret", Model: "jev"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = driver.Execute(context.Background(), tool.Call{Name: "select_context", Arguments: json.RawMessage(`{"task":"fix","candidates":[{"id":"a","text":"log","protected":false}]}`)}, nil)
			if err == nil || strings.Contains(err.Error(), "caller-secret") || calls.Load() != 1 || leaked.Load() != 0 {
				t.Fatalf("calls=%d leaked=%d err=%v", calls.Load(), leaked.Load(), err)
			}
		})
	}
}
