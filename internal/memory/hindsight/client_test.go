package hindsight

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

type transport func(*http.Request) (*http.Response, error)

func TestNativeListUsesValidFactsAndNativeFactType(t *testing.T) {
	c, err := New("http://hindsight.internal", "")
	if err != nil {
		t.Fatal(err)
	}
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" || r.URL.Path != "/v1/default/banks/opaque/memories/list" || r.URL.Query().Get("state") != "valid" || r.URL.Query().Get("offset") != "20" {
			t.Fatal(r.URL)
		}
		return reply(200, `{"items":[{"id":"source-fact","text":"Verified result","fact_type":"world","metadata":{"operation_id":"op"}}],"total":21,"limit":10,"offset":20}`), nil
	})
	facts, total, err := c.List(context.Background(), memory.Bank{ID: "opaque"}, 10, 20)
	if err != nil || total != 21 || len(facts) != 1 || facts[0].Type != "world" || facts[0].Metadata["operation_id"] != "op" {
		t.Fatal(facts, total, err)
	}
}

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }
func reply(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
func TestNativeConfigurationAndRetainIdentity(t *testing.T) {
	c, err := New("http://hindsight.internal", "server-key")
	if err != nil {
		t.Fatal(err)
	}
	methods := []string{}
	b := memory.Bank{ID: "bank-id", Scope: memory.Personal}
	job := memory.Job{ID: "efad427c-9167-4254-b9f8-ad639435a329", Content: "user prefers Russian", CreatedAt: time.Now(), Identity: memory.Identity{TenantID: "tenant", UserID: "user"}}
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer server-key" {
			t.Fatal("missing internal credential")
		}
		methods = append(methods, r.Method+" "+r.URL.Path)
		var body map[string]any
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
		}
		switch {
		case r.Method == http.MethodPut:
			if body["retain_mission"] != memory.PersonalMission {
				t.Fatal(body)
			}
			return reply(200, `{}`), nil
		case r.Method == http.MethodPatch:
			updates := body["updates"].(map[string]any)
			if updates["store_document_text"] != false || updates["enable_auto_consolidation"] != true || updates["enable_observations"] != true {
				t.Fatal(updates)
			}
			return reply(200, `{}`), nil
		case r.Method == http.MethodPost:
			if body["operation_id"] != nativeOperationID(job.ID) || body["async"] != true {
				t.Fatal(body)
			}
			items := body["items"].([]any)
			item := items[0].(map[string]any)
			if item["document_id"] != "ahp-"+job.ID {
				t.Fatal(item)
			}
			if _, ok := item["tags"]; ok {
				t.Fatal("volatile tags would partition consolidation")
			}
			return reply(202, `{"success":true,"operation_id":"`+nativeOperationID(job.ID)+`"}`), nil
		}
		return reply(500, `{}`), nil
	})
	if err := c.Configure(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := c.Retain(context.Background(), b, job); err != nil {
		t.Fatal(err)
	}
	want := []string{"PUT /v1/default/banks/bank-id", "PATCH /v1/default/banks/bank-id/config", "POST /v1/default/banks/bank-id/memories"}
	if strings.Join(methods, "|") != strings.Join(want, "|") {
		t.Fatal(methods)
	}
}
func TestMissingBankAndOperationAreNotCreatedByRead(t *testing.T) {
	c, _ := New("http://hindsight.internal", "")
	calls := []string{}
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.Method)
		return reply(404, `{}`), nil
	})
	facts, err := c.Recall(context.Background(), memory.Bank{ID: "missing"}, "query", 800)
	if err != nil || len(facts) != 0 {
		t.Fatal(facts, err)
	}
	status, err := c.Status(context.Background(), "missing", "operation")
	if err != nil || status != "not_found" {
		t.Fatal(status, err)
	}
	if strings.Join(calls, ",") != "POST,GET" {
		t.Fatal(calls)
	}
}
func TestOperationIdentityMismatchIsRejected(t *testing.T) {
	c, _ := New("http://engine", "")
	c.HTTP.Transport = transport(func(*http.Request) (*http.Response, error) {
		return reply(200, `{"success":true,"operation_id":"other"}`), nil
	})
	err := c.Retain(context.Background(), memory.Bank{}, memory.Job{ID: "expected"})
	if err == nil {
		t.Fatal("accepted mismatched operation")
	}
}
func TestUpstreamBodiesAreNotExposed(t *testing.T) {
	c, _ := New("http://engine", "")
	c.HTTP.Transport = transport(func(*http.Request) (*http.Response, error) { return reply(500, `{"error":"secret transcript"}`), nil })
	_, err := c.Status(context.Background(), "bank", "op")
	if err == nil || strings.Contains(err.Error(), "secret transcript") {
		t.Fatal(err)
	}
}
func TestObservationCannotBeEdited(t *testing.T) {
	c, _ := New("http://engine", "")
	calls := 0
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "GET" {
			t.Fatal("derived fact was patched")
		}
		return reply(200, `{"id":"id","type":"observation"}`), nil
	})
	err := c.Correct(context.Background(), memory.Bank{ID: "bank"}, memory.Job{MemoryID: "id", Kind: "correct", Content: "replacement"})
	if err == nil || calls != 1 {
		t.Fatal(err, calls)
	}
}
