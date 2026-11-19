package hindsight

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	memory "github.com/ahp-platform/ahp/internal/memory/longterm"
)

// DEF-DOTE2E-12 deterministic gate. The j5 personal Dot-journey leg failed
// because Hindsight's extraction silently dropped the second sentence of an
// explicitly-stored fact ("...keeps research notes. The employee stores the
// journey code word journey-otter-<stamp> for this conversation."), so
// /v1/memory/Remember reported succeeded while the code word never became a
// recallable memory_unit. The root cause was the personal bank's retain
// configuration; this test locks the fix without depending on the upstream LLM.

// capturedConfig records the extraction mode and mission the client sends for a
// scope, on both the PUT template and the PATCH /config update.
func capturedConfig(t *testing.T, scope memory.Scope) (putMode, patchMode string, putMission, patchMission any) {
	t.Helper()
	c, err := New("http://hindsight.internal", "")
	if err != nil {
		t.Fatal(err)
	}
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		var body map[string]any
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
		}
		switch r.Method {
		case http.MethodPut:
			putMode, _ = body["retain_extraction_mode"].(string)
			putMission = body["retain_mission"]
		case http.MethodPatch:
			updates, _ := body["updates"].(map[string]any)
			patchMode, _ = updates["retain_extraction_mode"].(string)
			patchMission = updates["retain_mission"]
			if updates["store_document_text"] != false {
				t.Fatalf("privacy: personal retain must not store raw document text: %v", updates)
			}
			return reply(200, `{}`), nil
		default:
			return reply(200, `{}`), nil
		}
		return reply(200, `{}`), nil
	})
	if err := c.Configure(context.Background(), memory.Bank{ID: "bank", Scope: scope}); err != nil {
		t.Fatal(err)
	}
	return
}

func TestPersonalRetainPreservesVerbatimProjectStaysConcise(t *testing.T) {
	putMode, patchMode, putMission, patchMission := capturedConfig(t, memory.Personal)
	if putMode != "verbatim" || patchMode != "verbatim" {
		t.Fatalf("personal retain must extract verbatim so an explicitly stored identifier survives: put=%q patch=%q", putMode, patchMode)
	}
	if putMission != memory.PersonalMission || patchMission != memory.PersonalMission {
		t.Fatal("personal retain must send the personal mission on both template and config")
	}

	putMode, patchMode, putMission, patchMission = capturedConfig(t, memory.Project)
	if putMode != "concise" || patchMode != "concise" {
		t.Fatalf("project/workspace plane must keep concise distillation: put=%q patch=%q", putMode, patchMode)
	}
	if putMission != memory.ProjectMission || patchMission != memory.ProjectMission {
		t.Fatal("project retain must send the project mission")
	}
}

// TestPersonalMissionKeepsStoredIdentifiersButRejectsNoise locks the two halves
// of the mission fix: it must now instruct the extractor to keep a user-recorded
// identifier even for one conversation (the dropped j5 code word), while it must
// still instruct the extractor to reject runtime/telemetry status (the noise the
// old mission already dropped, and that the live round-trip test depends on).
func TestPersonalMissionKeepsStoredIdentifiersButRejectsNoise(t *testing.T) {
	m := memory.PersonalMission
	if strings.Contains(m, "Keep only stable facts") {
		t.Fatal("the mission still forbids transient facts: it is what silently dropped the j5 code word")
	}
	for _, want := range []string{
		"code word",
		"identifier",
		"only for the current conversation",
		"verbatim",
		"Never store machine, system, deployment, runtime, health or telemetry status",
		"If no suitable new information exists, extract no facts",
	} {
		if !strings.Contains(m, want) {
			t.Fatalf("personal mission must contain %q to keep stored identifiers and reject noise", want)
		}
	}
}

// TestRetainFactsCommittedParsesExtractionOutcome locks the partial-loss
// observability: a completed retain that committed zero facts is observable
// (facts_committed=0), and the client prefers progress.detail.facts_committed
// over the metadata count. Recorded transport, no live LLM.
func TestRetainFactsCommittedParsesExtractionOutcome(t *testing.T) {
	cases := map[string]struct {
		body string
		want int
	}{
		"progress-zero": {`{"status":"completed","progress":{"detail":{"facts_committed":0}},"result_metadata":{"unit_ids_count":0}}`, 0},
		"progress-two":  {`{"status":"completed","progress":{"detail":{"facts_committed":2}}}`, 2},
		"meta-count":    {`{"status":"completed","result_metadata":{"unit_ids_count":1}}`, 1},
		"absent":        {`{"status":"completed"}`, 0},
	}
	for name, tc := range cases {
		c, _ := New("http://engine", "")
		c.HTTP.Transport = transport(func(*http.Request) (*http.Response, error) { return reply(200, tc.body), nil })
		got, err := c.RetainFactsCommitted(context.Background(), "bank", "op")
		if err != nil || got != tc.want {
			t.Fatalf("%s: got=%d err=%v want=%d", name, got, err, tc.want)
		}
	}
}
