package dote2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/sdk/go/sharedmemory"
)

// Real deployed Memory owner/Identity/Workspace/PG plus an actual neural embedder.
// The fixture is synthetic text; neither native bank data nor real user facts
// are copied into the shared profile. Python consumes the records Go creates.
func TestSharedMemorySDKsOnDeployedSemanticOwner(t *testing.T) {
	stand := loadStand(t)
	controlPath := os.Getenv("AHP_DOT_E2E_SHARED_MEMORY_CONTROL")
	if controlPath == "" {
		t.Skip("SKIP_ENV[tag=dot_e2e_layout] reason=shared_memory_curator_configuration_not_supplied missing=AHP_DOT_E2E_SHARED_MEMORY_CONTROL")
	}
	var control struct {
		CuratorUsername string `json:"curator_username"`
		CuratorPassword string `json:"curator_password"`
		CuratorSubject  string `json:"curator_subject"`
		MemberSubject   string `json:"member_subject"`
	}
	raw, err := os.ReadFile(controlPath)
	if err != nil || json.Unmarshal(raw, &control) != nil || control.CuratorSubject == "" || control.MemberSubject == "" {
		t.Fatal("read private shared Memory control")
	}
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	checks := map[string]any{}
	defer func() {
		v := "PASS"
		if t.Failed() {
			v = "FAIL"
			checks["test_failure"] = map[string]any{"status": "FAIL"}
		}
		stand.record(t, "shared-memory-sdks-semantic", v, checks)
	}()
	ownerHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	memberHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	curatorHuman := stand.humanToken(t, stand.HTTP, control.CuratorUsername, control.CuratorPassword)
	ownerToken, _ := stand.taskGrant(t, stand.HTTP, ownerHuman, "shared-sdk-owner-"+stamp, []string{"memory", "workspace", "state"})
	memberToken, _ := stand.taskGrant(t, stand.HTTP, memberHuman, "shared-sdk-member-"+stamp, []string{"memory", "workspace", "state"})
	curatorToken, _ := stand.taskGrant(t, stand.HTTP, curatorHuman, "shared-sdk-curator-"+stamp, []string{"memory", "memory:review", "memory:delete", "workspace", "state"})
	newClient := func(token string) *sharedmemory.Client {
		t.Helper()
		c, e := sharedmemory.New(sharedmemory.Config{BaseURL: stand.Origin, HTTPClient: stand.HTTP, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	owner, member, curator := newClient(ownerToken), newClient(memberToken), newClient(curatorToken)
	var project struct {
		ID string `json:"id"`
	}
	if err = stand.call(stand.HTTP, "/v1/workspace/Create", map[string]any{"workspace": "shared-sdk-" + stamp}, ownerToken, &project); err != nil || project.ID == "" {
		t.Fatalf("own Workspace Create: %v", err)
	}
	for _, subject := range []string{control.MemberSubject, control.CuratorSubject} {
		if err = stand.call(stand.HTTP, "/v1/workspace/AddMember", map[string]any{"workspace": project.ID, "subject": subject}, ownerToken, nil); err != nil {
			t.Fatalf("add own member: %v", err)
		}
	}
	defer func() {
		for _, subject := range []string{control.MemberSubject, control.CuratorSubject} {
			_ = stand.call(stand.HTTP, "/v1/workspace/RemoveMember", map[string]any{"workspace": project.ID, "subject": subject}, ownerToken, nil)
		}
	}()
	query := "doctor medication disease"
	in := sharedmemory.ProposeRequest{Kind: sharedmemory.Workspace, Workspace: project.ID, Text: "The physician prescribed a remedy for the illness.", SourceKind: "user_statement", Evidence: []sharedmemory.Evidence{{SourceKind: "user_statement", Detail: "Operator-authored synthetic acceptance corpus, not a real patient."}}}
	proposed, err := owner.Propose(t.Context(), "shared-sdk-propose-"+stamp, in)
	if err != nil || proposed.Memory.State != "proposed" {
		t.Fatalf("Go proposal: %+v err=%v", proposed, err)
	}
	replay, err := owner.Propose(t.Context(), "shared-sdk-propose-"+stamp, in)
	if err != nil || !replay.Replay || replay.Memory.ID != proposed.Memory.ID {
		t.Fatalf("same-key proposal replay: %+v err=%v", replay, err)
	}
	var apiError *sharedmemory.Error
	if _, err = owner.Review(t.Context(), "not-curator-"+stamp, proposed.Memory.ID, "approve"); !errors.As(err, &apiError) || apiError.Status != 403 {
		t.Fatalf("ordinary token review must be refused with 403: %v", err)
	}
	before, err := member.Search(t.Context(), sharedmemory.SearchRequest{Kind: sharedmemory.Workspace, Query: query, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range before.Hits {
		if h.Memory.ID == proposed.Memory.ID {
			t.Fatal("unreviewed proposal reached retrieval")
		}
	}
	active, err := curator.Review(t.Context(), "approve-"+stamp, proposed.Memory.ID, "approve")
	if err != nil || active.State != "active" {
		t.Fatalf("independent curator review: %+v err=%v", active, err)
	}
	for n, text := range []string{"The cargo ship crossed the ocean at dawn.", "The software build completed after compiling Go modules.", "An accountant prepared the quarterly tax invoice."} {
		distractor := in
		distractor.Text = text
		p, e := owner.Propose(t.Context(), "distractor-"+strconv.Itoa(n)+"-"+stamp, distractor)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = curator.Review(t.Context(), "approve-distractor-"+strconv.Itoa(n)+"-"+stamp, p.Memory.ID, "approve"); e != nil {
			t.Fatal(e)
		}
	}
	var lexical bool
	if err = stand.Pool.QueryRow(t.Context(), `SELECT search @@ plainto_tsquery('simple',$2) FROM ahp_memory.memories WHERE id=$1`, active.ID, query).Scan(&lexical); err != nil || lexical {
		t.Fatalf("semantic target has lexical overlap: %v %v", lexical, err)
	}
	got, err := member.Search(t.Context(), sharedmemory.SearchRequest{Kind: sharedmemory.Workspace, Query: query, Limit: 50})
	if err != nil || got.Mode != "hybrid" {
		t.Fatalf("actual semantic owner required, got mode=%s err=%v", got.Mode, err)
	}
	if len(got.Hits) == 0 || got.Hits[0].Memory.ID != active.ID {
		t.Fatal("non-lexical paraphrase was not ranked first among real distractors")
	}
	checks["go_propose_replay_review_hybrid"] = map[string]any{"status": "PASS", "workspace": project.ID, "record": active.ID, "mode": got.Mode, "lexical_target_matches": false, "retrieved": true}
	python := os.Getenv("AHP_DOT_E2E_PYTHON")
	if python == "" {
		t.Fatal("actual Python SDK interpreter required")
	}
	pyCall := func(action string) map[string]any {
		t.Helper()
		payload := map[string]any{"origin": stand.Origin, "token": memberToken, "cert": filepath.Join(stand.Dir, "certs/operator.crt"), "key": filepath.Join(stand.Dir, "certs/operator.key"), "ca": filepath.Join(stand.Dir, "certs/ca.crt"), "action": action, "id": active.ID, "query": query, "workspace": project.ID, "op_key": "python-revoked-" + stamp}
		input, _ := json.Marshal(payload)
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, python, filepath.Join("fixtures", "shared_memory_sdk.py"))
		cmd.Stdin = bytes.NewReader(input)
		cwd, e := os.Getwd()
		if e != nil {
			t.Fatal(e)
		}
		cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(cwd, "../../sdk/python"), "PYTHONDONTWRITEBYTECODE=1")
		var output, diagnostics bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &diagnostics
		if e = cmd.Run(); e != nil {
			t.Fatalf("Python shared Memory leg %s failed (diagnostics withheld): %v", action, e)
		}
		var result map[string]any
		if json.Unmarshal(output.Bytes(), &result) != nil || result["status"] != "PASS" {
			t.Fatal("Python SDK did not return a verified verdict")
		}
		return result
	}
	checks["python_member_read_and_hybrid"] = pyCall("member")
	// A private active record in the SAME canonical store must stay private.
	privateToken, _ := stand.taskGrant(t, stand.HTTP, curatorHuman, "shared-sdk-private-"+stamp, []string{"memory", "memory:review"})
	privateClient := newClient(privateToken)
	private, err := privateClient.Propose(t.Context(), "private-propose-"+stamp, sharedmemory.ProposeRequest{Kind: sharedmemory.User, Text: query, SourceKind: "user_statement"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = privateClient.Review(t.Context(), "private-review-"+stamp, private.Memory.ID, "approve"); err != nil {
		t.Fatal(err)
	}
	if _, err = member.Read(t.Context(), private.Memory.ID); !errors.As(err, &apiError) || (apiError.Status != 403 && apiError.Status != 404) {
		t.Fatalf("private record read must be refused with 403/404: %v", err)
	}
	got, err = member.Search(t.Context(), sharedmemory.SearchRequest{Query: query, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range got.Hits {
		if h.Memory.ID == private.Memory.ID {
			t.Fatal("exact-match private fact leaked into retrieval")
		}
	}
	checks["active_private_record_hidden"] = map[string]any{"status": "PASS", "record": private.Memory.ID}
	if err = stand.call(stand.HTTP, "/v1/workspace/RemoveMember", map[string]any{"workspace": project.ID, "subject": control.MemberSubject}, ownerToken, nil); err != nil {
		t.Fatal(err)
	}
	checks["python_existing_token_read_search_write_revoked"] = pyCall("revoked")
	// SDKs transport and owner semantics are tested; no claim of multilingual
	// quality, Dot auto-injection or shared semantic performance is made here.
	checks["declared_scope"] = map[string]any{"status": "PASS", "profile": "explicit shared owner; native personal banks unchanged", "corpus": "synthetic English paraphrase", "dot_auto_injection": false, "physical_compute": "not exercised"}
}
