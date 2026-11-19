package statejournal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCheckpointWritesBeforeHeaderAndReplay(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "lg", Subject: "user"}
	const session = "session"
	if err := store.CreateSession(ctx, scope, session, "langgraph", 1); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireSession(ctx, scope, session, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	write := PendingWrite{
		TaskID: "node-a", TaskPath: "pregel/pull/a", Index: -3, Channel: "__interrupt__",
		Codec: "msgpack", Payload: Payload{Inline: []byte{0, 255}},
	}
	if err := store.PutWrites(ctx, scope, session, lease, "", "cp-1", []PendingWrite{write}); err != nil {
		t.Fatalf("LangGraph pending writes must support an unpublished header: %v", err)
	}
	if _, err := store.GetCheckpoint(ctx, scope, session, "", "cp-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unpublished checkpoint must be invisible: %v", err)
	}
	checkpoint := Checkpoint{
		ID: "cp-1", Codec: "msgpack", Payload: Payload{Inline: []byte{128}},
		Metadata:        json.RawMessage(`{"step":1,"details":{"node":"node-a","task":17},"scaled":1e1100}`),
		ChannelVersions: json.RawMessage(`{"channel":"1","__start__":"0"}`),
	}
	request := CheckpointCommit{Key: "checkpoint-op", Checkpoint: checkpoint, Writes: []PendingWrite{write},
		Intents: []PreparedIntent{{
			CallID: "turn-a:call", ToolName: "test", IdempotencyKey: "tool-op",
			ArgsDigest: sha256.Sum256([]byte("input")),
		}},
	}
	for i := range 2 {
		id, err := store.PutCheckpoint(ctx, scope, session, lease, request)
		if err != nil || id != "cp-1" {
			t.Fatalf("checkpoint write %d: %s, %v", i, id, err)
		}
	}
	reordered := request
	reordered.Checkpoint.Metadata = json.RawMessage(`{ "details": {"task":17,"node":"node-a"}, "step" : 1.0,"scaled":10e1099 }`)
	reordered.Checkpoint.ChannelVersions = json.RawMessage(`{"__start__":"0","channel":"1"}`)
	if id, err := store.PutCheckpoint(ctx, scope, session, lease, reordered); err != nil || id != "cp-1" {
		t.Fatalf("replay with reordered JSON: %q, %v", id, err)
	}
	changed := reordered
	changed.Checkpoint.ChannelVersions = json.RawMessage(`{"__start__":"0","channel":"2"}`)
	if _, err := store.PutCheckpoint(ctx, scope, session, lease, changed); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("replay with different data: %v", err)
	}
	legacyWire, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	legacyDigest := sha256.Sum256(legacyWire)
	if _, err := pool.Exec(ctx, `UPDATE ahp_journal.checkpoints SET request_digest=$3, canonical_request_digest=NULL
		WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, session, legacyDigest[:]); err != nil {
		t.Fatal(err)
	}
	if id, err := store.PutCheckpoint(ctx, scope, session, lease, request); err != nil || id != "cp-1" {
		t.Fatalf("original replay against an old digest: %q, %v", id, err)
	}
	if id, err := store.PutCheckpoint(ctx, scope, session, lease, reordered); err != nil || id != "cp-1" {
		t.Fatalf("replay after promoting an old digest: %q, %v", id, err)
	}

	changing := write
	changing.TaskPath = "pregel/pull/different"
	if err := store.PutWrites(ctx, scope, session, lease, "", "cp-1", []PendingWrite{changing}); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("write replay with changed path: %v", err)
	}
	result, err := store.GetCheckpoint(ctx, scope, session, "", "")
	if err != nil || result.Checkpoint.ID != "cp-1" || len(result.Writes) != 1 {
		t.Fatalf("loading checkpoint: %+v, %v", result, err)
	}
	got := result.Writes[0]
	if got.TaskPath != write.TaskPath || got.TaskID != write.TaskID || got.Index != write.Index ||
		got.Channel != write.Channel || string(got.Payload.Inline) != string(write.Payload.Inline) {
		t.Fatalf("pending write lost information: %+v", got)
	}
	foreign := scope
	foreign.Deployment = "other"
	if _, err := store.GetCheckpoint(ctx, foreign, session, "", "cp-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign checkpoint read: %v", err)
	}

	fork := request
	fork.Key = "fork-op"
	fork.Checkpoint.ParentID = "cp-1"
	fork.Checkpoint.ID = "cp-2"
	fork.Writes = nil
	if _, err := store.PutCheckpoint(ctx, scope, session, lease, fork); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("duplicate intent must roll back checkpoint publication: %v", err)
	}
	if _, err := store.GetCheckpoint(ctx, scope, session, "", "cp-2"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("failed commit published a checkpoint: %v", err)
	}
	fork.Intents = nil
	if _, err := store.PutCheckpoint(ctx, scope, session, lease, fork); err != nil {
		t.Fatal(err)
	}
	result, err = store.GetCheckpoint(ctx, scope, session, "", "cp-2")
	if err != nil || result.Checkpoint.ParentID != "cp-1" || len(result.Writes) != 0 {
		t.Fatalf("fork incorrectly inherited pending writes: %+v, %v", result, err)
	}
}

func TestPreparedIntentDigestWireCompatibility(t *testing.T) {
	expected := sha256.Sum256([]byte("tool arguments"))
	array, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(data []byte) string {
		return "\"" + base64.StdEncoding.EncodeToString(data) + "\""
	}
	for _, input := range []string{string(array), encode(expected[:])} {
		var intent PreparedIntent
		err := json.Unmarshal([]byte(`{"args_digest":`+input+`}`), &intent)
		if err != nil || intent.ArgsDigest != expected {
			t.Fatalf("valid argument digest failed to decode: %v", err)
		}
	}
	for _, invalid := range []string{"null", "[0]", encode(expected[:31]), "[0," + strings.TrimPrefix(string(array), "[")} {
		var intent PreparedIntent
		if err := json.Unmarshal([]byte(`{"args_digest":`+invalid+`}`), &intent); err == nil {
			t.Fatalf("malformed argument digest accepted: %s", invalid)
		}
	}
}
