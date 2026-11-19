package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rev32Build = "common-owner/research-3"

type rev32Fixture struct {
	ctx            context.Context
	admin          *pgxpool.Pool
	source, target *Store
	unit           DeploymentUnit
	sourceID       string
	frozen         bool
	approved       string
	refuseCommit   bool
	mu             sync.Mutex
	phases         []string
	actors         []contracts.Scope
	threads        []string
	fences         []Fence
	first          []CommonCommit
	receipts       []CommonReceipt
	intents        []Intent
}

func (f *rev32Fixture) custody(_ context.Context, c DeploymentCustody) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.phases = append(f.phases, c.Phase)
	if !f.frozen || c.Unit != f.unit || c.MigrationID != "move" || c.SourceStoreID != f.sourceID {
		return contracts.NewError(contracts.ErrForbidden, "coordinator has no matching frozen custody")
	}
	switch c.Phase {
	case "before-export":
		if c.Digest != "" {
			return invalid("premature image digest")
		}
	case "after-export":
		if f.approved != "" && f.approved != c.Digest {
			return contracts.NewError(contracts.ErrKeyReuse, "coordinator refuses re-sealing migration id")
		}
		f.approved = c.Digest
	case "before-import", "before-commit":
		if c.Digest != f.approved || c.Digest == "" {
			return contracts.NewError(contracts.ErrForbidden, "unapproved image digest")
		}
		if c.Phase == "before-commit" && f.refuseCommit {
			return contracts.NewError(contracts.ErrForbidden, "custody was revoked before commit")
		}
	default:
		return invalid("unknown custody phase")
	}
	return nil
}
func rev32DB(t *testing.T, f *rev32Fixture) *Store {
	t.Helper()
	name := "ahp_rev32_" + isolateSafeID(contracts.NewID())
	if _, e := f.admin.Exec(f.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		t.Fatal("create own database")
	}
	cfg := f.admin.Config().Copy()
	cfg.ConnConfig.Database = name
	pool, e := pgxpool.NewWithConfig(f.ctx, cfg)
	if e != nil {
		t.Fatal("owned DB pool")
	}
	t.Cleanup(func() {
		pool.Close()
		c, done := context.WithTimeout(context.Background(), 8*time.Second)
		defer done()
		if _, e := f.admin.Exec(c, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); e != nil {
			t.Error("own database cleanup failed")
		}
	})
	option, e := WithKVNamespacePolicies([]KVNamespacePolicy{{Tenant: f.unit.Tenant, Deployment: f.unit.Deployment, Namespace: []string{"user"}, Owner: "subject"}, {Tenant: f.unit.Tenant, Deployment: f.unit.Deployment, Namespace: []string{"app"}, Owner: "deployment"}})
	if e != nil {
		t.Fatal(e)
	}
	s := New(pool, option, WithCommonPayloadObjects(), WithDeploymentTransfer(DeploymentTransferConfig{NativeBuild: rev32Build, Authorize: f.custody}))
	if e = s.Migrate(f.ctx); e != nil {
		t.Fatal(e)
	}
	return s
}
func rev32FixtureNew(t *testing.T) *rev32Fixture {
	t.Helper()
	p, _ := isolatedPool(t)
	if isolateName == "" {
		t.Fatal("isolated parent required")
	}
	f := &rev32Fixture{ctx: context.Background(), admin: p, unit: DeploymentUnit{Tenant: "migrate-" + contracts.NewID(), Deployment: "native"}, frozen: true}
	f.source = rev32DB(t, f)
	f.target = rev32DB(t, f)
	if e := f.source.pool.QueryRow(f.ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&f.sourceID); e != nil {
		t.Fatal(e)
	}
	for i, subject := range []string{"alice", "bob"} {
		sc := contracts.Scope{Tenant: f.unit.Tenant, Deployment: f.unit.Deployment, Subject: subject}
		th, e := f.source.CreateThread(f.ctx, sc, "context-"+subject, "thread-"+subject)
		if e != nil {
			t.Fatal(e)
		}
		run, e := f.source.CreateRun(f.ctx, sc, th.ID, "run-"+subject, "task-"+subject, json.RawMessage(`{}`))
		if e != nil {
			t.Fatal(e)
		}
		lease, e := f.source.ClaimRunFor(f.ctx, sc.Tenant, sc.Deployment, "source-"+subject, run.ID, time.Minute)
		if e != nil || lease == nil {
			t.Fatal("source lease", e)
		}
		fence := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
		intent, e := f.source.RecordIntent(f.ctx, sc, run.ID, fence.Worker, fence.Epoch, "effect", "call", "fixture.write", "external", json.RawMessage(`{"v":42}`))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.source.UpdateIntent(f.ctx, sc, run.ID, fence.Worker, fence.Epoch, "effect", "call", IntentDispatched, nil); e != nil {
			t.Fatal(e)
		}
		status := IntentUnknown
		var result json.RawMessage
		if i == 1 {
			status = IntentSucceeded
			result = json.RawMessage(`{"value":42}`)
		}
		intent, e = f.source.UpdateIntent(f.ctx, sc, run.ID, fence.Worker, fence.Epoch, "effect", "call", status, result)
		if e != nil {
			t.Fatal(e)
		}
		req := rev32Request(subject, 0, 0, intent.ID)
		req.KV = []KVPut{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject", Value: json.RawMessage(fmt.Sprintf(`{"preference":%q}`, subject))}, {Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", ExpectedVersion: int64(i), Value: json.RawMessage(fmt.Sprintf(`{"theme":%d}`, i))}}
		receipt, e := f.source.CommitRecords(f.ctx, sc, fence, req)
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			next := rev32Request(subject, 1, 1, intent.ID)
			if _, e = f.source.CommitRecords(f.ctx, sc, fence, next); e != nil {
				t.Fatal(e)
			}
		}
		f.actors = append(f.actors, sc)
		f.threads = append(f.threads, th.ID)
		f.fences = append(f.fences, fence)
		f.first = append(f.first, req)
		f.receipts = append(f.receipts, receipt)
		f.intents = append(f.intents, intent)
	}
	return f
}
func rev32Request(user string, step int, rev int64, intent string) CommonCommit {
	return CommonCommit{Namespace: "native", Key: fmt.Sprintf("%s-%d", user, step), ExpectedRevision: rev, Document: CommonDocument{Format: CommonFormat, Profile: "test/1", Root: "root", Records: map[string]CommonRecord{
		"root":  {Kind: "session", NativeID: json.RawMessage(`"\u0000"`), Sequence: json.RawMessage(`null`), OwnerHint: json.RawMessage(`null`), Attributes: json.RawMessage(fmt.Sprintf(`{"v":1,"v":2,"nul":"\u0000","n":9007199254740993,"user":%q,"step":%d,"intent":%q}`, user, step, intent)), Relations: map[string][]string{"children": {"child"}, "empty": {}}},
		"child": {Kind: "value", Attributes: json.RawMessage(`{"stable":true}`), Relations: map[string][]string{}},
	}}}
}
func rev32Export(t *testing.T, f *rev32Fixture) DeploymentV3 {
	t.Helper()
	e, err := f.source.ExportDeploymentV3(f.ctx, f.unit, "move", rev32Build)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func rev32Clone(e DeploymentV3) DeploymentV3 {
	b, _ := json.Marshal(e)
	var out DeploymentV3
	_ = json.Unmarshal(b, &out)
	return out
}
func rev32Seal(e *DeploymentV3) { e.Digest, _ = deploymentDigest(*e) }
func rev32Rows(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if e := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM ahp_state.`+table).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestRev32DeploymentRoundTripAllSubjectsCommonHistoryOwnersAndIntents(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	if !reflect.DeepEqual(image.Manifest.Subjects, []string{"alice", "bob"}) || image.Manifest.Counts["common_cuts"] != 3 || image.Manifest.Counts["common_payloads"] != 5 {
		t.Fatal("incomplete image counts")
	}
	raw, _ := json.Marshal(image)
	parsed, e := ParseDeploymentV3(raw, f.unit, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := f.target.ImportDeploymentV3(f.ctx, f.unit, parsed, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	if receipt.SourceStoreID == receipt.TargetStoreID {
		t.Fatal("same physical store")
	}
	for i, sc := range f.actors {
		max := int64(1)
		if i == 0 {
			max = 2
		}
		for rev := int64(1); rev <= max; rev++ {
			a, e := f.source.ReadRecords(f.ctx, sc, f.threads[i], "native", rev)
			if e != nil {
				t.Fatal(e)
			}
			b, e := f.target.ReadRecords(f.ctx, sc, f.threads[i], "native", rev)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(a.Document.Records["root"].Attributes, b.Document.Records["root"].Attributes) || !reflect.DeepEqual(a.Document, b.Document) {
				t.Fatal("native bytes/relations/versions differ")
			}
		}
		bundle, e := f.target.ReadRecordsBundle(f.ctx, sc, f.threads[i], "native", rev31Keys())
		if e != nil || bundle.KV[0] == nil || bundle.KV[1] == nil {
			t.Fatal("owner state lost", e)
		}
		original, err := f.source.GetIntent(f.ctx, sc, f.fences[i].RunID, "effect", "call")
		if err != nil {
			t.Fatal(err)
		}
		it, e := f.target.GetIntent(f.ctx, sc, f.fences[i].RunID, "effect", "call")
		if e != nil || it.ID != f.intents[i].ID || it.Status != f.intents[i].Status || !bytes.Equal(it.Result, original.Result) {
			t.Fatal("intent changed", e)
		}
	}
	var leases int
	if e = f.target.pool.QueryRow(f.ctx, `SELECT count(*) FROM ahp_state.threads WHERE lease_worker IS NOT NULL OR lease_expires IS NOT NULL`).Scan(&leases); e != nil || leases != 0 {
		t.Fatal("source lease transported", e)
	}
	if rev32Rows(t, f.target, "common_cuts") != 3 || rev32Rows(t, f.target, "common_roles") != 6 || rev32Rows(t, f.target, "common_edges") != 3 {
		t.Fatal("missing common history")
	}
}
func TestRev32MigrationReplayNoDuplicateAndEpochFloor(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	a, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	b, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build)
	if e != nil || !reflect.DeepEqual(a, b) || rev32Rows(t, f.target, "deployment_transfers_v3") != 1 {
		t.Fatal("import replay changed state", e)
	}
	old, e := f.target.CommitRecords(f.ctx, f.actors[0], f.fences[0], f.first[0])
	if e != nil {
		t.Fatal(e)
	}
	one, _ := json.Marshal(old)
	two, _ := json.Marshal(f.receipts[0])
	if string(one) != string(two) || rev32Rows(t, f.target, "common_cuts") != 3 {
		t.Fatal("checkpoint receipt not preserved")
	}
	mutation := rev32Request("alice", 2, 2, f.intents[0].ID)
	if _, e = f.target.CommitRecords(f.ctx, f.actors[0], f.fences[0], mutation); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("old source fence regained authority", e)
	}
	lease, e := f.target.ClaimRunFor(f.ctx, f.unit.Tenant, f.unit.Deployment, "target-worker", f.fences[0].RunID, time.Minute)
	if e != nil || lease == nil || lease.Epoch <= f.fences[0].Epoch {
		t.Fatal("epoch floor not advanced", e)
	}
	fence := Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if _, e = f.target.CommitRecords(f.ctx, f.actors[0], fence, mutation); e != nil {
		t.Fatal("new owner cannot continue", e)
	}
	if _, e = f.target.UpdateIntent(f.ctx, f.actors[0], fence.RunID, fence.Worker, fence.Epoch, "effect", "call", IntentSucceeded, json.RawMessage(`{"value":42}`)); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("unknown cleared by new owner without reconciliation", e)
	}
}
func TestRev32CoordinatorBeforeCommitRefusalRollsBackEveryEntity(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	f.refuseCommit = true
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("custody revocation ignored", e)
	}
	for _, name := range []string{"threads", "runs", "common_cuts", "common_records", "common_payloads", "intents", "id_operations", "kv", "deployment_transfers_v3"} {
		if rev32Rows(t, f.target, name) != 0 {
			t.Fatal("partial import after denied commit", name)
		}
	}
	f.refuseCommit = false
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); e != nil {
		t.Fatal("clean retry failed", e)
	}
}
func TestRev32ExportRejectsUnfrozenAndExcludesForeignDeployment(t *testing.T) {
	f := rev32FixtureNew(t)
	f.frozen = false
	if _, e := f.source.ExportDeploymentV3(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("unfrozen source exported", e)
	}
	f.frozen = true
	foreign := contracts.Scope{Tenant: "foreign-" + contracts.NewID(), Deployment: "elsewhere", Subject: "outsider"}
	th, e := f.source.CreateThread(f.ctx, foreign, "foreign-context", "foreign-thread")
	if e != nil {
		t.Fatal(e)
	}
	image := rev32Export(t, f)
	if len(image.Entities["threads"]) != 2 {
		t.Fatal("foreign thread leaked")
	}
	for _, r := range deploymentRows(image, "threads") {
		if deploymentString(r, "id") == th.ID {
			t.Fatal("foreign ID included")
		}
	}
	other := f.unit
	other.Tenant = "another"
	if e = ValidateDeploymentV3(image, other, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("cross-tenant retarget accepted", e)
	}
}
func TestRev32RecomputedDigestDoesNotHideBrokenGraphOrOpaqueBytes(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	cases := []struct {
		name   string
		change func(*DeploymentV3)
	}{
		{"payload-hash", func(e *DeploymentV3) {
			var r deploymentRow
			_ = json.Unmarshal(e.Entities["common_payloads"][0], &r)
			r["attributes"] = json.RawMessage(`"e30="`)
			e.Entities["common_payloads"][0], _ = json.Marshal(r)
		}},
		{"missing-receipt", func(e *DeploymentV3) {
			e.Entities["common_receipts"] = e.Entities["common_receipts"][1:]
			e.Manifest.Counts["common_receipts"]--
		}},
		{"bad-edge", func(e *DeploymentV3) {
			var r deploymentRow
			_ = json.Unmarshal(e.Entities["common_edges"][0], &r)
			r["target"] = json.RawMessage(`"absent"`)
			e.Entities["common_edges"][0], _ = json.Marshal(r)
		}},
		{"wrong-receipt-key", func(e *DeploymentV3) {
			var r deploymentRow
			_ = json.Unmarshal(e.Entities["common_receipts"][0], &r)
			r["key"] = json.RawMessage(`"other"`)
			e.Entities["common_receipts"][0], _ = json.Marshal(r)
		}},
		{"owner-lifetime", func(e *DeploymentV3) {
			for i, raw := range e.Entities["kv"] {
				var r deploymentRow
				_ = json.Unmarshal(raw, &r)
				if deploymentString(r, "subject") == "" {
					r["subject"] = json.RawMessage(`"alice"`)
					e.Entities["kv"][i], _ = json.Marshal(r)
					break
				}
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bad := rev32Clone(image)
			c.change(&bad)
			rev32Seal(&bad)
			if e := ValidateDeploymentV3(bad, f.unit, rev32Build); e == nil {
				t.Fatal("re-signed inconsistent image accepted")
			}
			if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, bad, rev32Build); e == nil {
				t.Fatal("inconsistent image imported")
			}
			if rev32Rows(t, f.target, "threads") != 0 {
				t.Fatal("failed image left rows")
			}
		})
	}
}
func TestRev32DirtyTargetSameSourceAndSchemaDriftFailClosed(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	if _, e := f.source.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("same physical store accepted", e)
	}
	if _, e := f.target.CreateThread(f.ctx, f.actors[0], "occupied", "occupied"); e != nil {
		t.Fatal(e)
	}
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("dirty target accepted", e)
	}
	if _, e := f.source.pool.Exec(f.ctx, `ALTER TABLE ahp_state.common_cuts ADD COLUMN future_data text`); e != nil {
		t.Fatal(e)
	}
	if _, e := f.source.ExportDeploymentV3(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("schema drift silently omitted", e)
	}
}
func TestRev32PortableParserRejectsUnknownDuplicateTrailingAndDigestDamage(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	raw, _ := json.Marshal(image)
	inputs := [][]byte{append(append([]byte{}, raw...), []byte(` {}`)...), []byte(strings.Replace(string(raw), `"manifest":`, `"unexpected":1,"manifest":`, 1)), []byte(strings.Replace(string(raw), `"format":"ahp-state-deployment/3"`, `"format":"x","format":"ahp-state-deployment/3"`, 1))}
	for _, data := range inputs {
		if _, e := ParseDeploymentV3(data, f.unit, rev32Build); e == nil {
			t.Fatal("invalid portable archive parsed")
		}
	}
	bad := rev32Clone(image)
	bad.Digest = strings.Repeat("0", 64)
	if e := ValidateDeploymentV3(bad, f.unit, rev32Build); e == nil {
		t.Fatal("damaged digest accepted")
	}
}

func TestRev32CustodyBindsExactDigestAndTargetReceiptRejectsReuse(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	changed := rev32Clone(image)
	changed.Manifest.ExportedAt = changed.Manifest.ExportedAt.Add(time.Second)
	rev32Seal(&changed)
	if e := ValidateDeploymentV3(changed, f.unit, rev32Build); e != nil {
		t.Fatal("semantically valid metadata variation", e)
	}
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, changed, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("custody accepted unapproved image digest", e)
	}
	if rev32Rows(t, f.target, "threads") != 0 {
		t.Fatal("unapproved image published")
	}
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); e != nil {
		t.Fatal(e)
	}
	// Even an operator that incorrectly approves another archive under the same
	// migration id cannot replace the target's durable receipt and data.
	f.approved = changed.Digest
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, changed, rev32Build); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("target migration receipt reused for new digest", e)
	}
}
func TestRev32ReviewedNamespaceConfigCannotBeReinterpretedOnTarget(t *testing.T) {
	f := rev32FixtureNew(t)
	image := rev32Export(t, f)
	for key, p := range f.target.kvOwnerPolicies {
		if reflect.DeepEqual(p.Namespace, []string{"app"}) {
			p.Owner = "subject"
			f.target.kvOwnerPolicies[key] = p
		}
	}
	if _, e := f.target.ImportDeploymentV3(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("reviewed owner lifetime changed at import", e)
	}
	if rev32Rows(t, f.target, "threads") != 0 {
		t.Fatal("policy mismatch partially published")
	}
}
func TestRev32DeploymentControlMarkersAreNotSilentlyDropped(t *testing.T) {
	f := rev32FixtureNew(t)
	if e := f.source.FenceSubject(f.ctx, f.actors[0]); e != nil {
		t.Fatal(e)
	}
	if _, e := f.source.ExportDeploymentV3(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("tenant-wide erasure marker silently omitted", e)
	}
}
