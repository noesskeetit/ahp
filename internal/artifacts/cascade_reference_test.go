package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// DEF-H24-15: deleting a chat is a cascade, and a cascade that tombstones bytes a
// workspace candidate still names leaves the workspace holding a reference that
// answers 404 when it goes to Publish. The contract chosen here is HOLD (not
// refuse the deletion, not invent a second pin row): the cascade runs, the
// referenced row keeps serving, and the decision is audited in the same
// transaction. Without a configured authority the behaviour must be byte for byte
// what it was before the option existed.

// referenceKeeper is a test authority whose answer can change between sweeps, the
// way a real candidate changes state. It records what it was asked so the test can
// check the probe is asked with the digest as well as the id -- a workspace that
// names bytes rather than a row must still match.
type referenceKeeper struct {
	referenced map[string]bool
	err        error
	asked      []ArtifactQuery
}

func (k *referenceKeeper) ArtifactsReferenced(_ context.Context, _ string, arts []ArtifactQuery) (map[string]bool, error) {
	k.asked = append(k.asked, arts...)
	if k.err != nil {
		return nil, k.err
	}
	return k.referenced, nil
}

// TestCascadeHoldsArtifactThatWorkspaceNames is the green leg of the defect: one
// context, two artifacts, one of them named by a durable reference outside the
// artifact plane. The cascade must run, must take the other one, must leave the
// named one readable, and must say so in the audit trail out-of-band.
func TestCascadeHoldsArtifactThatWorkspaceNames(t *testing.T) {
	keeper := &referenceKeeper{}
	e := setup(t, WithReferenceKeeper(keeper))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contextID := "ctx-h2415-" + t.Name()
	held := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 named by a candidate")
	free := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 nothing names")
	heldObject := e.objectOf(t, held)

	keeper.referenced = map[string]bool{held: true}
	res, err := e.store.DeleteByContext(ctx, e.scope, contextID)
	if err != nil {
		t.Fatalf("DeleteByContext: %v", err)
	}
	if res.Referenced != 1 || res.Tombstoned != 1 {
		t.Fatalf("cascade must hold one and take one: %+v", res)
	}
	if res.Held != 0 {
		t.Fatalf("no legal hold is in play here: %+v", res)
	}

	// Asked with the digest, not only the id: matching bytes is what lets a
	// candidate that stores a digest protect the row.
	var sawDigest bool
	for _, q := range keeper.asked {
		if q.ID == held && q.Digest != "" {
			sawDigest = true
		}
	}
	if !sawDigest {
		t.Fatalf("the keeper must be asked with the digest of each candidate: %+v", keeper.asked)
	}

	// The held artifact is still a fully usable artifact, read through the plane.
	mustReadable(t, e, held)
	mustGone(t, e, free)
	// Out-of-band: the bytes behind the held artifact are still there under the
	// exact version the plane serves, checked with a raw client GET.
	body, err := e.rawGet(t, heldObject, mustVersionOf(t, e, held))
	if err != nil {
		t.Fatalf("raw GET of the held object: %v", err)
	}
	if string(body) != "h2415 named by a candidate" {
		t.Fatalf("held bytes must be intact")
	}

	// Both decisions are in the durable trail, read with plain SQL rather than a
	// return value.
	heldEvents := e.auditOf(t, held)
	if len(heldEvents) != 1 || heldEvents[0].Event != GCEventCascadeHeld {
		t.Fatalf("held artifact must carry exactly one %s audit row, got %+v", GCEventCascadeHeld, heldEvents)
	}
	if !strings.HasSuffix(heldEvents[0].Reason, ":held-by-reference") {
		t.Fatalf("the held audit must name its cause, got %q", heldEvents[0].Reason)
	}
	for _, ev := range e.auditOf(t, free) {
		if ev.Event == GCEventCascadeHeld {
			t.Fatalf("nothing names %s: it must not be reported as held", free)
		}
	}

	// A held row is not a deleted row and not an immortal one: once the reference
	// goes, the next cascade collects it through the ordinary path.
	keeper.referenced = map[string]bool{}
	res, err = e.store.DeleteByContext(ctx, e.scope, contextID)
	if err != nil {
		t.Fatalf("second DeleteByContext: %v", err)
	}
	if res.Tombstoned != 1 || res.Referenced != 0 || res.AlreadyDeleted != 1 {
		t.Fatalf("with the reference gone the row must be collectable: %+v", res)
	}
	mustGone(t, e, held)
}

// TestCascadeRefusedWhenLiveReferencesUnreadable pins the fail-closed half: an
// authority that cannot answer must not be read as "nothing refers to it". The
// whole cascade is refused and NOTHING is tombstoned -- including the artifacts
// nobody references, because a half-applied deletion is the outcome this plane
// refuses to choose on its own.
func TestCascadeRefusedWhenLiveReferencesUnreadable(t *testing.T) {
	keeper := &referenceKeeper{err: errors.New("workspace plane unavailable")}
	e := setup(t, WithReferenceKeeper(keeper))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contextID := "ctx-h2415ff-" + t.Name()
	named := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 fail closed named")
	unneeded := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 fail closed other")

	if _, err := e.store.DeleteByContext(ctx, e.scope, contextID); err == nil {
		t.Fatal("an unreadable reference authority must refuse the cascade, not be ignored")
	}
	mustReadable(t, e, named)
	mustReadable(t, e, unneeded)
	for _, id := range []string{named, unneeded} {
		if _, deleted := e.rowExists(t, id); deleted {
			t.Fatalf("%s: a refused cascade must tombstone nothing", id)
		}
		if len(e.auditOf(t, id)) != 0 {
			t.Fatalf("%s: a refused cascade must write no audit either", id)
		}
	}

	// An empty conversation has nothing to check, so it must not fail on an
	// authority that would fail: refusing to delete nothing is its own defect.
	if _, err := e.store.DeleteByContext(ctx, e.scope, "ctx-h2415-empty"); err != nil {
		t.Fatalf("deleting a context with no artifacts must not consult the keeper: %v", err)
	}
}

// TestCascadeWithoutKeeperIsUnchanged is the regression boundary for every
// deployment that does not run the workspace or memory plane: with no authority
// configured the cascade is what it always was.
func TestCascadeWithoutKeeperIsUnchanged(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contextID := "ctx-h2415nil-" + t.Name()
	first := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 no keeper one")
	second := e.uploadSealBound(t, Binding{ContextID: contextID}, "h2415 no keeper two")

	res, err := e.store.DeleteByContext(ctx, e.scope, contextID)
	if err != nil {
		t.Fatalf("DeleteByContext: %v", err)
	}
	if res.Tombstoned != 2 || res.Referenced != 0 {
		t.Fatalf("without an authority everything matched is taken: %+v", res)
	}
	mustGone(t, e, first)
	mustGone(t, e, second)
	for _, id := range []string{first, second} {
		for _, ev := range e.auditOf(t, id) {
			if ev.Event == GCEventCascadeHeld {
				t.Fatalf("%s: nothing was configured to hold anything, yet the trail says held", id)
			}
		}
	}
}

// mustVersionOf reads the object version the plane serves for one artifact, so a
// raw GET checks the same bytes rather than the latest object state.
func mustVersionOf(t *testing.T, e *env, id string) string {
	t.Helper()
	var version string
	if err := isolatedPool(t).QueryRow(context.Background(),
		`SELECT COALESCE(version,'') FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&version); err != nil {
		t.Fatalf("version lookup: %v", err)
	}
	if version == "" {
		t.Fatalf("%s has no sealed version", id)
	}
	return version
}
