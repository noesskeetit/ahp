package state

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Break caught: lossy UTF-8 fingerprints acknowledge another binary request,
// including when replaying a receipt produced by a pre-upgrade server.
func TestCheckpointBinaryReplay(t *testing.T) {
	for _, door := range []string{"checkpoint", "batch"} {
		for _, legacy := range []bool{false, true} {
			name := door + "/current"
			if legacy {
				name = door + "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				e := cpFixture(t)
				c := cp("binary", Part{Channel: "ch", Version: "v1", Data: []byte{0xff}})
				put := func(f Fence, value Checkpoint, key string) error {
					if door == "batch" {
						_, err := e.s.PutBatch(e.ctx, e.scope, f, BatchRequest{Key: key, Checkpoint: &value, Stream: &StreamOp{Key: "output", Kind: "model", Payload: json.RawMessage(`{"text":"once"}`)}})
						return err
					}
					_, err := e.s.PutCheckpoint(e.ctx, e.scope, f, value, key)
					return err
				}
				if err := put(e.fence, c, "op"); err != nil {
					t.Fatal(err)
				}
				if legacy {
					// Exact previous-server receipt algorithm, not a mocked State API.
					old, err := checkpointFingerprint(c, c.Parts)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = e.s.pool.Exec(e.ctx, "UPDATE ahp_state.checkpoints SET body_digest=$1 WHERE thread_id=$2", old, e.threadID); err != nil {
						t.Fatal(err)
					}
				}
				if err := put(e.fence, c, "op"); err != nil {
					t.Fatalf("identical replay: %v", err)
				}
				changed := c
				changed.Parts = []Part{{Channel: "ch", Version: "v1", Data: []byte{0xfe}}}
				if err := put(e.fence, changed, "op"); contracts.Code(err) != contracts.ErrKeyReuse {
					t.Errorf("different binary request acknowledged: %v", err)
				}
				got, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "ch", "v1")
				if err != nil || !bytes.Equal(got, []byte{0xff}) {
					t.Fatalf("original blob changed: %x %v", got, err)
				}
				stale := e.fence
				stale.Epoch++
				if err := put(stale, c, "op"); contracts.Code(err) != contracts.ErrStaleEpoch {
					t.Errorf("replay bypassed fence: %v", err)
				}
				if door == "checkpoint" {
					if err := put(e.fence, changed, "another-key"); contracts.Code(err) != contracts.ErrConflict {
						t.Errorf("public id acknowledged other bytes under fresh key: %v", err)
					}
				}
			})
		}
	}
}

// Break caught: new rows keep the ambiguous legacy receipt forever, or a
// referenced part aliases an explicit empty part in the new request identity.
func TestCheckpointBinaryNewReceipt(t *testing.T) {
	e := cpFixture(t)
	c := cp("empty", Part{Channel: "ch", Version: "empty", Data: []byte{}})
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "op"); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := e.s.pool.QueryRow(e.ctx, "SELECT body_digest FROM ahp_state.checkpoints WHERE thread_id=$1", e.threadID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stored, "checkpoint-v2:") {
		t.Errorf("new receipt has no binary-safe version")
	}
	changed := c
	changed.Parts = []Part{{Channel: "ch", Version: "empty"}}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, changed, "op"); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("nil reference and explicit empty bytes share request identity: %v", err)
	}
}

func TestCheckpointBinaryEmptyPartsCanonicalAcrossImport(t *testing.T) {
	// An omitted optional parts list comes back as nil through JSON exports.
	// It must identify the same request as the validated empty [] list.
	c := cp("no-parts")
	a, err := checkpointFingerprintV2(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := checkpointFingerprintV2(c, []Part{})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("empty parts list changes receipt across export/import")
	}
}
