package artifacts_test

// The inherited boundary these legs close: a mutation whose answer never arrived
// used to look exactly like a rejected request (INVALID_ARGUMENT), which invites a
// caller to re-send under a new key and mint a second artifact, or to refund a
// budget line the owner may already have spent. The distinction is now pinned on
// the real transport: the same interruption is UNKNOWN for a mutation and an
// ordinary error for a read, and an owner that actually answered is never reported
// as unknown -- except where its own answer says the effect may exist.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

func ownerClient(t *testing.T, h http.HandlerFunc) *sdk.Client {
	t.Helper()
	srv, hc := mtlsFixture(t, h)
	c, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return "task-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// death answers a request by disappearing: the owner took the bytes and the
// response never existed. That is the production shape of a lost ACK.
func death(op string, seen *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}
}

func TestInterruptedMutationIsOutcomeUnknownReadIsNot(t *testing.T) {
	for _, leg := range []struct {
		name    string
		call    func(*sdk.Client) error
		unknown bool
	}{
		{"BeginUpload", func(c *sdk.Client) error {
			_, err := c.BeginUpload(context.Background(), sdk.UploadRequest{Key: "key-1", Name: "n", MIME: "text/plain", SHA256: "ab", Size: 1})
			return err
		}, true},
		{"Seal", func(c *sdk.Client) error {
			_, err := c.Seal(context.Background(), "artifact-1")
			return err
		}, true},
		{"Process", func(c *sdk.Client) error {
			_, err := c.Process(context.Background(), "artifact-1")
			return err
		}, true},
		{"CopyFromTransferGrant", func(c *sdk.Client) error {
			_, err := c.CopyFromTransferGrant(context.Background(), "grant-1", "key-1")
			return err
		}, true},
		{"TransferGrant", func(c *sdk.Client) error {
			_, err := c.TransferGrant(context.Background(), sdk.ArtifactRef{ID: "artifact-1"}, sdk.TransferTarget{Deployment: "d-other", ContextID: "c-other", TaskID: "t-other"})
			return err
		}, true},
		{"Get", func(c *sdk.Client) error {
			_, err := c.Get(context.Background(), "artifact-1")
			return err
		}, false},
		{"Readiness", func(c *sdk.Client) error {
			_, err := c.Readiness(context.Background(), "artifact-1")
			return err
		}, false},
	} {
		t.Run(leg.name, func(t *testing.T) {
			var seen atomic.Int32
			c := ownerClient(t, death(leg.name, &seen))
			err := leg.call(c)
			if err == nil {
				t.Fatal("an interrupted call returned success")
			}
			if seen.Load() == 0 {
				t.Fatal("the owner never saw the request -- the leg measured nothing")
			}
			if got := sdk.IsOutcomeUnknown(err); got != leg.unknown {
				t.Errorf("IsOutcomeUnknown=%v, want %v (err=%v)", got, leg.unknown, err)
			}
			if leg.unknown {
				var ue *sdk.UnknownOutcomeError
				if !errors.As(err, &ue) || ue.Op != leg.name {
					t.Errorf("the unknown error must name the operation to reconcile: %v", err)
				}
				// With no answer to preserve there is still a code to branch on,
				// and it must not be one that implies the owner refused the request.
				if code := contracts.Code(err); code != "UNKNOWN" {
					t.Errorf("code=%q, want UNKNOWN: Python reports the same interruption as UNKNOWN, and INVALID_ARGUMENT here is exactly the defect being closed", code)
				}
				if reason := contracts.ReasonOf(err); reason != "outcome_unknown" {
					t.Errorf("reason=%q, want outcome_unknown", reason)
				}
			}
		})
	}
}

func TestUnreadableAnswerOnMutationIsUnknown(t *testing.T) {
	for _, body := range []string{"not json at all", `{"id":`, ""} {
		t.Run(fmt.Sprintf("body=%q", body), func(t *testing.T) {
			c := ownerClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			})
			if _, err := c.Seal(context.Background(), "artifact-1"); !sdk.IsOutcomeUnknown(err) {
				t.Errorf("Seal got something unreadable and the caller was told it failed cleanly: %v", err)
			}
			// The same unreadable answer on a read has no durable consequence, so a
			// retry is legitimate and must not be dressed up as an unknown outcome.
			if _, err := c.Get(context.Background(), "artifact-1"); sdk.IsOutcomeUnknown(err) {
				t.Errorf("a read cannot have an unknown outcome: %v", err)
			}
		})
	}
}

func TestAnsweredOwnerClassification(t *testing.T) {
	for _, leg := range []struct {
		name    string
		status  int
		body    string
		unknown bool
		code    string
	}{
		{"refused-with-a-reason", http.StatusForbidden, `{"code":"FORBIDDEN","message":"no authority"}`, false, contracts.ErrForbidden},
		{"already-sealed", http.StatusConflict, `{"code":"CONFLICT","message":"already sealed"}`, false, contracts.ErrConflict},
		{"server-failed-after-taking-it", http.StatusInternalServerError, `{"code":"INTERNAL","message":"storage failed"}`, true, "INTERNAL"},
		{"owner-says-transfer-unknown", http.StatusBadRequest, `{"code":"CONFLICT","reason":"transfer_outcome_unknown","message":"peer answer lost"}`, true, contracts.ErrConflict},
		{"intermediary-lies-with-502", http.StatusBadGateway, `gate 502 from an intermediary, not json`, true, ""},
	} {
		t.Run(leg.name, func(t *testing.T) {
			c := ownerClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(leg.status)
				_, _ = w.Write([]byte(leg.body))
			})
			_, err := c.CopyFromTransferGrant(context.Background(), "grant-1", "key-1")
			if err == nil {
				t.Fatal("an error answer returned success")
			}
			if got := sdk.IsOutcomeUnknown(err); got != leg.unknown {
				t.Errorf("IsOutcomeUnknown=%v, want %v (err=%v)", got, leg.unknown, err)
			}
			if leg.code != "" && contracts.Code(err) != leg.code {
				t.Errorf("code=%q, want %q: classification must not eat the code a caller branches on", contracts.Code(err), leg.code)
			}
		})
	}
}

// A context that was already canceled never reached the owner, so its outcome IS
// known: nothing was sent, nothing can exist.
func TestPreCanceledContextIsNotAnUnknownOutcome(t *testing.T) {
	var seen atomic.Int32
	c := ownerClient(t, func(w http.ResponseWriter, r *http.Request) { seen.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Seal(ctx, "artifact-1"); err == nil {
		t.Fatal("a canceled call returned success")
	} else if sdk.IsOutcomeUnknown(err) {
		t.Errorf("a request that never left must not be reported as unknown: %v", err)
	}
	if seen.Load() != 0 {
		t.Fatal("a canceled context reached the owner")
	}
}
