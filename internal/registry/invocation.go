package registry

// Invoke must name the artifact it selected, and someone must check it.
//
// The measured fact this file exists for (root audit, canon 6fd2d0b6, live stand
// 6fd2d0b614de / sha 9b352b4c...): a2a tasks accepted by this product carried no
// artifact identity at all -- ahp_registry.task_pins held 0 rows against 5 live
// installations and 124 accepted tasks, and 0 of those tasks mentioned a version,
// a digest or an installation in their metadata. /v1/registry/Invoke resolved the
// selected version from the installation row (internal/registry/invokeapi.go) and
// then dropped it: the dispatcher posted a message and a bearer to one fixed
// self-endpoint, and the run that came out was indistinguishable from a run
// started by any other route. Whatever process happened to serve was therefore
// "the selected runtime", which is the claim root refuses.
//
// Two halves fix it, one per side of the hop:
//
//   - the sending side (this file plus invokeapi.go) states the artifact it
//     selected -- installation, version, digest -- resolved from ITS OWN row, never
//     taken from the caller's body, and pins the accepted task to exactly that
//     version afterwards (PinTaskToVersion), so "which artifact serves this task"
//     survives promote, rollback and republish (PDF rev3.2 c.42: "Версия деплоя и
//     AgentConfig закрепляются за задачей");
//   - the receiving side (internal/gateway/invocation.go) verifies the named
//     artifact against this tenant's installation before it admits anything, and
//     refuses with a reason when the named artifact is not what is installed.
//
// Pin rows and marker metadata are identities, not content: no prompt, no input,
// no tool arguments cross this line (PDF :347/:277).

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// InvocationMetadataKey is the A2A SendMessage metadata key carrying the
// artifact an Invoke selected. The receiving contract is the gateway's
// (internal/gateway/invocation.go names the same literal); this side states it
// again rather than importing the execution plane, exactly as the emil adapter
// restates its own delegation reference key.
const InvocationMetadataKey = "ahp.invocation"

// Refusal reasons of the invocation binding. Stable operator contract: an
// operator scripts on them, so one is added freely and none is renamed.
const (
	// ReasonInvokedVersionMoved: the installation no longer points at the version
	// the invocation named. A rollout landed between selection and binding; the
	// invocation is not silently re-pointed at the new artifact.
	ReasonInvokedVersionMoved = "invoked_version_moved"
	// ReasonInvokedDigestMismatch: the digest named by the invocation is not the
	// digest of the version it names -- a substitution, in either direction.
	ReasonInvokedDigestMismatch = "invoked_digest_mismatch"
	// ReasonTaskPinnedElsewhere: the task already carries a pin to another
	// version. The first pin wins, so a later claim cannot move a running task.
	ReasonTaskPinnedElsewhere = "task_pinned_to_other_version"
)

// Invocation is the artifact identity of one Invoke: which installation, which
// version row, which digest. Every field is an identity the registry itself
// published; none of them is caller-supplied.
type Invocation struct {
	InstallationID string `json:"installation_id"`
	VersionID      string `json:"version_id"`
	VersionDigest  string `json:"version_digest"`
}

// Validate rejects an invocation that could not be verified: an empty field or a
// digest that is not the canonical shape is not a partial claim, it is a claim
// that cannot be checked, and an unchecked claim is the thing being fixed.
func (inv Invocation) Validate() error {
	if err := requireID("installation id", inv.InstallationID); err != nil {
		return err
	}
	if err := requireID("version id", inv.VersionID); err != nil {
		return err
	}
	return ValidateVersionDigest(inv.VersionDigest)
}

// Metadata renders the wire value of the claim.
func (inv Invocation) Metadata() map[string]any {
	return map[string]any{
		"installation_id": inv.InstallationID,
		"version_id":      inv.VersionID,
		"version_digest":  inv.VersionDigest,
	}
}

// InstallationArtifact reads the artifact one installation currently serves:
// version id and digest together, so a comparison cannot be made against a
// half-read row. Tenant-scoped: another tenant's installation answers NOT_FOUND,
// the same way every other registry read does, so a probe cannot learn that an
// installation id exists.
func (s *Store) InstallationArtifact(ctx context.Context, tenant, installationID string) (Invocation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Invocation{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return Invocation{}, err
	}
	var out Invocation
	err := s.pool.QueryRow(ctx, `
SELECT i.id, v.id, v.digest
  FROM ahp_registry.installations i
  JOIN ahp_registry.versions v ON v.id = i.version_id
 WHERE i.tenant = $1 AND i.id = $2`, tenant, installationID).
		Scan(&out.InstallationID, &out.VersionID, &out.VersionDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Invocation{}, contracts.NewError(contracts.ErrNotFound, "installation not found")
		}
		return Invocation{}, storageError("read installation artifact", err)
	}
	return out, nil
}

// TaskArtifact answers the artifact a task is bound to. It exists because the
// acceptance question is "which artifact served THIS task", and a verifier has to
// be able to ask it without reading anybody's memory.
func (s *Store) TaskArtifact(ctx context.Context, tenant, taskID string) (Invocation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Invocation{}, err
	}
	if err := requireID("task id", taskID); err != nil {
		return Invocation{}, err
	}
	var out Invocation
	err := s.pool.QueryRow(ctx, `
SELECT p.installation_id, v.id, v.digest
  FROM ahp_registry.task_pins p
  JOIN ahp_registry.versions v ON v.id = p.version_id
 WHERE p.tenant = $1 AND p.task_id = $2`, tenant, taskID).
		Scan(&out.InstallationID, &out.VersionID, &out.VersionDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Invocation{}, contracts.NewError(contracts.ErrNotFound, "task is not bound to an artifact")
		}
		return Invocation{}, storageError("read task artifact", err)
	}
	return out, nil
}

// PinTaskToVersion binds a task to the exact version an Invoke named.
//
// It differs from Store.Pin in the one way that matters for honesty: Pin binds
// "whatever the installation serves now", which is true by construction and
// proves nothing about what the caller asked for. This binds the INVOKED version
// and refuses, in order:
//
//   - an installation that no longer points at that version (ReasonInvokedVersionMoved);
//   - a version whose digest is not the invoked digest (ReasonInvokedDigestMismatch);
//   - a task already pinned to a different version (ReasonTaskPinnedElsewhere).
//
// Replaying the same binding succeeds and returns the same version, so a retried
// invoke does not turn into a conflict.
func (s *Store) PinTaskToVersion(ctx context.Context, tenant, installationID, versionID, wantDigest, taskID string) (Version, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Version{}, err
	}
	if err := requireID("task id", taskID); err != nil {
		return Version{}, err
	}
	inv := Invocation{InstallationID: installationID, VersionID: versionID, VersionDigest: wantDigest}
	if err := inv.Validate(); err != nil {
		return Version{}, err
	}

	var out Version
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Same serialisation point as Pin and Promote, so a pin never observes a
		// half-moved version pointer.
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		if inst.VersionID != versionID {
			return contracts.NewReasoned(contracts.ErrConflict, ReasonInvokedVersionMoved,
				fmt.Sprintf("registry: installation %s serves version %s, the invocation named %s",
					inst.ID, inst.VersionID, versionID))
		}
		v, err := getVersion(ctx, tx, versionID)
		if err != nil {
			return err
		}
		if v.Digest != wantDigest {
			return contracts.NewReasoned(contracts.ErrConflict, ReasonInvokedDigestMismatch,
				fmt.Sprintf("registry: version %s has digest %s, the invocation named %s",
					versionID, v.Digest, wantDigest))
		}

		var pinned string
		switch err := tx.QueryRow(ctx,
			`SELECT version_id FROM ahp_registry.task_pins WHERE tenant = $1 AND task_id = $2`,
			tenant, taskID).Scan(&pinned); {
		case err == nil:
			if pinned != versionID {
				return contracts.NewReasoned(contracts.ErrConflict, ReasonTaskPinnedElsewhere,
					fmt.Sprintf("registry: task %s is already pinned to version %s, the invocation named %s",
						taskID, pinned, versionID))
			}
			pv, err := getVersion(ctx, tx, pinned)
			if err != nil {
				return err
			}
			out = pv
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return storageError("read task pin", err)
		}

		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.task_pins (task_id, tenant, installation_id, version_id)
VALUES ($1, $2, $3, $4)`, taskID, tenant, inst.ID, versionID); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewReasoned(contracts.ErrConflict, ReasonTaskPinnedElsewhere,
					"registry: task is already pinned")
			}
			return storageError("insert task pin", err)
		}
		out = v
		return nil
	})
	if err != nil {
		return Version{}, err
	}
	return out, nil
}
