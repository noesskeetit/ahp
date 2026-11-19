package state

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Called only inside the writer's lease transaction. New receipts are byte-safe.
// Legacy receipts retain their old key/digest, but require exact comparison of
// supplied bytes against immutable stored blobs before acknowledging a replay.
func checkpointReplayMatches(ctx context.Context, tx pgx.Tx, threadID string, cp Checkpoint, parts []Part, stored, current string) (bool, error) {
	if stored == current {
		return true, nil
	}
	if !strings.HasPrefix(stored, "sha256:") {
		return false, nil
	}
	legacy, err := checkpointFingerprint(cp, parts)
	if err != nil || legacy != stored {
		return false, err
	}
	for _, part := range parts {
		if part.Data == nil {
			continue // Legacy request referenced an already immutable blob.
		}
		var data []byte
		var encoding string
		err := tx.QueryRow(ctx, `SELECT data, encoding FROM ahp_state.checkpoint_blobs
 WHERE thread_id=$1 AND namespace=$2 AND channel=$3 AND version=$4`,
			threadID, cp.Namespace, part.Channel, part.Version).Scan(&data, &encoding)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !bytes.Equal(data, part.Data) || encoding != part.Encoding {
			return false, nil
		}
	}
	return true, nil
}
