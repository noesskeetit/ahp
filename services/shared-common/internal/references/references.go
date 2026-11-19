// Package references defines the content-free contract by which each owner
// reports durable liveness and artifact references. Implementations own their
// storage; a collector never queries another service's tables.
package references

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"regexp"
)

type Ref struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}
type Artifact struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}
type Result struct {
	Alive      map[Ref]bool
	Referenced map[string]bool
}
type Owner interface {
	InspectReferences(context.Context, string, []Ref, []Artifact) (Result, error)
}

func Empty() Result { return Result{Alive: map[Ref]bool{}, Referenced: map[string]bool{}} }
func IDs(refs []Ref) (tasks, contexts []string) {
	tasks = []string{}
	contexts = []string{}
	for _, r := range refs {
		switch r.Kind {
		case "task", "run":
			tasks = append(tasks, r.ID)
		case "context", "thread":
			contexts = append(contexts, r.ID)
		}
	}
	return
}
func ArtifactIDs(arts []Artifact) (ids, digests []string) {
	ids = []string{}
	digests = []string{}
	for _, a := range arts {
		ids = append(ids, a.ID)
		digests = append(digests, a.Digest)
	}
	return
}
func Mark(out *Result, refs []Ref, taskIDs, contextIDs []string) {
	for _, r := range refs {
		var ids []string
		switch r.Kind {
		case "task", "run":
			ids = taskIDs
		case "context", "thread":
			ids = contextIDs
		}
		for _, id := range ids {
			if id != "" && r.ID == id {
				out.Alive[r] = true
			}
		}
	}
}

var schemaPattern = regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,60}$`)

func Validate(pool *pgxpool.Pool, schema string) error {
	if pool == nil || !schemaPattern.MatchString(schema) {
		return fmt.Errorf("references: owner storage configuration invalid")
	}
	return nil
}
func QueryIDs(ctx context.Context, pool *pgxpool.Pool, out *Result, query string, args ...any) error {
	return Each(ctx, pool, query, func(rows pgx.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		out.Referenced[id] = true
		return nil
	}, args...)
}

// Each deliberately propagates missing-schema/table/column errors: an absent
// owner is unavailable evidence, never proof that its references are dead.
func Each(ctx context.Context, pool *pgxpool.Pool, query string, scan func(pgx.Rows) error, args ...any) error {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
