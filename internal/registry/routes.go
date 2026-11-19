package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// ModelRoute is one tenant model route: the Registry owns "маршруты моделей"
// (PDF rev 3.2, c. 28, Registry row), and a manifest names a route in
// capabilities.models rather than a provider or a credential.
type ModelRoute struct {
	ID        string          `json:"id"`
	Tenant    string          `json:"tenant"`
	Route     string          `json:"route"`
	Revision  int64           `json:"revision"`
	Document  json.RawMessage `json:"document"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

var routePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,63}$`)

// maxRouteDocumentBytes bounds a route document.
const maxRouteDocumentBytes = 64 << 10

// PutModelRoute writes a route document with compare-and-swap on the revision
// counter, exactly like an AgentConfig write. expectedRevision 0 creates.
//
// A route document must be a JSON object naming a "model". It must NOT contain
// credentials: model provider secrets live in the Tool & Credential Gateway /
// Vault, never in the Registry (PDF rev 3.2, c. 48: токены внешних систем
// существуют только в Tool & Credential Gateway).
func (s *Store) PutModelRoute(ctx context.Context, tenant, route string, expectedRevision int64, document json.RawMessage) (ModelRoute, error) {
	if err := requireID("tenant", tenant); err != nil {
		return ModelRoute{}, err
	}
	if !routePattern.MatchString(route) {
		return ModelRoute{}, invalid("model route must be a lowercase identifier of at most 64 bytes")
	}
	if expectedRevision < 0 {
		return ModelRoute{}, contracts.NewError(contracts.ErrInvalid, "expected revision must not be negative")
	}
	if len(document) == 0 || len(document) > maxRouteDocumentBytes {
		return ModelRoute{}, invalid("model route document must be non-empty and within 64 KiB")
	}
	var probe map[string]any
	if err := json.Unmarshal(document, &probe); err != nil {
		return ModelRoute{}, invalid("model route document must be a JSON object")
	}
	model, _ := probe["model"].(string)
	if model == "" {
		return ModelRoute{}, invalid("model route document must name a model")
	}
	for forbidden := range probe {
		switch forbidden {
		case "token", "api_key", "apikey", "secret", "password", "credentials":
			return ModelRoute{}, invalid("model route document must not carry credentials: " + forbidden)
		}
	}

	var out ModelRoute
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"ahp_registry/route/"+tenant+"/"+route); err != nil {
			return storageError("lock route", err)
		}
		var (
			id, created, updated string
			revision             int64
		)
		err := tx.QueryRow(ctx, `
SELECT id, revision, created_at::text, updated_at::text FROM ahp_registry.model_routes
WHERE tenant = $1 AND route = $2`, tenant, route).Scan(&id, &revision, &created, &updated)
		switch {
		case err == nil:
			if revision != expectedRevision {
				return contracts.NewError(contracts.ErrConflict, fmt.Sprintf(
					"model route revision is %d, expected %d", revision, expectedRevision))
			}
			if _, err := tx.Exec(ctx, `
UPDATE ahp_registry.model_routes SET document = $1::jsonb, revision = revision + 1, updated_at = now()
WHERE tenant = $2 AND route = $3 AND revision = $4`,
				string(document), tenant, route, expectedRevision); err != nil {
				return storageError("update model route", err)
			}
			out = ModelRoute{ID: id, Tenant: tenant, Route: route, Revision: revision + 1,
				Document: append(json.RawMessage(nil), document...)}
			return nil
		case errors.Is(err, pgx.ErrNoRows):
			if expectedRevision != 0 {
				return contracts.NewError(contracts.ErrConflict,
					"model route does not exist yet, expected revision must be 0")
			}
			var nextRevision int64
			if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT revision FROM ahp_registry.model_route_generations WHERE tenant=$1 AND route=$2),0)+1`, tenant, route).Scan(&nextRevision); err != nil {
				return storageError("read route generation", err)
			}
			out = ModelRoute{ID: contracts.NewID(), Tenant: tenant, Route: route, Revision: nextRevision,
				Document: append(json.RawMessage(nil), document...)}
			if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.model_routes (id, tenant, route, revision, document) VALUES ($1, $2, $3, $4, $5::jsonb)`,
				out.ID, tenant, route, nextRevision, string(document)); err != nil {
				if isUniqueViolation(err) {
					return contracts.NewError(contracts.ErrConflict, "model route already exists")
				}
				return storageError("store model route", err)
			}
			return nil
		default:
			return storageError("read model route", err)
		}
	})
	if err != nil {
		return ModelRoute{}, err
	}
	return out, nil
}

// ResolveModelRoute reads one route document of the caller tenant.
func (s *Store) ResolveModelRoute(ctx context.Context, tenant, route string) (ModelRoute, error) {
	if err := requireID("tenant", tenant); err != nil {
		return ModelRoute{}, err
	}
	if !routePattern.MatchString(route) {
		return ModelRoute{}, invalid("model route must be a lowercase identifier of at most 64 bytes")
	}
	var (
		out     ModelRoute
		doc     []byte
		created time.Time
		updated time.Time
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, tenant, route, revision, document, created_at, updated_at FROM ahp_registry.model_routes
WHERE tenant = $1 AND route = $2`, tenant, route).
		Scan(&out.ID, &out.Tenant, &out.Route, &out.Revision, &doc, &created, &updated)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ModelRoute{}, contracts.NewError(contracts.ErrNotFound, "model route not found")
		}
		return ModelRoute{}, storageError("read model route", err)
	}
	out.Document = json.RawMessage(doc)
	out.CreatedAt = created.UTC()
	out.UpdatedAt = updated.UTC()
	return out, nil
}

// ListModelRoutes lists the tenant's routes without documents.
func (s *Store) ListModelRoutes(ctx context.Context, tenant string) ([]ModelRoute, error) {
	if err := requireID("tenant", tenant); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT id, tenant, route, revision, created_at, updated_at FROM ahp_registry.model_routes
WHERE tenant = $1 ORDER BY route`, tenant)
	if err != nil {
		return nil, storageError("list model routes", err)
	}
	defer rows.Close()
	out := make([]ModelRoute, 0, 4)
	for rows.Next() {
		var r ModelRoute
		if err := rows.Scan(&r.ID, &r.Tenant, &r.Route, &r.Revision, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, storageError("scan model route", err)
		}
		r.CreatedAt = r.CreatedAt.UTC()
		r.UpdatedAt = r.UpdatedAt.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}
