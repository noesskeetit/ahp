package httpapi

import (
	"context"
	"crypto/subtle"
	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"strings"
	"time"
)

type devAccess struct{ Token string }

func (d devAccess) Authorize(_ context.Context, token string, req memory.AccessRequest) (memory.Identity, error) {
	for _, kind := range []string{"agent", "runtime", "console", "platform"} {
		if subtle.ConstantTimeCompare([]byte(token), []byte(d.Token+"."+kind)) != 1 {
			continue
		}
		if (strings.HasPrefix(req.Action, "runtime:") && kind != "runtime" && kind != "platform") || (req.Action == "memory:manage" && kind != "console" && kind != "platform") || (req.Action == "project:delete" && kind != "platform") {
			return memory.Identity{}, memory.Fail(403, "access_denied", "actor cannot perform this operation")
		}
		return memory.Identity{TenantID: "local", UserID: "local-user", ActorID: "local-" + kind, ActorKind: kind, GrantID: "local-" + kind, TaskID: "local-task", ExpiresAt: time.Now().UTC().Add(24 * time.Hour)}, nil
	}
	return memory.Identity{}, memory.Fail(401, "authentication_required", "invalid development token")
}
func (d devAccess) Revalidate(_ context.Context, id memory.Identity, _ memory.AccessRequest) error {
	if id.TenantID != "local" || id.UserID != "local-user" {
		return memory.Fail(403, "access_denied", "invalid development owner")
	}
	return id.Validate(time.Now().UTC())
}
func (d devAccess) Bind(context.Context, memory.Identity, []string) error { return nil }
