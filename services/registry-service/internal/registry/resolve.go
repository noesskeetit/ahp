package registry

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// EffectiveConfig is what ResolveConfig hands a task: the pinned version, the
// narrowed capabilities, the tenant AgentConfig document, and the model routes
// the manifest names.
//
// It exists because the platform contract fixes two things per task (PDF rev
// 3.2, c. 42 "Версии. Версия деплоя и AgentConfig закрепляются за задачей, и
// выкладка не меняет поведения начатых задач") and because a manifest's
// capabilities are a ceiling the tenant narrows at install time (c. 45:
// "Возможности из манифеста -- потолок: администратор tenant может сузить их при
// установке").
type EffectiveConfig struct {
	Tenant         string                `json:"tenant"`
	Installation   string                `json:"installation_id"`
	AgentID        string                `json:"agent_id"`
	Environment    string                `json:"environment"`
	VersionID      string                `json:"version_id"`
	Digest         string                `json:"digest"`
	Manifest       Manifest              `json:"manifest"`
	Capabilities   Capabilities          `json:"capabilities"`
	ConfigRevision int64                 `json:"config_revision"`
	Config         json.RawMessage       `json:"config"`
	ModelRoutes    map[string]ModelRoute `json:"model_routes,omitempty"`
	MissingRoutes  []string              `json:"missing_routes,omitempty"`
	// SharesState is the fact the catalog card must show: this agent declares
	// shared state spaces, i.e. it can keep data between users.
	SharesState bool   `json:"shares_state"`
	Task        string `json:"task,omitempty"`
	// GrantTask is trusted pin metadata for owner-side State reads. Task remains
	// the public runtime task ID; no execution credential is serialized here.
	GrantTask string `json:"-"`
	// DrainAction reports what the drain rules did for this task: "" when the
	// task simply runs its pinned/current version, "waiting" while the previous
	// version still serves it inside the drain window, and "transferred" when the
	// window elapsed and the task moved onto the running compatible version. It
	// exists because ResolveConfig must report what ACTUALLY executes, not what
	// was intended at some earlier moment.
	DrainAction string `json:"drain_action,omitempty"`
	// MigratedFrom names the version a transferred task was moved off.
	MigratedFrom string `json:"migrated_from,omitempty"`
}

// ResolveConfig is the Registry's ResolveConfig of the platform service table
// (PDF rev 3.2, c. 28, Registry row).
//
// taskID may be empty: then the current installation version is resolved. When a
// task IS named and it has a pin, the pinned version wins even if the
// installation was promoted or rolled back since -- a running task's behaviour
// must not change under it.
func (s *Store) ResolveConfig(ctx context.Context, tenant, installationID, taskID string) (EffectiveConfig, error) {
	if err := requireID("tenant", tenant); err != nil {
		return EffectiveConfig{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return EffectiveConfig{}, err
	}
	if taskID != "" {
		if err := requireID("task id", taskID); err != nil {
			return EffectiveConfig{}, err
		}
	}

	var out EffectiveConfig
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		versionID := inst.VersionID
		var pinnedConfig []byte
		var configRevision *int64
		var grantTask string
		isPinned := false
		if taskID != "" {
			var pinned, pinnedInstallation string
			err := tx.QueryRow(ctx, `
SELECT version_id, installation_id, config_data, config_revision, grant_task_id FROM ahp_registry.task_pins WHERE tenant = $1 AND task_id = $2`, tenant, taskID).Scan(&pinned, &pinnedInstallation, &pinnedConfig, &configRevision, &grantTask)
			switch {
			case err == nil:
				if pinnedInstallation != installationID {
					return contracts.NewError(contracts.ErrConflict, "task belongs to another installation")
				}
				if configRevision == nil || len(pinnedConfig) == 0 {
					return contracts.NewError(contracts.ErrConflict, "legacy task pin has no config snapshot; cannot safely resolve")
				}
				versionID, isPinned = pinned, true
			case errors.Is(err, pgx.ErrNoRows):
				// Not pinned yet: the caller resolves what the task WOULD start
				// on, and the gateway pins exactly this version when accepting.
			default:
				return storageError("read task pin", err)
			}
		}
		version, err := getVersion(ctx, tx, versionID)
		if err != nil {
			return err
		}
		if version.AgentID != inst.AgentID {
			return contracts.NewError(contracts.ErrConflict, "installation points at another agent's version")
		}

		// Decide what the task ACTUALLY executes, then validate that answer.
		//
		// A pinned task whose version was replaced is served by the drain rules:
		// the previous version keeps it inside the manifest's window, and after
		// the window it either moves to the running compatible version or is
		// refused with a reason. A task with no pin yet may only start on a
		// version the supply chain currently admits -- otherwise a row written
		// behind the Registry's back, or a verdict that failed after the promote,
		// would still take new tasks here.
		drain := drainOutcome{VersionID: version.ID}
		if isPinned {
			if version.ID != inst.VersionID {
				running, rerr := getVersion(ctx, tx, inst.VersionID)
				if rerr != nil {
					return rerr
				}
				if drain, rerr = s.applyPinnedDrain(ctx, tx, tenant, inst.Installation, version, running, taskID); rerr != nil {
					return rerr
				}
				if drain.VersionID != version.ID {
					if version, rerr = getVersion(ctx, tx, drain.VersionID); rerr != nil {
						return rerr
					}
				}
			}
		} else if err := s.requireDeployAdmission(ctx, tx, version, inst.Environment); err != nil {
			return err
		}
		if err := requireGrantedSubset(version.Manifest.Capabilities, inst.Capabilities); err != nil {
			return contracts.NewError(contracts.ErrConflict,
				"stored grant exceeds the pinned manifest ceiling: "+err.Error())
		}

		out = EffectiveConfig{
			Tenant:       tenant,
			Installation: inst.ID,
			AgentID:      inst.AgentID,
			Environment:  inst.Environment,
			VersionID:    version.ID,
			Digest:       version.Digest,
			Manifest:     version.Manifest,
			Capabilities: inst.Capabilities,
			Config:       json.RawMessage(`{}`),
			SharesState:  len(version.Manifest.State.Shared) > 0,
			Task:         taskID,
			GrantTask:    grantTask,
			DrainAction:  drain.Action,
			MigratedFrom: drain.MigratedFrom,
		}
		if isPinned {
			out.Config, out.ConfigRevision = pinnedConfig, *configRevision
		} else {
			config, err := getConfig(ctx, tx, tenant, inst.AgentID)
			switch {
			case err == nil:
				out.Config, out.ConfigRevision = config.Data, config.Revision
			case contracts.Code(err) != contracts.ErrNotFound:
				return err
			}
		}

		// The pinned version's model routes are resolved with it, so a task
		// never has to guess which route a capability name means.
		if len(version.Manifest.Capabilities.Models) > 0 {
			out.ModelRoutes = map[string]ModelRoute{}
			for _, name := range version.Manifest.Capabilities.Models {
				route, rerr := resolveRouteInTx(ctx, tx, tenant, name)
				if rerr != nil {
					if contracts.Code(rerr) == contracts.ErrNotFound {
						out.MissingRoutes = append(out.MissingRoutes, name)
						continue
					}
					return rerr
				}
				out.ModelRoutes[name] = route
			}
		}
		return nil
	})
	if err != nil {
		return EffectiveConfig{}, err
	}
	return out, nil
}

// resolveRouteInTx reads one route document inside a transaction.
func resolveRouteInTx(ctx context.Context, tx pgx.Tx, tenant, route string) (ModelRoute, error) {
	var (
		out ModelRoute
		doc []byte
	)
	err := tx.QueryRow(ctx, `
SELECT id, tenant, route, revision, document FROM ahp_registry.model_routes
WHERE tenant = $1 AND route = $2`, tenant, route).
		Scan(&out.ID, &out.Tenant, &out.Route, &out.Revision, &doc)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ModelRoute{}, contracts.NewError(contracts.ErrNotFound, "model route not found")
		}
		return ModelRoute{}, storageError("read model route", err)
	}
	out.Document = json.RawMessage(doc)
	return out, nil
}
