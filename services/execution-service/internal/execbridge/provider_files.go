package execbridge

import (
	"context"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

// WithProviderFiles mounts an explicitly configured file-copy owner. Ordinary
// text generation never uploads attachments as a side effect of this setting.
func (b *Bridge) WithProviderFiles(files *model.FileService) {
	b.files = files
	b.svc.WithProviderFiles(files)
}

func (b *Bridge) DeleteSubjectFiles(ctx context.Context, tenant, subject string) error {
	return b.svc.DeleteSubjectFiles(ctx, tenant, subject)
}

// RouteFileProvider uses the same live task journal and operator provider
// policy as generation routing, with the artifact's source restrictions merged.
func (b *Bridge) RouteFileProvider(ctx context.Context, a model.FileActor, source labels.Set) (string, error) {
	if b.router == nil || b.taskLabels == nil {
		return "", model.FilesNotSupported()
	}
	authority, ok := b.taskLabels.(TaskGrantAuthority)
	if !ok {
		return "", contracts.NewError(contracts.ErrUnavailable, "model files: live task authority unavailable")
	}
	if err := authority.AuthorizeModelTask(ctx, a.Scope, a.Task); err != nil {
		return "", err
	}
	set, err := b.taskLabels.LabelsForContext(ctx, a.Scope, a.Context)
	if err != nil {
		return "", err
	}
	set = labels.Merge(set, source)
	if b.deploymentLabels != nil {
		deployment, e := b.deploymentLabels.LabelsFor(ctx, a.Scope.Tenant, a.Scope.Deployment)
		if e != nil {
			return "", e
		}
		set = labels.Merge(set, deployment)
	}
	route, err := b.router.Resolve(a.Scope, routing.Request{Labels: set, Class: b.executionClass, Model: b.modelName})
	return route.Provider, err
}

func (b *Bridge) serveProviderFile(w http.ResponseWriter, r *http.Request, op string) {
	if b.files == nil {
		transport.WriteError(w, r, model.FilesNotSupported())
		return
	}
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "model files: verified task required"))
		return
	}
	a := model.FileActor{Scope: p.Scope, Task: p.Claims.Task, Context: p.Claims.Context}
	var in struct {
		Key        string `json:"key"`
		ArtifactID string `json:"artifact_id,omitempty"`
		ID         string `json:"id,omitempty"`
	}
	if err := transport.ReadJSON(w, r, &in, 16<<10); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var f model.FileCopy
	var err error
	switch op {
	case "CopyFile":
		f, err = b.files.Copy(r.Context(), a, in.Key, in.ArtifactID)
	case "GetFile":
		f, err = b.files.Get(r.Context(), a, in.ID)
	case "DeleteFile":
		f, err = b.files.Delete(r.Context(), a, in.Key, in.ID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, f)
}

func (b *Bridge) FenceSubject(ctx context.Context, tenant, subject string) error {
	return b.svc.FenceSubject(ctx, tenant, subject)
}
func (b *Bridge) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return b.svc.DeleteSubject(ctx, tenant, subject)
}
