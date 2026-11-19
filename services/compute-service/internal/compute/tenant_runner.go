package compute

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// watchTenant stops an in-flight backend call when its durable tenant is fenced.
// The backend's outcome and release still determine whether erasure can finish.
func (r *Runner) watchOwner(ctx context.Context, scope contracts.Scope) (context.Context, context.CancelFunc) {
	watched, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watched.Done():
				return
			case <-ticker.C:
				var erased bool
				err := r.store.pool.QueryRow(watched, `SELECT EXISTS(SELECT 1 FROM ahp_compute.erased_tenants WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_compute.erased_subjects WHERE tenant=$1 AND subject=$2)`, scope.Tenant, scope.Subject).Scan(&erased)
				if err != nil || erased {
					cancel()
					return
				}
			}
		}
	}()
	return watched, cancel
}
func (r *Runner) FenceTenant(ctx context.Context, tenant string) error {
	return r.store.FenceTenant(ctx, tenant)
}

// DeleteTenant removes only validated data below this runner's configured root.
// A foreign-node incarnation, unknown command, live lease or unowned path keeps
// the deletion pending; no process is assumed dead from a metadata update.
func (r *Runner) DeleteTenant(ctx context.Context, tenant string) error {
	return r.deleteOwner(ctx, tenant, "")
}

func (r *Runner) deleteOwner(ctx context.Context, tenant, subject string) error {
	if r.jailBase == "" {
		return contracts.NewError(contracts.ErrUnavailable, "compute: filesystem root unavailable")
	}
	tx, err := r.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	// Keep cleanup serialized through the filesystem phase. Environment
	// reconciliation takes this barrier shared until its final rename/removal.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ahp_compute/tenant-erasure/"+tenant); err != nil {
		return err
	}
	var ids, paths []string
	err = func() error {
		if subject == "" {
			if err := r.store.tenantPending(ctx, tx, tenant); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ahp_compute/subject-erasure/"+tenant+"/"+subject); err != nil {
				return err
			}
			if err := r.store.subjectPending(ctx, tx, tenant, subject); err != nil {
				return err
			}
		}
		rows, err := tx.Query(ctx, `SELECT id FROM ahp_compute.computers WHERE tenant=$1 AND ($2='' OR subject=$2) ORDER BY id`, tenant, subject)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT c.id,s.path FROM ahp_compute.snapshots s JOIN ahp_compute.computers c ON c.id=s.computer_id WHERE c.tenant=$1 AND ($2='' OR c.subject=$2)`, tenant, subject)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, path string
			if err = rows.Scan(&id, &path); err != nil {
				rows.Close()
				return err
			}
			if err = withinTenantRoot(filepath.Join(r.jailBase, "computers", id), path); err != nil {
				rows.Close()
				return err
			}
			paths = append(paths, path)
		}
		err = rows.Err()
		rows.Close()
		return err
	}()
	if err != nil {
		return err
	}
	for _, id := range ids {
		// Resolve scope through this owner's table; the lease API still validates it.
		var scope contracts.Scope
		if err := tx.QueryRow(ctx, `SELECT tenant,deployment,subject FROM ahp_compute.computers WHERE id=$1 AND tenant=$2`, id, tenant).Scan(&scope.Tenant, &scope.Deployment, &scope.Subject); err != nil {
			return err
		}
		if r.leases != nil {
			n, err := r.leases.holdingCount(ctx, id)
			if err != nil {
				return err
			}
			if n > 0 {
				return contracts.NewError(contracts.ErrUnavailable, "compute: live lease keeps tenant deletion pending")
			}
		}
	}
	// Validate every snapshot and directory before the first removal, then use
	// pinned parent handles. Snapshots are already below their Computer: never
	// remove them by absolute path through potentially substituted ancestors.
	files, err := prepareOwnerFiles(r.jailBase, ids, paths)
	if err != nil {
		return err
	}
	defer files.Close()
	if err = files.RemoveAll(); err != nil {
		return err
	}
	for _, id := range ids {
		if r.leases != nil {
			if err := r.leases.redis.Del(ctx, r.leases.activityIndexKey(id), r.leases.writerKey(id)); err != nil {
				return err
			}
		}
	}
	if subject == "" {
		err = r.store.purgeTenantRows(ctx, tx, tenant)
	} else {
		_, err = tx.Exec(ctx, "DELETE FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2", tenant, subject)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
// withinTenantRoot checks snapshot ownership lexically. Physical validation is
// performed later by prepareOwnerFiles on pinned directory handles.
func withinTenantRoot(root, path string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return contracts.NewError(contracts.ErrForbidden, "compute: deletion path is outside owned root")
	}
	return nil
}

// ownerFiles pins the two service-owned parent directories. RemoveAll receives
// only a single Computer ID; neither a snapshot path nor an absolute parent is
// resolved again during mutation. Go's descriptor-relative RemoveAll unlinks
// symlinks rather than recursing through them, including a late replacement.
type ownerFiles struct {
	parents []*os.Root
	ids     []string
}

func (f *ownerFiles) Close() {
	for _, root := range f.parents {
		_ = root.Close()
	}
}

func (f *ownerFiles) RemoveAll() error {
	for _, root := range f.parents {
		for _, id := range f.ids {
			if err := root.RemoveAll(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareOwnerFiles(base string, ids, snapshots []string) (_ *ownerFiles, err error) {
	f := &ownerFiles{ids: ids}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	for _, id := range ids {
		if id == "" || id == "." || id == ".." || filepath.Base(id) != id {
			return nil, contracts.NewError(contracts.ErrForbidden, "compute: invalid owned directory")
		}
	}
	abs, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// The configured jail's parent is trusted host configuration (e.g. /var on
	// macOS is itself an OS alias). Reject symlinks at the jail and below it.
	parent, err := os.OpenRoot(filepath.Dir(abs))
	if os.IsNotExist(err) {
		return f, nil
	}
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	jail, err := openOwnerDirectory(parent, filepath.Base(abs))
	if err != nil {
		return nil, err
	}
	if jail == nil {
		return f, nil
	}
	defer jail.Close()
	for _, name := range []string{"computers", "procs"} {
		root, e := openOwnerDirectory(jail, name)
		if e != nil {
			return nil, e
		}
		if root == nil {
			continue
		}
		f.parents = append(f.parents, root)
		for _, id := range ids {
			if err = rejectOwnerSymlinks(root, id); err != nil {
				return nil, err
			}
		}
		if name == "computers" {
			for _, snapshot := range snapshots {
				pathAbs, e := filepath.Abs(snapshot)
				if e != nil {
					return nil, e
				}
				rel, e := filepath.Rel(filepath.Join(abs, name), pathAbs)
				if e != nil {
					return nil, e
				}
				if err = rejectOwnerSymlinks(root, rel); err != nil {
					return nil, err
				}
			}
		}
	}
	return f, nil
}

// OpenRoot follows links. Compare its pinned inode with a no-follow observation
// so replacing the directory with a link between Lstat and OpenRoot is refused.
func openOwnerDirectory(parent *os.Root, name string) (*os.Root, error) {
	before, err := parent.Lstat(name)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, contracts.NewError(contracts.ErrForbidden, "compute: deletion directory is not a physical directory")
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	after, err := root.Stat(".")
	if err != nil || !os.SameFile(before, after) {
		_ = root.Close()
		return nil, contracts.NewError(contracts.ErrForbidden, "compute: deletion directory changed during validation")
	}
	return root, nil
}

func rejectOwnerSymlinks(root *os.Root, path string) error {
	if !filepath.IsLocal(path) || path == "." {
		return contracts.NewError(contracts.ErrForbidden, "compute: deletion path is outside owned directory")
	}
	current := ""
	for _, part := range strings.Split(filepath.Clean(path), string(os.PathSeparator)) {
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return contracts.NewError(contracts.ErrForbidden, "compute: deletion path contains a symlink")
		}
	}
	return nil
}
