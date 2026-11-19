package compute

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// EnvironmentManager materialises a Computer's environment (installed
// packages, runtime layers) as real on-disk prefixes inside the jail base:
//
//	<jailBase>/computers/<computerID>/env/rev-<n>/   one prefix per revision
//	<jailBase>/computers/<computerID>/env/current    cursor: the applied rev
//
// Installing a package is an exclusive environment operation that materialises
// a NEW prefix from the applied one; the install command runs inside that
// prefix. A successful install moves the cursor to the new prefix; a failed
// install is rolled back for real: the throwaway prefix is deleted and the
// cursor stays on the parent, so the applied environment is byte-for-byte
// what it was before the attempt.
type EnvironmentManager struct {
	base string
}

func NewEnvironmentManager(base string) *EnvironmentManager {
	if base == "" {
		base = filepath.Join(os.TempDir(), "ahp-compute-host")
	}
	return &EnvironmentManager{base: filepath.Clean(base)}
}

func (m *EnvironmentManager) envRoot(computerID string) string {
	if err := validIdentifier(computerID, "computer id"); err != nil {
		return ""
	}
	return filepath.Join(m.base, "computers", computerID, "env")
}

func (m *EnvironmentManager) prefixDir(computerID string, rev int64) string {
	return filepath.Join(m.envRoot(computerID), fmt.Sprintf("rev-%d", rev))
}

func (m *EnvironmentManager) cursorFile(computerID string) string {
	return filepath.Join(m.envRoot(computerID), "current")
}

// CurrentRevision reads the applied cursor, or 0 when the environment has not
// been initialised.
func (m *EnvironmentManager) CurrentRevision(computerID string) (int64, error) {
	b, err := os.ReadFile(m.cursorFile(computerID))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("compute: bad environment cursor: %w", err)
	}
	return n, nil
}

// Materialise creates the throwaway prefix rev from its parent prefix (or
// empty when parent is 0). It fails if the prefix already exists so a
// half-finished install can never be silently overwritten.
func (m *EnvironmentManager) Materialise(computerID string, rev, parent int64) error {
	if m.envRoot(computerID) == "" || rev < 1 {
		return invalid("environment materialise: bad computer id or revision")
	}
	root := m.envRoot(computerID)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	dir := m.prefixDir(computerID, rev)
	if _, err := os.Stat(dir); err == nil {
		return contracts.NewError(contracts.ErrConflict, "environment prefix already exists")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if parent >= 1 {
		src := m.prefixDir(computerID, parent)
		if st, err := os.Stat(src); err == nil && st.IsDir() {
			if err := copyTree(src, dir); err != nil {
				_ = os.RemoveAll(dir)
				return err
			}
		}
		// A missing parent prefix means the applied environment is the base
		// image: the new prefix starts empty.
	}
	return nil
}

// Activate moves the cursor to rev. The prefix must exist.
func (m *EnvironmentManager) Activate(computerID string, rev int64) error {
	if m.envRoot(computerID) == "" || rev < 1 {
		return invalid("environment activate: bad computer id or revision")
	}
	if _, err := os.Stat(m.prefixDir(computerID, rev)); err != nil {
		return fmt.Errorf("compute: cannot activate missing environment prefix rev-%d: %w", rev, err)
	}
	return os.WriteFile(m.cursorFile(computerID), []byte(strconv.FormatInt(rev, 10)+"\n"), 0o600)
}

// Rollback deletes the throwaway prefix of a failed install. The cursor is
// asserted to point elsewhere first, so a rollback can never remove the
// applied environment.
func (m *EnvironmentManager) Rollback(computerID string, rev int64) error {
	if m.envRoot(computerID) == "" || rev < 1 {
		return invalid("environment rollback: bad computer id or revision")
	}
	cur, err := m.CurrentRevision(computerID)
	if err != nil {
		return err
	}
	if cur == rev {
		return contracts.NewError(contracts.ErrConflict,
			"cannot roll back the applied environment revision")
	}
	return os.RemoveAll(m.prefixDir(computerID, rev))
}

// EnvFile is one file of a materialised environment prefix.
type EnvFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

// EnvFiles lists the applied environment prefix with per-file SHA-256 digests
// — the observable proof that a rollback restored the previous bytes.
func (m *EnvironmentManager) EnvFiles(computerID string) ([]EnvFile, error) {
	cur, err := m.CurrentRevision(computerID)
	if err != nil {
		return nil, err
	}
	if cur == 0 {
		return nil, nil
	}
	var out []EnvFile
	err = filepath.WalkDir(m.prefixDir(computerID, cur), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(m.prefixDir(computerID, cur), path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return err
		}
		out = append(out, EnvFile{Path: filepath.ToSlash(rel), Size: info.Size(), Digest: digest})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// copyTree copies src into dst recursively (files keep their mode bits). A
// dst inside src is supported: the dst subtree is pruned from the walk so the
// copy cannot recurse into itself.
func copyTree(src, dst string) error {
	src, err := filepath.Abs(src)
	if err != nil {
		return err
	}
	dst, err = filepath.Abs(dst)
	if err != nil {
		return err
	}
	inside := strings.HasPrefix(dst, src+string(os.PathSeparator))
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if inside && d.IsDir() && path != src {
			abs, _ := filepath.Abs(path)
			if abs == dst {
				return filepath.SkipDir
			}
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		mode := os.FileMode(0o600)
		if info, err := d.Info(); err == nil {
			mode = info.Mode().Perm()
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
}

// RunInPrefix runs command inside the prefix of rev as a real host process
// (its own process group, output bounded). It returns the exit code; a
// non-zero exit is a failed install, not an error of the substrate.
func (m *EnvironmentManager) RunInPrefix(ctx context.Context, computerID string, rev int64, command string, timeoutSecs int64) (int, string, string, error) {
	dir := m.prefixDir(computerID, rev)
	if _, err := os.Stat(dir); err != nil {
		return 0, "", "", fmt.Errorf("compute: environment prefix rev-%d missing: %w", rev, err)
	}
	if timeoutSecs <= 0 {
		timeoutSecs = 60
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "/bin/sh", "-c", command)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir}
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(cmd.SysProcAttr)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return 0, "", "", fmt.Errorf("compute: environment install failed to start: %w", err)
	}
	pid := cmd.Process.Pid
	// An environment install is not a ledger command, so it registers with no command row.
	registerProcess(m.base, computerID, pid, pid, "")
	err := cmd.Wait()

	res := stdout.String()
	if len(res) > maxOutputBytes {
		res = res[:maxOutputBytes]
	}
	errOut := stderr.String()
	if len(errOut) > maxOutputBytes {
		errOut = errOut[:maxOutputBytes]
	}
	switch {
	case err == nil:
		return 0, res, errOut, nil
	case runCtx.Err() == context.DeadlineExceeded:
		return -1, res, errOut+"\ncompute: environment install exceeded timeout", nil
	case isExitError(err):
		return exitCode(err), res, errOut, nil
	default:
		return 0, res, errOut, fmt.Errorf("compute: environment install failed to run: %w", err)
	}
}

func isExitError(err error) bool {
	_, ok := err.(*exec.ExitError)
	return ok
}

func exitCode(err error) int {
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return 0
}
