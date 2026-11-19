//go:build !linux

package compute

import "errors"

// reflinkCopy is Linux-only (FICLONE). On other platforms it always errors so
// copyCoW falls back to a byte copy; a control host without reflink never
// pretends a CoW clone happened.
func reflinkCopy(src, dst string) error {
	return errors.New("reflink: not supported on this platform")
}
