//go:build unix && !linux && !darwin

package vmruntime

import "errors"

const processWaitSupported = false

func waitProcessExit(int) error {
	return errors.ErrUnsupported
}
