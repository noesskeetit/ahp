//go:build !darwin

package toolsapi

import "os"

func syncEffectFile(file *os.File) error {
	return file.Sync()
}
