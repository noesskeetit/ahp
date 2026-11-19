package dote2e_test

import "os"

// readLaneFile is a single seam for reading lane-private files so scenarios do
// not scatter os.ReadFile calls (and never log their contents).
func readLaneFile(path string) ([]byte, error) { return os.ReadFile(path) }
