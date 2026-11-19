package harness

import "os"

func environ() []string { return os.Environ() }
