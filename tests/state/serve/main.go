// Command serve is the historical entry point of the State acceptance harness.
// The implementation moved to internal/stateapi/stand and the public command is
// cmd/state-stand; this shim stays so that already-written acceptance tooling
// (which builds ./tests/state/serve) keeps working unchanged.
//
// Behaviour differences against cmd/state-stand, on purpose: the log prefix
// stays "state-serve", and no database allowlist is enforced unless
// AHP_STATE_DB_ALLOWLIST is set, because the historical callers configure their
// own isolated database.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ahp-platform/ahp/internal/stateapi/stand"
)

func main() {
	err := stand.RunLegacy(os.Args[1:])
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	fmt.Fprintln(os.Stderr, "state-serve: "+err.Error())
	os.Exit(1)
}
