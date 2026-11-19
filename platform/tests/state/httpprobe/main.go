// Command httpprobe is the historical entry point of the State wire driver.
// The implementation moved to internal/stateapi/consumer and the public command
// is cmd/state-consumer; this shim stays so the path the acceptance harness has
// always used keeps building and behaves the same, exit codes included
// (0 = contract held, 1 = deviation, 2 = bad command line).
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ahp-platform/ahp/internal/stateapi/consumer"
)

func main() {
	err := consumer.Run(os.Args[1:])
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	fmt.Fprintln(os.Stderr, "state-consumer: "+err.Error())
	if errors.Is(err, consumer.ErrUsage) {
		os.Exit(2)
	}
	os.Exit(1)
}
