// Command state-stand boots a REAL State API process for out-of-process
// consumers: real HTTPS with mandatory client certificates, real Ed25519 task
// tokens, real PostgreSQL, product migrations only - no mock and no httptest.
//
// It is the public stand of the State lane, meant for the SDK owner and for
// integration validation. One command, one isolated database:
//
//	go run ./cmd/state-stand \
//	  -env /home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env \
//	  -handshake /tmp/state-handshake/handshake.json
//
// It prints READY once it is serving and writes the handshake file (0600) that
// carries the address, the CA and the credentials of the mounted scope. Before
// opening anything it refuses a database outside -db-allowlist, and it prints
// only the database NAME - never a DSN, a key or a token.
//
// The implementation lives in internal/stateapi/stand; this file only maps the
// outcome onto a process exit status.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ahp-platform/ahp/internal/stateapi/stand"
)

func main() {
	err := stand.Run(os.Args[1:])
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	fmt.Fprintln(os.Stderr, "state-stand: "+err.Error())
	os.Exit(1)
}
