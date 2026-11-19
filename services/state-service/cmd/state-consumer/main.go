// Command state-consumer is the consumer-driven conformance driver of the State
// HTTP contract. It talks to a REAL stand (see cmd/state-stand) over
// HTTPS+mTLS and appends one JSONL evidence record per wire call with the
// status the server really returned, the contract code and the post-conditions
// observed in the response. Nothing is mocked.
//
//	go run ./cmd/state-consumer \
//	  -handshake /tmp/state-handshake/handshake.json \
//	  -evidence  /tmp/state-evidence.jsonl \
//	  -worker    w-review-1
//
// Exit status: 0 when every contract operation succeeded and no call deviated,
// 1 when the candidate deviated from the contract (or the run could not
// finish), 2 when the command line is wrong. Tokens, DSNs and keys are never
// printed; records carry only body hashes.
//
// The implementation lives in internal/stateapi/consumer; this file only maps
// the outcome onto a process exit status.
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
