# Standalone AHP owner clients

See `docs/sdk/public-clients.md` for credentials, active lease requirements,
request shapes and the complete parity/evidence table. These programs are
framework-neutral. They consume an existing State lease, record an Intent, and
use its persistent ID as the owner operation key. They do not claim/create
replacement leases or regenerate operation IDs on recovery.

Use `go run ./sdk/examples/public-clients/go -h` or
`PYTHONPATH=sdk/python python3 sdk/examples/public-clients/python.py --help`.

`Call`/`Get`/`Fence` return a content-free receipt by design; the raw stored
result of one successful operation is read by a separate call, see
`sdk/examples/tool-result`. Compute Run needs an executing backend;
physical mock Compute intentionally executes nothing. A2A send acceptance is
not task completion: use Get/Resubscribe for the durable task outcome.
