# Frame-free legs: declaration, count guard, durable stand-in (DEF-DOTSDK-TERMINAL-1)

Rule, in one sentence: **a leg is completed without a terminal frame only if it is
declared in `frameFreeDeclarations` (`harness_test.go`), only for the reason that
entry states, and only while it presents the durable stand-in the entry names.**
Everything else is `unknown` or `frame_free_unproven` - label vocabulary **v2**,
stamped per artifact as `terminal_frame_vocabulary`; v1 spelled the two frame-free
states differently, so read a v1 artifact's labels from the source revision that
produced it instead of rewriting it. Under `AHP_DOT_E2E_STRICT_TERMINAL=1` both
`unknown` and `frame_free_unproven` are BLOCKED. Silence is not a pass and a
declaration is not evidence.

Why not "skip the strict rule for scenarios without turns": a scenario without a
turn can be declared after the fact for any red leg, which turns the terminal-frame
gate into advice. One mechanism covers all three acceptance scenarios.

## Declared legs (guard: `TestFrameFreeDeclarationCountsAreExact`)

| Scenario | Frame-free legs | Total |
|---|---|---|
| `personal-dot-journey` (+ `-defect-recorded` variant) | j1, j2, j3, j4, j5, j6, j7, j8, j9, j11 | 10 |
| `custom-agent-standalone-go` | state_work_created, state_run_terminal_cycle, model_gateway_on_real_qwen, sdk_get_metadata, artifact_round_trip, text_extraction_gate | 6 |
| `custom-agent-standalone-python` | python_agent | 1 |

j10 (`j10_browser_turn_def1_branch`) may never appear here - it has a durable
terminal frame, and a test refuses it. Adding a frame-free leg means editing this
table, the declaration, and the count in the same commit.

## Locator kinds (what stands in place of a frame)

| Kind | Where a verifier re-reads it without the stand |
|---|---|
| `durable_row` | the named PostgreSQL table + the exact query in the declaration |
| `public_face_read` | the OTLP access span in `runtime/<instance>/logs/backend.log`, named by `http.target` + `http.status_code` |
| `durable_store_file` | the connector's own store file + predicate (e.g. `notes.jsonl` must not contain the note) |

The artifact publishes, per leg, `frame_free_legs[<leg>]` with `frame_free_reason`,
the locator, `required_facts`, the **observed values**, `proven`, `provenance` and
`missing_facts`. Frame-free legs that offered nothing are additionally listed in
`frame_free_unproven_legs`.

Provenances are never conflated:

* `harness-reread` - the harness ran the query itself (`durableRow` observations in
  `harness_test.go`); the table must agree with the declaration, so evidence cannot
  point at an unrelated row, and a failed read (`read_error`) never proves a leg.
* `leg-declared` - the leg wrote the durable fact. Used by the journey legs today,
  including j9 (Laplace's): the declaration names where to re-check, and the values
  come from the leg's own read. Converting them to `harness-reread` needs
  `journey_test.go`, which this band does not own.

## Frame-free is not the same as unprovable

`agent_space_not_used` stopped being a leg in both SDK scenarios and became a
scenario-level attestation (no `status` key, so the classifier cannot read it as a
leg). It attests the scenario's own traffic, not a platform behaviour, and no
durable row anywhere says "no Space route was called"; declaring it frame-free would
have meant inventing a locator.

`custom-agent-standalone-go` gained `state_run_terminal_cycle`: the agent claims its
own run, works under the claim's own task token, writes a checkpoint and a KV answer,
and releases the run. `ahp_state.runs` reaching `done`, with the checkpoint and KV
rows present, is what a terminal frame would otherwise have said. The Python leg is
checked the same way, from the lane side, using the ids the child published - a child
that says "released" while the run row says otherwise is a FAIL.

## rc=0 is not acceptance

`go test` exits 0 while the artifact says `verdict=BLOCKED`. Two independent fixes:

* `AHP_DOT_E2E_ACCEPT=1` (acceptance mode) makes `record()` fail the test on any
  non-PASS verdict, so exit code and verdict agree.
* `tests/dote2e/cmd/dote2e-verdict` reads the verdict field of artifacts and exits
  `0` PASS / `4` BLOCKED / `5` FAIL-or-DEFECT / `3` unreadable, dry-run, dirty tree,
  missing legs, or `product_revision` absent/foreign. It takes `@list.txt` because
  artifact paths in this workspace contain spaces.

`tests/dote2e/run-acceptance.sh` is the acceptance series: journey, Go SDK, Python
SDK under `STRICT_TERMINAL=1` + `ACCEPT=1`, then the reader over exactly the
artifacts that run produced. **Acceptance is the reader's exit code and the verdict
field, never a runner's rc.**

## Presence is not proof: value-level requirements (added after j9 moved)

A key that exists can still contradict the requirement it was cited for. The
declaration therefore also states:

| Field | Meaning when checked |
|---|---|
| `must_be_empty` | the key must be recorded AND be an empty list/map/string. A non-empty list is a contradiction, and its absence is "not recorded" - never "empty". |
| `must_equal` | the key must be recorded AND equal the declared value (`why` states what the value means; a numeric comparison is type-tolerant, because the leg writes `int` and a re-read artifact yields `float64`). |
| `expected_http_any_of` | for a public face, the set of contract codes that count as the answer. A single expected code is refused for j9: since the canonical NOT_FOUND merge a foreign read is answered 404 on most kinds, and demanding 403 would make an honest refusal unprovable. |

Guarded by `TestFrameFreeDeclarationsNameRealLocators` (a `must_be_empty` key may not
also sit in `observed_keys`, a `must_equal` key must be an observed fact and must say
`why`, and a face locator must name at least one status), plus
`TestFrameFreeProofRejectsABreachTheLegItselfRecords` and
`TestFrameFreeProofRequiresTheDeclaredValueNotJustTheKey`.

j9 is the case that forced this. Its three refusal booleans are the leg's own
conclusion; they are no longer sufficient. The declaration now requires the resources
the stranger was shown, the per-resource contract codes and probes, `leaked_resources`
empty, `foreign_subject_grant_rows` = 0 (a grant row for the stranger inside the
owner's tenant is a breach in the durable record whatever the endpoints answered), and
`owner_control_sentinel_ran` = true (a trap that never ran proves nothing about the
owner's own access). The leg itself belongs to Laplace (`isolation-scope`); only the
declaration and the verdict rule are here.

`provenance` for j9 remains `leg-declared`, and that is stated rather than dressed up:
this harness has no OTLP span reader, so nobody should read j9's evidence as a
harness-side re-read of the access log. Adding that reader is an open item, not a
claimed property.

## The exit code of the RUNNER is not the exit code of the READER (measured 2026-11-10T05:23Z)

`go run ./tests/dote2e/cmd/dote2e-verdict` prints `exit status 3` and then returns **1** of its
own. Left as it was, `run-acceptance.sh` - which ended in `go run` - would have handed every
caller a flat 1 for every non-zero verdict, flattening exactly the 0/3/4/5 distinction this tool
publishes. Measured, not inferred:

| invocation | exit |
|---|---|
| reader binary, BLOCKED artifact | 4 |
| `go run` same reader, same artifact | 1 |

The runner now builds the binary and execs it, capturing the real code (`rc=0 is not acceptance`
still holds: 0 means every artifact in the list said `verdict=PASS` at the expected revision).

Aggregation over a list is by rank, not by number: unusable(3) outranks FAIL(5), which outranks
BLOCKED(4) - a reader that cannot read one artifact cannot certify the series, whatever the others
say.
