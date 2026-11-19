# tests/dote2e: what this band owns, and how it merges

Erdos, band `dot-identity-carrier` (branch `root-pdf-closure-identity-carrier`, base 8484f8ce).
Written because `tests/dote2e/coverage_test.go` is being edited by another band at the same time
(`isolation-scope`, Laplace) and the director resolves any overlap, not the authors.

## Files and functions owned by this band
- `identity_carrier_test.go` (new, entirely mine): `providerFamilies`, `laneIdentityTokenRe`,
  `laneModelShapeRe`, `laneProfileShapeRe`, `laneHostShapeRe`, `laneSourceExtRe`,
  `laneIdentityShaped`, `laneIdentitiesIn`, `scanLaneForIdentities`,
  `TestThisLaneMentionsNoProviderIdentity`, `TestLaneIdentityScanFiresOnASyntheticCarrier`.
- `coverage_test.go`: ONE statement inside `journeyGreenChecks()` - the `j3_model_real_qwen` fixture
  line and the comment above it. Nothing else in this file: not the gate helpers, not the spec table,
  not `journeyCoverageClaims`, not any j9 content.
- `model_cause_attribution_test.go`: ONE struct field (`model:`) inside
  `TestEmptyCompletionIsAttributedToObservedFacts`, plus its comment.

## Regions this band will not touch (Laplace's, in isolation-scope)
`legListEmpty`, `legIntExactly`, the `notProvenFromLeg` field and the block in
`journeyCoverageClaims` that joins leg-declared gaps, the whole j9 entry of `journeyCoverageSpecs()`,
the j9 fixture in `journeyGreenChecks()`, `TestJ9IsolationClaimDegradesOnLeakOrMissingEvidence`, and
the new `isolation_resources_test.go` (which also declares `legStrings`, used by his coverage gates -
so his unit must merge that file together with `coverage_test.go` or the package will not build).

## Merge prediction, measured rather than assumed
Three-way file merge of `tests/dote2e/coverage_test.go` with base 8484f8ce, ours = a76fccd0, theirs =
the isolation-scope working file (`git merge-file -p --diff3`): exit 0, zero conflict markers, and the
output contains both my fixture label and his nine `legListEmpty`/`notProvenFromLeg` occurrences. The
changed lines are six apart inside `journeyGreenChecks()`, which is close enough to overlap context
but not to overlap content, so either merge order is clean.

## Gate state this band is responsible for
`isolation-scope` did not touch the two fixture lines, so its tree still carries the canonical routed
model identity twice. Consequences:
- merging this band does not undo his work, and merging his band does not undo this fix;
- the wide gate stays RED until this band (both commits together) is in the trunk;
- the strict lane self-check in `identity_carrier_test.go` scans the whole directory, so it must never
  be merged without its fix commit - a76fccd0 removes the carriers it would otherwise report.


## Frame-free band (branch `root-pdf-closure-sdk-terminal-framefree`, base 35683c38) - overlap notes

### `journeyGreenChecks()` fixture: which keys this band added, and the one leg it did not
The frame-free rule (a declared frame-free leg is completed only while it presents the durable
facts its declaration names) made the synthetic green fixture insufficient: legs classified
`frame_free_unproven`, so the journey verdict went BLOCKED. The fixture was repaired by adding
the keys the legs actually publish, never by relaxing a rule:

| Leg | Key added | Whose leg content that key is |
|---|---|---|
| `j3_model_real_qwen` | `generation` | this band (the j3 fixture line is claimed above) |
| `j5_memory` | `operation_states` | not claimed by anyone; the leg publishes it |
| `j6_file_sha` | `artifact` | not claimed by anyone; the leg publishes it |
| `j11_human_artifact_read` | `sealed_sha256`, `received_sha256` | not claimed by anyone; the leg publishes it |
| `j9_two_user_isolation` | **NOT added** | Laplace (`isolation-scope`) |

`TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict` therefore still fails until the j9 fixture
carries the two remaining keys its declaration requires: `contract_codes` (a list of canonical
codes) and `probes` (a `[]map[string]any` of per-resource `{resource, http, code}`). Validated:
with those two keys present the fixture classifies 11/11 completed and the test passes. That edit
is inside the region this file lists as Laplace's, so it is NOT made here - the director decides
whether Laplace's band or a follow-up carries it. Measured state at this band's tip:
`TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict` FAIL, every other leg of the lane PASS;
the failure is in a fixture, not in a verdict rule, and no expectation was changed to hide it.

### Two verdict readers, one policy each (no duplicated counter)
`scripts/skip-verdict.mjs` (Russell, DEF-GATE-SKIP-POLICY, tip `a6b0229f`) reads the **test
stream** and owns the skip policy: SKIP is not PASS, a PASS parent that measured nothing is a
FAIL. `tests/dote2e/cmd/dote2e-verdict` reads the **scenario artifact** and owns the acceptance
verdict of a run: `verdict=PASS` at the expected revision with legs, on a clean tree, with
distinct exit codes 0/4/5/3. Different objects, different counters, no shared code, no file
overlap. This band does not call his tool (it is not in the tree at this base) and does not
re-implement skip counting; `run-acceptance.sh` documents how to run under his policy by
wrapping the command with `node scripts/skip-verdict.mjs -- <cmd>`, which needs no change on
either side. One implementation of each policy, as required.
