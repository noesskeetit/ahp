# Candidate build (reproducible, real provenance)

Entry: `scripts/build-candidate.sh` (build host; `--linux` adds linux/arm64; `--images`
also builds the AHP + runner candidate images from the SAME stamped linux binaries).
Manifest: `build-out/candidate-manifest.json` (also committed copy: this dir).

## Why a standalone clone (the provenance trap)
`go build` VCS-stamps only when the module `.git` is a real DIRECTORY. In a nested
worktree `.git` is a FILE, so Go walks up and stamps the enclosing CONTROL repo
(company-brain, e.g. `8e4d8766`) or omits the stamp -> `vcs.modified=true`. The stand's
own `private/composed-backend/scripts/provenance-build-runner.sh` documents the same and
always builds in a full clone. This script clones `--depth 1` of the exact revision, so
`vcs.revision==HEAD` and `vcs.modified=false` are real (verified in the manifest). The
same `internal/buildinfo.{Revision,CommitTime,Modified}` ldflags the stand uses are also
injected, but those are additive runtime reporting, not the proof.

## Verified candidate (this run)
- build HEAD `587373931faab55849a1e40fc483990824d41b67` (base 7a3a7a22 + the two build scripts)
- 10 host binaries (darwin/arm64) + 10 linux binaries (linux/arm64, CGO_ENABLED=0), tags `ahp_data,ahp_exec`
- every binary built TWICE in fresh caches, byte-identical; all `vcs.revision==HEAD`, `vcs.modified=false`
- images: `ahp-candidate-ahp` (FROM alpine + prebuilt stamped linux ahp) and
  `ahp-candidate-runner` (`infra/agent-workload/Dockerfile`, FROM scratch + prebuilt stamped linux ahp-agent).
  Image ids are LOCAL containerd store digests; NOT pushed to a registry.

## Existing image recipes (inventory, not invented)
- `infra/Dockerfile.ahp` builds ONLY `cmd/ahp` and stamps via `-ldflags -X main.version` -> does NOT satisfy acceptance-run.md section 0 (ldflags is not provenance, and no other cmd is built). Kept as-is; candidate AHP image instead COPYs a prebuilt VCS-stamped linux binary (minimal recipe in the script).
- `infra/agent-workload/Dockerfile` = FROM scratch + `COPY ahp-agent` -> provenance comes from the binary; used directly.
- `infra/local-k8s/runtime.Dockerfile`, `infra/minio.Dockerfile`, `sdk/python/Dockerfile` = runtime/sidecar, not the AHP/runner product images.
- The live stand is NOT run from these Docker images: it runs macOS launchd jobs over `composed-backend/bin/*` binaries (see `runner-stand-launch.sh`).

## Flap checklist: make BOTH live binaries vcs.revision == one HEAD (DRY-RUN; no restarts performed)
Live pair today (verified read-only 2026-11-09): front = `com.ahp.delivery.composed-backend`
-> child `bin/ahp-full` (vcs `be0cafb4`, modified=false, darwin/arm64, PID 46371, :29612);
runner = `com.ahp.delivery.runner-stand` -> `bin/as-runner` (vcs `6fd2d0b6`, modified=false, PID 46295, :29690).
Both read their binary through a symlink, so a flap = rebuild + repoint symlink + relaunch:
1. Choose ONE unified revision R (the accepted integration commit; must be identical for both).
2. Build both in a standalone clone at R (this script), host targets:
   `scripts/build-candidate.sh --revision R --out <dir>` -> `build-out/host/ahp`, `build-out/host/as-runner`.
   Gate: `go version -m build-out/host/{ahp,as-runner}` show `vcs.revision==R`, `vcs.modified=false`.
3. Stage with the stand's naming (`<name>-<R first12>-t`) under `private/composed-backend/bin/`:
   `cp build-out/host/ahp .../bin/ahp-full-<R12>-t` ; `cp build-out/host/as-runner .../bin/as-runner-<R12>-t`.
4. Runner launcher PINS a SHA: `runner-stand-launch.sh` `EXPECTED_SHA=<sha256 of as-runner-<R12>-t>`
   (it currently pins `8954125d...` = old runner). Update it or the launcher refuses (exit 3).
5. Repoint symlinks atomically: `ln -sfn ahp-full-<R12>-t bin/ahp-full` ; `ln -sfn as-runner-<R12>-t bin/as-runner`.
6. Relaunch via launchd (DIRECTOR/root only; NOT done here):
   `launchctl kickstart -k gui/501/com.ahp.delivery.composed-backend`
   `launchctl kickstart -k gui/501/com.ahp.delivery.runner-stand`
7. Verify per section 0: `go version -m "$(lsof -p <newPID> -a -d txt -Fn | awk '/^n/{sub(/^n/,"");print;exit}')"`
   -> `vcs.revision==R` and `vcs.modified=false` for BOTH.

## Needs a director decision
- The single unified revision R (front and runner must equal it). Integration branch tip, not this lane.
- Whether to push candidate images to a registry to obtain real registry digests (none pushed here).
- Whether the front is built by a `provenance-build.sh` symmetric to the runner's (compose stand); if so, reuse it instead of raw `build-out/host/ahp` for identical naming/guards.
