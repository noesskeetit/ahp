package harness

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// ContainerImageDigests records what container images the measured stack
// actually ran on, resolved from the live containers (name -> sha256 digest).
// A profile without image pins is anonymous: the same binary against a
// different PG/Redis/MinIO build is a different experiment.
func ContainerImageDigests(containers []string) map[string]string {
	out := map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, c := range containers {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		img, err := runOut(ctx, "docker", "inspect", "-f", "{{.Config.Image}}", c)
		if err != nil || img == "" {
			continue
		}
		dg, err := runOut(ctx, "docker", "image", "inspect", "-f", "{{.RepoDigests}}", img)
		if err != nil {
			continue
		}
		digest := firstDigest(dg)
		if digest == "" {
			continue
		}
		out[c+" ("+img+")"] = digest
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// firstDigest picks the first sha256 from docker's "[repo@sha256:...]" list.
func firstDigest(raw string) string {
	raw = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(raw), "["), "]")
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool { return r == ' ' || r == ',' }) {
		item = strings.Trim(item, "\"'[]")
		if i := strings.Index(item, "@"); i >= 0 {
			return item[i+1:]
		}
	}
	return ""
}

// ProductDeltaOutsideProfiling counts files differing between a declared
// product revision and the current tree, excluding profiling/ (the harness
// owner's range). 0 means the measured PRODUCT code is exactly that revision.
func ProductDeltaOutsideProfiling(repo, productRev string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := runOut(ctx, "git", "-C", repo, "diff", "--name-only", productRev+"..HEAD", "--", ".", ":(exclude)profiling")
	if err != nil {
		return -1
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return 0
	}
	return len(strings.Split(out, "\n"))
}

func runOut(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	b, err := cmd.Output()
	return strings.TrimSpace(string(b)), err
}
