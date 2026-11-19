package model

// The artifact-plane legs in this package write to a real object store. Two
// rules, both learned the hard way on this Mac:
//
//  1. A missing prerequisite is a SKIP with a machine-readable reason, never a
//     red default suite. A suite that reddens because the environment has no
//     MinIO says nothing about the product, and the wide gate becomes unreadable
//     (this is the failure mode from the night of 09.11). Equally, a silent skip
//     is a lie by omission: the reason carries a tag and says that a skip is not
//     coverage (precedent D-REG-3 in internal/registry).
//
//  2. An endpoint that happens to be in the environment is not a permission to
//     write. One MinIO (127.0.0.1:29000) serves every lane here -- audited at
//     1768 buckets, with the shared ahp-root-check-20261102 holding objects from
//     several harnesses -- so the lane has to NAME its store via
//     AHP_TEST_OBJECT_STORE=<host:port> before any client is constructed.

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
)

// artifactPlaneSkipReason returns "" when the leg may run, and otherwise the
// reason it may not.
func artifactPlaneSkipReason() string {
	var missing []string
	for _, k := range []string{"AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_env_absent missing=" + strings.Join(missing, ",") +
			" -- this leg measures the real artifact plane; it is evidence only when it RAN, " +
			"and a skip here means not-covered, not passed"
	}
	return laneObjectStoreSkipReason(os.Getenv("AHP_S3_ENDPOINT"))
}

func laneObjectStoreSkipReason(endpoint string) string {
	have, err := normalizeObjectStoreEndpoint(endpoint)
	if err != nil {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_endpoint_unparsable -- AHP_S3_ENDPOINT is not a host:port; refusing to guess which instance to write to"
	}
	designated := strings.TrimSpace(os.Getenv("AHP_TEST_OBJECT_STORE"))
	if designated == "" {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_not_lane_designated endpoint=" + have +
			" -- an inherited AHP_S3_ENDPOINT is not a write permission: this Mac shares one MinIO across lanes. " +
			"Set AHP_TEST_OBJECT_STORE=<host:port> for the instance this lane owns. A skip here means not-covered, not passed"
	}
	want, err := normalizeObjectStoreEndpoint(designated)
	if err != nil {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_designation_unparsable -- AHP_TEST_OBJECT_STORE is not a host:port, so the lane's own designation cannot be honoured"
	}
	if have != want {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_identity_mismatch endpoint=" + have +
			" designated=" + want + " -- refusing to write to an object store the lane was not given. A skip here means not-covered, not passed"
	}
	return ""
}

func normalizeObjectStoreEndpoint(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("empty endpoint")
	}
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" {
		return "", errors.New("unparsable endpoint")
	}
	if u.User != nil {
		return "", errors.New("endpoint must not carry credentials")
	}
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return host + ":" + port, nil
}

func TestArtifactPlaneGateRefusesAnObjectStoreTheLaneWasNotGiven(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:29000")
	t.Setenv("AHP_MINIO_ROOT_USER", "lane-access-key")
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "lane-secret-key")

	t.Setenv("AHP_TEST_OBJECT_STORE", "")
	reason := artifactPlaneSkipReason()
	if !strings.Contains(reason, "SKIP_ENV[tag=artifact_plane]") ||
		!strings.Contains(reason, "reason=object_store_not_lane_designated") ||
		!strings.Contains(reason, "endpoint=127.0.0.1:29000") {
		t.Fatalf("an inherited endpoint without a lane designation must skip by name, got %q", reason)
	}
	if strings.Contains(reason, "lane-access-key") || strings.Contains(reason, "lane-secret-key") {
		t.Fatalf("the skip reason carries credentials: %q", reason)
	}

	t.Setenv("AHP_TEST_OBJECT_STORE", "127.0.0.1:29000")
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Fatalf("the designated store must be accepted: %q", reason)
	}

	t.Setenv("AHP_TEST_OBJECT_STORE", "127.0.0.1:39000")
	reason = artifactPlaneSkipReason()
	if !strings.Contains(reason, "reason=object_store_identity_mismatch") ||
		!strings.Contains(reason, "endpoint=127.0.0.1:29000") ||
		!strings.Contains(reason, "designated=127.0.0.1:39000") {
		t.Fatalf("a mismatching designation must refuse and name both identities, got %q", reason)
	}

	t.Setenv("AHP_TEST_OBJECT_STORE", "http://user:pass@127.0.0.1:29000")
	if reason := artifactPlaneSkipReason(); !strings.Contains(reason, "reason=object_store_designation_unparsable") {
		t.Fatalf("a credential-bearing designation must be refused, got %q", reason)
	}

	t.Setenv("AHP_S3_ENDPOINT", "")
	if reason := artifactPlaneSkipReason(); !strings.Contains(reason, "reason=object_store_env_absent") {
		t.Fatalf("a missing prerequisite must be named as such, got %q", reason)
	}
}
