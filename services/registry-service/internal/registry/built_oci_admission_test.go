package registry_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type ociDescriptor struct {
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	MediaType string `json:"mediaType"`
}

func builtOCI(t *testing.T) (image, digest string, sbom []byte) {
	t.Helper()
	archive := os.Getenv("AHP_TEST_BUILT_OCI_ARCHIVE")
	if archive == "" {
		t.Skip("requires an explicitly built product OCI archive, not a fabricated digest")
	}
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr := tar.NewReader(f)
	blobs := map[string][]byte{}
	var index []byte
	var total int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg || (h.Name != "index.json" && !strings.HasPrefix(h.Name, "blobs/sha256/")) {
			continue
		}
		total += h.Size
		if h.Size < 0 || h.Size > 32<<20 || total > 128<<20 {
			t.Fatal("OCI fixture exceeds the bounded archive profile")
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "index.json" {
			index = b
		} else {
			sum := sha256.Sum256(b)
			if hex.EncodeToString(sum[:]) != strings.TrimPrefix(h.Name, "blobs/sha256/") {
				t.Fatal("OCI blob name does not match its actual bytes")
			}
			blobs["sha256:"+hex.EncodeToString(sum[:])] = b
		}
	}
	read := func(d ociDescriptor) []byte {
		b, ok := blobs[d.Digest]
		if !ok || int64(len(b)) != d.Size {
			t.Fatal("OCI descriptor points to absent or wrongly sized bytes")
		}
		return b
	}
	var root struct {
		Manifests []ociDescriptor `json:"manifests"`
	}
	if err := json.Unmarshal(index, &root); err != nil || len(root.Manifests) != 1 {
		t.Fatal("expected one exported OCI image")
	}
	desc := root.Manifests[0]
	var manifest struct {
		Manifests []ociDescriptor `json:"manifests"`
		Config    ociDescriptor   `json:"config"`
		Layers    []ociDescriptor `json:"layers"`
	}
	for depth := 0; depth < 5; depth++ {
		if err := json.Unmarshal(read(desc), &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Config.Digest != "" {
			break
		}
		if len(manifest.Manifests) != 1 {
			t.Fatal("ambiguous OCI image index")
		}
		desc = manifest.Manifests[0]
		manifest.Manifests = nil
	}
	var config struct {
		OS, Architecture string
		Config           struct {
			User   string
			Labels map[string]string
		}
	}
	if err := json.Unmarshal(read(manifest.Config), &config); err != nil || config.OS != "linux" || config.Architecture != "arm64" || config.Config.User != "65532:65532" {
		t.Fatal("not the expected non-root Linux product image")
	}
	if config.Config.Labels["com.docker.compose.project"] != "ahp-handbook" || config.Config.Labels["org.opencontainers.image.revision"] == "" {
		t.Fatal("image has no owned source provenance")
	}
	var binary []byte
	for _, layer := range manifest.Layers {
		b := read(layer)
		var stream io.Reader = bytes.NewReader(b)
		if strings.HasSuffix(layer.MediaType, "+gzip") {
			gz, err := gzip.NewReader(stream)
			if err != nil {
				t.Fatal(err)
			}
			defer gz.Close()
			stream = gz
		}
		files := tar.NewReader(stream)
		for {
			h, err := files.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimPrefix(h.Name, "./") == "ahp-agent" {
				if h.Typeflag != tar.TypeReg || h.Size > 32<<20 {
					t.Fatal("invalid product executable")
				}
				binary, err = io.ReadAll(files)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	imageELF, err := elf.NewFile(bytes.NewReader(binary))
	if err != nil {
		t.Fatal("scratch image agent is not a Linux ELF executable")
	}
	for _, program := range imageELF.Progs {
		if program.Type == elf.PT_INTERP {
			t.Fatal("scratch image agent requires a dynamic loader absent from its root filesystem; rebuild with CGO_ENABLED=0")
		}
	}
	info, err := buildinfo.Read(bytes.NewReader(binary))
	if err != nil || info.Path != "github.com/ahp-platform/ahp/cmd/ahp-agent" {
		t.Fatal("OCI image does not contain the real AHP agent executable")
	}
	var revision string
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			revision = setting.Value
		}
		if setting.Key == "vcs.modified" && setting.Value != "false" {
			t.Fatal("image was built from dirty source")
		}
	}
	if revision == "" || revision != config.Config.Labels["org.opencontainers.image.revision"] {
		t.Fatal("binary and OCI image source revisions differ")
	}
	components := []map[string]any{{"type": "container", "name": "ahp-agent", "version": revision,
		"hashes": []map[string]string{{"alg": "SHA-256", "content": strings.TrimPrefix(desc.Digest, "sha256:")}}}}
	for _, dependency := range info.Deps {
		components = append(components, map[string]any{"type": "library", "name": dependency.Path, "version": dependency.Version})
	}
	sbom, err = json.Marshal(map[string]any{"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1, "components": components})
	if err != nil || len(components) < 2 {
		t.Fatal("could not derive the Go dependency inventory from the built executable")
	}
	t.Logf("actual OCI manifest %s, binary source %s, dependency inventory %d components; not a vulnerability clearance", desc.Digest, revision, len(components))
	return "registry.local/ahp/agent@" + desc.Digest, strings.TrimPrefix(desc.Digest, "sha256:"), sbom
}

func TestAdmissionOfActualBuiltOCIImage(t *testing.T) {
	image, digest, sbom := builtOCI(t)
	e := newSupplyEnv(t)
	m := manifest(t, "built-oci."+randSuffix(t))
	m.Components[0].Image = image
	agent := "built-oci-" + randSuffix(t)
	v := publish(t, e.store, agent, m)
	sum := sha256.Sum256(sbom)
	im := imageEnv{agent: agent, version: v, imageRef: image, digestHex: digest,
		sbom: sbom, sbomDigest: "sha256:" + hex.EncodeToString(sum[:])}
	if _, err := e.store.Install(context.Background(), e.tenant, agent, v.ID, registry.EnvironmentDev,
		"unsigned-"+randSuffix(t), v.Manifest.Capabilities); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("unsigned real OCI image was admitted: %v", err)
	}
	e.admit(t, im)
	e.install(t, im, registry.EnvironmentDev)
	att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{AgentID: agent, VersionID: v.ID,
		Image: image, Attestation: att, SBOM: append(append([]byte{}, sbom...), ' ')}); err == nil {
		t.Fatal("modified dependency inventory was accepted under the original signature")
	}
	if directory := os.Getenv("AHP_OCI_EVIDENCE_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "agent.cdx.json"), sbom, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("real OCI bytes + native Go inventory + current DSSE/PKI + PG dev admission verified; evaluation authority is a fixture, not LLM E2E/security clearance/registry push/K8s rollout")
}
