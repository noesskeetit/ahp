package space

import (
	"net/http"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-H24-12. The two human artifact routes build their upstream call as
// <ArtifactsURL>/v1/artifacts/{Operation}. An origin configured as
// ".../29612/v1/artifacts" therefore asks for "/v1/artifacts/v1/artifacts/Resolve", the
// artifacts plane answers "unknown route", and the human surface reports NOT_FOUND - an
// operator's typo wearing the mask of "this artifact does not exist". The mount must
// refuse that configuration instead of serving a wrong answer to users.
func TestArtifactsProxiesRefuseAnOriginThatCarriesTheAPIPath(t *testing.T) {
	cases := []struct {
		name        string
		url         string
		wantErrCode string
	}{
		{"bare origin", "https://artifacts.internal:8083", ""},
		{"origin with trailing slash", "https://artifacts.internal:8083/", ""},
		{"origin carrying the api path", "https://artifacts.internal:8083/v1/artifacts", string(contracts.ErrInvalid)},
		{"origin carrying any other path", "https://artifacts.internal:8083/api", string(contracts.ErrInvalid)},
		{"relative url", "artifacts.internal:8083", string(contracts.ErrInvalid)},
		{"empty", "", string(contracts.ErrInvalid)},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/resolve", func(t *testing.T) {
			_, err := NewResolveProxy(ResolveProxyConfig{ArtifactsURL: tc.url, Client: http.DefaultClient})
			if tc.wantErrCode == "" {
				if err != nil {
					t.Fatalf("a usable origin must mount: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("configuration %q must be refused at construction", tc.url)
			}
			if got := contracts.Code(err); got != tc.wantErrCode {
				t.Fatalf("want %s, got %q (%v)", tc.wantErrCode, got, err)
			}
		})
		t.Run(tc.name+"/download", func(t *testing.T) {
			_, err := NewDownloadProxy(DownloadProxyConfig{ArtifactsURL: tc.url, Client: http.DefaultClient})
			if tc.wantErrCode == "" {
				if err != nil {
					t.Fatalf("a usable origin must mount: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("configuration %q must be refused at construction", tc.url)
			}
			if got := contracts.Code(err); got != tc.wantErrCode {
				t.Fatalf("want %s, got %q (%v)", tc.wantErrCode, got, err)
			}
		})
	}
}
