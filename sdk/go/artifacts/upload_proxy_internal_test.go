package artifacts

// upload_proxy_internal_test.go -- an upload must never be routed by the environment.
//
// White-box on purpose. The property lives in the transport the SDK builds for itself, and an
// external test could only observe it by watching a request that happens to be proxied, which
// is the shape of evidence that goes green while nothing was tested.
//
// The assertion is deliberately about the Proxy field and not about http.ProxyFromEnvironment
// under a set HTTP_PROXY: net/http resolves the environment once through a sync.Once
// (transport.go, envProxyFunc), so an env-driven leg would depend on which leg in this test
// binary dialed first -- it could pass with the defect in place and fail with the fix. The env
// values below document the condition the policy is about; they are not read back.

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
)

const uploadProxyCanary = "http://upload-proxy.invalid:3128"

// newProxyLegClient builds a client whose owner transport satisfies the verified-mTLS check
// without anything being dialed: these legs read the upload transport, they do not use it.
func newProxyLegClient(t *testing.T, upload *http.Client) *Client {
	t.Helper()
	owner := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{{}}},
	}}
	c, err := New(Config{
		BaseURL:          "https://artifacts.internal",
		HTTPClient:       owner,
		UploadHTTPClient: upload,
		TokenSource:      func(context.Context) (string, error) { return "unused", nil },
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

func uploadTransportOf(t *testing.T, c *Client) *http.Transport {
	t.Helper()
	tr, ok := c.upload.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("upload transport is %T, want *http.Transport", c.upload.Transport)
	}
	return tr
}

// TestDefaultUploadTransportNeverInheritsAnEnvironmentProxy pins the decision: a presigned URL
// carries its signature in the query, so handing the request to a proxy from the environment
// sends an equivalent credential to a host the deployment never named. A nil Proxy policy is
// what "never" looks like on http.Transport.
// proxyLegRequest is what a transport's Proxy policy is actually handed:
// func(*http.Request) (*url.URL, error). The legs below used to pass a bare *url.URL, which
// is not the parameter type -- go vet (and any compile of the test binary) refused the
// package, so both legs pinned nothing at all while the file sat in the tree unread. A leg
// that cannot compile is worse than a missing leg, because it looks like coverage.
func proxyLegRequest() *http.Request {
	return &http.Request{Method: http.MethodPut, URL: &url.URL{Scheme: "https", Host: "blob.example.com:443"}}
}

func TestDefaultUploadTransportNeverInheritsAnEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", uploadProxyCanary)
	t.Setenv("HTTPS_PROXY", uploadProxyCanary)
	t.Setenv("http_proxy", uploadProxyCanary)
	t.Setenv("https_proxy", uploadProxyCanary)

	// Anti-vacuity: the policy the SDK used to inherit has to still be a policy. Should the
	// stdlib stop putting ProxyFromEnvironment on DefaultTransport, this leg would pin
	// nothing, and it has to say so rather than pass.
	def, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is %T, want *http.Transport: this leg pins nothing", http.DefaultTransport)
	}
	if inherited := def.Clone(); inherited.Proxy == nil {
		t.Fatal("http.DefaultTransport no longer carries an inherited proxy policy: this leg pins nothing")
	}

	c := newProxyLegClient(t, nil)
	if proxy := uploadTransportOf(t, c).Proxy; proxy != nil {
		got, err := proxy(proxyLegRequest())
		t.Fatalf("the default upload transport kept an inherited proxy policy (err %v, resolves to %v): a presigned "+
			"PUT carries its signature in the query, so that PUT would be handed to a host the deployment never chose", err, got)
	}
}

// TestExplicitUploadClientKeepsItsProxy is the other half of the decision: clearing the
// inherited policy must not turn into refusing to proxy at all. An explicit UploadHTTPClient is
// the only remaining way to route an upload through a proxy, and it has to survive the clone.
func TestExplicitUploadClientKeepsItsProxy(t *testing.T) {
	canary, err := url.Parse(uploadProxyCanary)
	if err != nil {
		t.Fatalf("canary: %v", err)
	}
	c := newProxyLegClient(t, &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(canary)}})
	proxy := uploadTransportOf(t, c).Proxy
	if proxy == nil {
		t.Fatal("an explicitly configured upload proxy was cleared: the SDK overrode a deployment's routing decision")
	}
	got, err := proxy(proxyLegRequest())
	if err != nil {
		t.Fatalf("configured proxy policy errored: %v", err)
	}
	if got == nil || got.String() != canary.String() {
		t.Fatalf("the upload resolves to %v, want the configured %s", got, canary)
	}
}
