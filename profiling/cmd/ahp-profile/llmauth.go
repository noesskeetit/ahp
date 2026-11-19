package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/profiling/harness"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The LLM phase authorizes REAL model calls through the product boundary:
// Human login -> /v1/identity/control/exchange -> scoped task token.
// Only the human ACCOUNT itself needs the documented operator seam
// (identity.SeedBrowserUser), exactly the way the acceptance stand provisions
// its user; every credential afterwards is minted by the product.

// seedProfilerUser provisions the human account the exchange path will use.
// The password is random, lives only for this process, and is never logged.
// perms are the capabilities the account must personally hold: the exchange can
// only mint a token carrying capabilities the human holds, so a state run passes
// ["space:access","state"] and a model run passes ["space:access","model"].
func seedProfilerUser(ctx context.Context, dsn, tenant, subject, username string, perms []string) (string, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	store := identity.New(pool)
	if err := store.Migrate(ctx); err != nil {
		return "", fmt.Errorf("identity migrate: %w", err)
	}
	pw := make([]byte, 24)
	if _, err := rand.Read(pw); err != nil {
		return "", err
	}
	password := "Pw!" + base64.RawURLEncoding.EncodeToString(pw)
	user := identity.BrowserUser{UserID: "prof-" + subject, Tenant: tenant, Subject: subject, DisplayName: "profiler", Email: "profiler@example.invalid"}
	if err := store.SeedBrowserUser(ctx, user, username, password, []string{"profiler"}, perms); err != nil {
		return "", fmt.Errorf("seed browser user: %w", err)
	}
	return password, nil
}

// browserClient is an mTLS HTTP client with manual cookie handling.
func browserClient(cm harness.CertMaterial, cc harness.ClientCertFile, port int) (*http.Client, error) {
	caPem, err := os.ReadFile(cm.CACert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("ca")
	}
	cert, err := tls.LoadX509KeyPair(cc.Cert, cc.Key)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "localhost", MinVersion: tls.VersionTLS12}},
		Timeout:   30 * time.Second,
	}, nil
}

func postJSON(ctx context.Context, hc *http.Client, url string, body any, headers map[string]string, cookie string) (int, []byte, string, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, "", err
	}
	req.Header.Set("content-type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if cookie != "" {
		req.Header.Set("cookie", cookie)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	newCookie := ""
	if sc := resp.Cookies(); len(sc) > 0 {
		newCookie = sc[0].Name + "=" + sc[0].Value
	}
	return resp.StatusCode, out, newCookie, nil
}

// scopedTaskToken runs the canonical Human -> authorized exchanger -> scoped
// task transition and returns the token plus the task it is really bound to.
func scopedTaskToken(ctx context.Context, port int, cm harness.CertMaterial, cc harness.ClientCertFile, username, password, task string, capabilities []string) (string, string, error) {
	hc, err := browserClient(cm, cc, port)
	if err != nil {
		return "", "", err
	}
	base := fmt.Sprintf("https://127.0.0.1:%d", port)
	status, body, cookie, err := postJSON(ctx, hc, base+"/u-ahp/v1/auth/login", map[string]any{"username": username, "password": password}, map[string]string{"x-ahp-session": "1"}, "")
	if err != nil {
		return "", "", err
	}
	if status != http.StatusOK {
		return "", "", authFailure("login", status, body)
	}
	status, body, _, err = postJSON(ctx, hc, base+"/u-ahp/v1/auth/token", map[string]any{}, map[string]string{"x-ahp-session": "1"}, cookie)
	if err != nil {
		return "", "", err
	}
	if status != http.StatusOK {
		return "", "", fmt.Errorf("human token http %d (%d bytes)", status, len(body))
	}
	var tokResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokResp); err != nil || tokResp.AccessToken == "" {
		return "", "", fmt.Errorf("human token parse failed (body %d bytes)", len(body))
	}
	status, body, _, err = postJSON(ctx, hc, base+"/v1/identity/control/exchange", map[string]any{"human_token": tokResp.AccessToken, "task": task, "capabilities": capabilities}, nil, cookie)
	if err != nil {
		return "", "", err
	}
	if status != http.StatusOK {
		return "", "", authFailure("exchange", status, body)
	}
	var ex struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	if err := json.Unmarshal(body, &ex); err != nil || ex.Token == "" {
		return "", "", fmt.Errorf("exchange parse failed (body %d bytes)", len(body))
	}
	return ex.Token, ex.Task, nil
}

// authFailure renders a product authorization error WITHOUT ever echoing the
// response body. The body can contain the very bearer it rejected (some handlers
// echo the request), and this text reaches log.Fatalf and the tailed platform log
// -> an artifact. We surface only the operation, the HTTP status, the product's
// machine-readable code (a short token from a known field, if present) and the
// byte length. A diagnostic that leaks a credential is not a diagnostic.
func authFailure(op string, status int, body []byte) error {
	code := ""
	var e struct {
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil {
		code = e.Code
		if code == "" {
			code = e.Error.Code
		}
	}
	// The code is a bounded vocabulary, but never trust a peer: cap it so a
	// hostile/garbage "code" cannot smuggle a long payload into the log either.
	if len(code) > 64 {
		code = code[:64]
	}
	if code != "" {
		return fmt.Errorf("%s failed: http %d code=%s (%d bytes)", op, status, code, len(body))
	}
	return fmt.Errorf("%s failed: http %d (%d bytes)", op, status, len(body))
}
