package secretstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ErrCAS signals a conflicting KV version; a rotated token must never overwrite it.
var ErrSecretNotFound = errors.New("secretstore: secret not found")

var ErrCAS = errors.New("secretstore: compare-and-set rejected")

// ReadVersion reads secret data and its CAS version from Vault KV v2.
func (c *Client) ReadVersion(ctx context.Context, path string) (map[string]any, int, error) {
	b, err := c.kvRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, err
	}
	var v struct {
		Data struct {
			Data     map[string]any `json:"data"`
			Metadata struct {
				Version int `json:"version"`
			} `json:"metadata"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &v) != nil || v.Data.Data == nil || v.Data.Metadata.Version < 1 {
		return nil, 0, errors.New("secretstore: malformed versioned secret")
	}
	return v.Data.Data, v.Data.Metadata.Version, nil
}

// WriteCAS writes a KV v2 secret only at expectedVersion (zero means create-only).
func (c *Client) WriteCAS(ctx context.Context, path string, data map[string]any, expectedVersion int) (int, error) {
	if expectedVersion < 0 {
		return 0, ErrCAS
	}
	body, err := json.Marshal(map[string]any{"data": data, "options": map[string]int{"cas": expectedVersion}})
	if err != nil {
		return 0, errors.New("secretstore: invalid secret data")
	}
	b, err := c.kvRequest(ctx, http.MethodPost, path, body)
	if err != nil {
		return 0, err
	}
	var v struct {
		Data struct {
			Version int `json:"version"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &v) != nil || v.Data.Version != expectedVersion+1 {
		return 0, errors.New("secretstore: invalid written version")
	}
	return v.Data.Version, nil
}

// DeleteMetadata permanently removes all versions, including soft-deleted tokens.
func (c *Client) DeleteMetadata(ctx context.Context, dataPath string) error {
	if !strings.Contains(dataPath, "/data/") {
		return errors.New("secretstore: KV v2 data path required")
	}
	_, err := c.kvRequest(ctx, http.MethodDelete, strings.Replace(dataPath, "/data/", "/metadata/", 1), nil)
	return err
}

func (c *Client) kvRequest(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	// Paths come from service configuration, never a provider/user response.
	if path == "" || strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#\\") {
		return nil, errors.New("secretstore: invalid KV path")
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return nil, errors.New("secretstore: invalid KV path")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.addr+"/v1/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("secretstore: invalid request")
	}
	req.Header.Set("X-Vault-Token", c.token)
	req.Header.Set("Content-Type", "application/json")
	client := *c.http
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("secretstore: Vault unavailable")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return nil, errors.New("secretstore: invalid Vault response")
	}
	if method == http.MethodDelete && resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrSecretNotFound
	}
	if method == http.MethodPost && resp.StatusCode == http.StatusBadRequest {
		return nil, ErrCAS
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("secretstore: Vault status %d", resp.StatusCode)
	}
	return b, nil
}
