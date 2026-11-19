package model

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// HTTPFileConfig describes an explicitly enabled OpenAI-compatible Files API.
// APIKey is resolved from Vault by composition, never accepted from task JSON.
type HTTPFileConfig struct {
	BaseURL string
	Account string
	APIKey  string
	Egress  netpolicy.Policy
}

type HTTPFileProvider struct {
	base, key, identity string
	client              *http.Client
}

func NewHTTPFileProvider(cfg HTTPFileConfig) (*HTTPFileProvider, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || cfg.APIKey == "" || cfg.Account == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "model files: explicit provider endpoint, account and secret required")
	}
	cfg.Egress.AllowedMethods = []string{http.MethodPost, http.MethodDelete}
	cfg.Egress.AllowedPathPrefixes = []string{strings.TrimRight(u.Path, "/") + "/files"}
	cfg.Egress.MaxResponseBytes = 64 << 10
	if cfg.Egress.Timeout == 0 {
		cfg.Egress.Timeout = 30 * time.Second
	}
	if cfg.Egress.Timeout > time.Minute {
		return nil, contracts.NewError(contracts.ErrInvalid, "model files: provider timeout exceeds bound")
	}
	c, err := netpolicy.NewClient(cfg.Egress)
	if err != nil {
		return nil, err
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	id, _ := contracts.Digest([]string{"files-v1", strings.TrimRight(cfg.BaseURL, "/"), cfg.Account})
	return &HTTPFileProvider{base: strings.TrimRight(cfg.BaseURL, "/"), key: cfg.APIKey, identity: id, client: c}, nil
}
func (p *HTTPFileProvider) Identity() string { return p.identity }

// Upload uses an opaque platform filename; original user filenames are not
// leaked in provider metadata. The common driver enables only user_data.
func (p *HTTPFileProvider) Upload(ctx context.Context, name, mime string, data []byte) (string, error) {
	if len(data) > FileMaxBytes || !remoteFileID.MatchString(name) {
		return "", contracts.NewError(contracts.ErrInvalid, "model files: upload outside bounds")
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("purpose", "user_data"); err != nil {
		return "", err
	}
	part, err := w.CreateFormFile("file", name)
	if err != nil {
		return "", err
	}
	if _, err = part.Write(data); err != nil {
		return "", err
	}
	if err = w.Close(); err != nil {
		return "", err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+"/files", &body)
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "model files: invalid upload request")
	}
	r.Header.Set("Authorization", "Bearer "+p.key)
	r.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := p.client.Do(r)
	if err != nil {
		return "", contracts.NewError(contracts.ErrUnavailable, "model files: upload transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", contracts.NewError(contracts.ErrUnavailable, "model files: upload not acknowledged")
	}
	var result struct {
		ID    string `json:"id"`
		Bytes *int64 `json:"bytes"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result) != nil || !remoteFileID.MatchString(result.ID) || (result.Bytes != nil && *result.Bytes != int64(len(data))) {
		return "", contracts.NewError(contracts.ErrUnavailable, "model files: invalid upload receipt")
	}
	return result.ID, nil
}
func (p *HTTPFileProvider) Delete(ctx context.Context, id string) error {
	if !remoteFileID.MatchString(id) {
		return contracts.NewError(contracts.ErrInvalid, "model files: invalid provider ID")
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodDelete, p.base+"/files/"+id, nil)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "model files: invalid delete request")
	}
	r.Header.Set("Authorization", "Bearer "+p.key)
	resp, err := p.client.Do(r)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "model files: delete transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return contracts.NewError(contracts.ErrUnavailable, "model files: delete not acknowledged")
	}
	var result struct {
		ID      string `json:"id"`
		Deleted bool   `json:"deleted"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result) != nil || !result.Deleted || result.ID != id {
		return contracts.NewError(contracts.ErrUnavailable, "model files: invalid delete receipt")
	}
	return nil
}
