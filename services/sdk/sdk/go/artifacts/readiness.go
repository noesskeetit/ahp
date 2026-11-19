package artifacts

import "context"

// Derivation is an owner-reported derived representation, not a client assertion.
type Derivation struct {
	Kind   string `json:"kind"`
	Status string `json:"status"`
	SHA256 string `json:"sha256,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// Readiness keeps immutable bytes, extracted text and indexed search distinct.
// A sealed binary object can have ContentReady=true while Ready=false.
type Readiness struct {
	Sealed           bool         `json:"sealed"`
	ContentReady     bool         `json:"content_ready"`
	ExtractionReady  bool         `json:"extraction_ready"`
	SearchReady      bool         `json:"search_ready"`
	Quarantined      bool         `json:"quarantined"`
	ExtractionStatus string       `json:"extraction_status"`
	SearchStatus     string       `json:"search_status"`
	SearchDetail     string       `json:"search_detail,omitempty"`
	Detail           string       `json:"detail,omitempty"`
	Ready            bool         `json:"ready"`
	Derivations      []Derivation `json:"derivations"`
}

// TextChunk is an exact-version, byte-budgeted owner extraction slice.
// NextOffset and EOF, not len(Text) alone, drive continuation.
type TextChunk struct {
	ID         string `json:"id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	Text       string `json:"text"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	TotalBytes int64  `json:"total_bytes"`
	EOF        bool   `json:"eof"`
}

// TextSearchResult reports a match in one exact version's owner-maintained index.
type TextSearchResult struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Matched bool   `json:"matched"`
}

// ResolvedVersion contains only the readable exact-version download descriptor.
// The owner's field is artifact_id, unlike ArtifactRef's id.
type ResolvedVersion struct {
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	Name       string `json:"name"`
	MIME       string `json:"mime"`
}

// ResolveResult contains versions admitted under current read authority.
type ResolveResult struct {
	SHA256   string            `json:"sha256"`
	Versions []ResolvedVersion `json:"versions"`
}

// Readiness checks current read authority before reporting owner readiness.
func (c *Client) Readiness(ctx context.Context, id string) (Readiness, error) {
	var out Readiness
	err := c.call(ctx, "Readiness", map[string]string{"id": id}, &out, false)
	return out, err
}

// Process requests sandboxed extraction/indexing under current derive authority.
// It grants no manager authority; the owner must have a parser configured.
// No retry is automatic. Reconcile a lost reply with Readiness for the same ID.
func (c *Client) Process(ctx context.Context, id string) (Readiness, error) {
	var out Readiness
	err := c.call(ctx, "Process", map[string]string{"id": id}, &out, true)
	return out, err
}

// ReadTextChunk reads owner-extracted UTF-8 for an explicit immutable version.
// Limit and contextBudget are byte bounds; owner validates them (max32KiB).
func (c *Client) ReadTextChunk(ctx context.Context, id, version string, offset int64, limit, contextBudget int) (TextChunk, error) {
	var out TextChunk
	in := struct {
		ID            string `json:"id"`
		Version       string `json:"version"`
		Offset        int64  `json:"offset"`
		Limit         int    `json:"limit"`
		ContextBudget int    `json:"context_budget"`
	}{id, version, offset, limit, contextBudget}
	err := c.call(ctx, "ReadTextChunk", in, &out, false)
	return out, err
}

// SearchText queries one version's index, never a database or provider directly.
func (c *Client) SearchText(ctx context.Context, id, version, query string) (TextSearchResult, error) {
	var out TextSearchResult
	err := c.call(ctx, "SearchText", map[string]string{"id": id, "version": version, "query": query}, &out, false)
	return out, err
}

// Resolve maps a digest to currently readable versions in the caller's scope.
// It does not issue a download grant or expand the task's authority.
func (c *Client) Resolve(ctx context.Context, sha256 string) (ResolveResult, error) {
	var out ResolveResult
	err := c.call(ctx, "Resolve", map[string]string{"sha256": sha256}, &out, false)
	return out, err
}
