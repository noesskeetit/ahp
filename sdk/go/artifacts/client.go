// Package artifacts implements the task-authenticated Artifacts owner API.
package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// MaxSize is the owner's maximum artifact size, in bytes.
const MaxSize int64 = 64 << 20

// TokenSource obtains current task authority for every owner API call.
type TokenSource func(context.Context) (string, error)

// Config separates the authenticated owner connection from presigned blob PUTs.
type Config struct {
	BaseURL     string
	HTTPClient  *http.Client
	TokenSource TokenSource
	// UploadHTTPClient is optional; its transport is cloned with client credentials
	// removed. Uploads never send the task bearer or follow a redirect.
	UploadHTTPClient *http.Client
	// RequestDecorator, when set, is called on every outgoing request before it is
	// sent, so the caller can attach cross-cutting request metadata (the W3C trace
	// context, for example) without this SDK owning that dependency. It cannot weaken
	// the two guarantees this package exists for: the owner API sets Authorization
	// AFTER the decorator has run, so a decorator can neither substitute nor drop the
	// task authority the call is made with; and a presigned upload that ends up
	// carrying any credential header is refused before it leaves the process. A third
	// guarantee is checked rather than ordered: the decorator receives a mutable
	// *http.Request and the transport dials req.URL, so both paths compare the
	// destination after the decorator has run and refuse a moved request.
	RequestDecorator func(*http.Request)
}

// ArtifactRef is the immutable object and its server-attested ownership.
type ArtifactRef struct {
	ID              string     `json:"id"`
	Tenant          string     `json:"tenant"`
	Deployment      string     `json:"deployment"`
	Subject         string     `json:"subject"`
	ContextID       string     `json:"context_id,omitempty"`
	TaskID          string     `json:"task_id,omitempty"`
	Version         string     `json:"version"`
	SHA256          string     `json:"sha256"`
	Size            int64      `json:"size"`
	Name            string     `json:"name"`
	MIME            string     `json:"mime"`
	Labels          labels.Set `json:"labels"`
	EffectiveLabels labels.Set `json:"effective_labels"`
}

// UploadRequest declares the intended bytes; scope and binding come from the token.
type UploadRequest struct {
	Key          string     `json:"key"`
	Name         string     `json:"name"`
	MIME         string     `json:"mime"`
	SHA256       string     `json:"sha256"`
	Size         int64      `json:"size"`
	Labels       labels.Set `json:"labels"`
	JournalStage bool       `json:"journal_stage,omitempty"`
}

// Upload is an owner-issued, short-lived capability for one blob PUT.
type Upload struct {
	Headers      map[string]string `json:"headers,omitempty"`
	ExpiresAt    time.Time         `json:"expires_at,omitempty"`
	ID           string            `json:"id"`
	PresignedURL string            `json:"presigned_url"`
	Object       string            `json:"object"`
	ExpectedSHA  string            `json:"sha256"`
	ExpectedSize int64             `json:"size"`
}

// PutRequest supplies bytes to BeginUpload, PUT and Seal in order.
type PutRequest struct {
	Key, Name, MIME string
	Data            []byte
	Labels          labels.Set
	JournalStage    bool
}

// Client is safe for concurrent tasks. It does not automatically retry calls.
type Client struct {
	base string
	// baseScheme/baseHost are the owner authority parsed ONCE here rather than on
	// every call. A request decorator is a caller hook that receives a mutable
	// *http.Request, so it can rewrite req.URL -- and the transport dials req.URL,
	// not the Host header. Both request paths have to prove the destination survived
	// decoration, which needs the authority the client was built for.
	baseScheme, baseHost string
	http, upload         *http.Client
	token                TokenSource
	decorate             func(*http.Request)
}

// New requires HTTPS and a verified mTLS transport for the owner API.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(cfg.BaseURL, "%?#") {
		return nil, invalid("BaseURL must be a bare HTTPS origin")
	}
	if cfg.HTTPClient == nil {
		return nil, invalid("mTLS HTTPClient required")
	}
	tr, ok := cfg.HTTPClient.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil || tr.TLSClientConfig.InsecureSkipVerify || (len(tr.TLSClientConfig.Certificates) == 0 && tr.TLSClientConfig.GetClientCertificate == nil) {
		return nil, invalid("verified mTLS transport required")
	}
	hc := *cfg.HTTPClient
	hc.Transport = tr.Clone()
	hc.Jar = nil
	hc.CheckRedirect = noRedirect
	if hc.Timeout == 0 {
		hc.Timeout = 60 * time.Second
	}
	upload := http.Client{Timeout: 60 * time.Second}
	uploadTr := http.DefaultTransport.(*http.Transport).Clone()
	inheritedProxy := true
	if cfg.UploadHTTPClient != nil {
		upload = *cfg.UploadHTTPClient
		if cfg.UploadHTTPClient.Transport != nil {
			t, ok := cfg.UploadHTTPClient.Transport.(*http.Transport)
			if !ok {
				return nil, invalid("upload transport must be an HTTP transport")
			}
			uploadTr = t.Clone()
			inheritedProxy = false
		}
	}
	// A presigned URL carries its signature in the query, which makes the URL an
	// equivalent credential for the duration it is valid. http.DefaultTransport's policy
	// is ProxyFromEnvironment, so an upload would otherwise be handed -- signature and
	// all -- to a proxy the deployment never chose and that the local environment happens
	// to name. The Python client never trusts the environment here (trust_env=False);
	// clearing the inherited policy puts the two clients on the same footing and leaves an
	// explicit UploadHTTPClient as the only way to route an upload through a proxy.
	if inheritedProxy {
		uploadTr.Proxy = nil
	}
	if uploadTr.TLSClientConfig != nil {
		uploadTr.TLSClientConfig = uploadTr.TLSClientConfig.Clone()
		if uploadTr.TLSClientConfig.InsecureSkipVerify {
			return nil, invalid("upload TLS verification required")
		}
		uploadTr.TLSClientConfig.Certificates = nil
		uploadTr.TLSClientConfig.GetClientCertificate = nil
	}
	upload.Transport = uploadTr
	upload.Jar = nil
	upload.CheckRedirect = noRedirect
	if upload.Timeout == 0 {
		upload.Timeout = 60 * time.Second
	}
	// baseHost is the authority in the form requests carry it, not the form url.Parse
	// reported it: see authority.
	return &Client{base: strings.TrimSuffix(cfg.BaseURL, "/"), baseScheme: u.Scheme, baseHost: authority(u),
		http: &hc, upload: &upload, token: cfg.TokenSource, decorate: cfg.RequestDecorator}, nil
}
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
func invalid(message string) error {
	return contracts.NewError(contracts.ErrInvalid, "artifacts sdk: "+message)
}

// authority is the destination net/http dials for u, which is not always the authority
// url.Parse reported. NewRequestWithContext strips an explicit empty port from its own
// copy of the parsed URL before filling req.URL.Host and req.Host (removeEmptyPort,
// net/http/request.go, Issue 14836), so "https://host:/object" parses as "host:" and is
// dialed as "host". Any guard that compares a request against the raw parse therefore
// refuses a URL this package accepted on input -- and a guard that fires on the shape of
// a legitimate URL is indistinguishable, from the caller's side, from one that stopped an
// attack. The rule below is net/http's, copied rather than trusted: an authority whose
// last colon sits outside the last bracket and has nothing after it.
func authority(u *url.URL) string {
	if strings.LastIndex(u.Host, ":") > strings.LastIndex(u.Host, "]") {
		return strings.TrimSuffix(u.Host, ":")
	}
	return u.Host
}

// The headers this package sets or forecloses, so a caller decorator may put neither family
// on a request. credentialHeaderNames are the authorities a request is made with: the task
// token, the cookies this client refuses to carry (Jar is nil), and the proxy credential.
// authorityHeaderNames name the authority the request line and the verified TLS connection
// are claimed for. They are kept apart because a refusal has to say which guarantee fired.
var (
	credentialHeaderNames = map[string]bool{"authorization": true, "cookie": true, "proxy-authorization": true}
	authorityHeaderNames  = map[string]bool{"host": true}
)

// framingHeaderNames are the headers net/http derives from the request itself -- how the
// body is delimited and what the connection is claimed to support. A caller decorator has
// no business naming them, and two of them are a duplicate-on-the-wire hazard rather than
// a style problem: Request.write writes the Header map under whatever key it holds, and
// reqWriteExcludeHeader (net/http/request.go:99) suppresses only the CANONICAL spellings of
// Host, User-Agent, Content-Length, Transfer-Encoding and Trailer, while the transfer writer
// emits its own Content-Length line from the sanitized field (net/http/transfer.go:290). So
// Header["content-length"] arrives as a second Content-Length beside the transport's, and
// Header["transfer-encoding"] as a second Transfer-Encoding -- the CL.TE/TE.CL shape that
// makes a front end and an origin read different request boundaries. Connection, Upgrade, TE
// and Expect are not in that exclusion at all, so even canonically spelled they claim
// connection state this transport never negotiated. Refused on both decoration points.
var framingHeaderNames = map[string]bool{
	"content-length": true, "transfer-encoding": true, "te": true,
	"connection": true, "upgrade": true, "trailer": true, "expect": true,
}

// ownerNeutralizedHeaders are the canonical spellings the owner path defeats by
// construction: Authorization is overwritten immediately after the decorator has run, and
// net/http never writes a Header map entry for Host (reqWriteExcludeHeader) -- the Host: line
// comes from the request's Host FIELD instead (net/http/request.go:601-605 picks r.Host over
// r.URL.Host, and :679 writes it), which is why the field is what this package compares
// against the destination, and why reading the map alone was never enough. Every other
// spelling of an owned name reaches the wire as written, so it is refused.
var ownerNeutralizedHeaders = map[string]bool{"Authorization": true, "Host": true}

// pinnedURL is the request this package built, in the components that decide what is sent
// and to whom: Scheme and Host are where the transport dials, EscapedPath says which
// operation (owner) or which object (upload) the bytes are for, and RawQuery carries the
// signature a presigned URL is nothing without. Pinning only the authority leaves the path
// free, which is enough to turn GET into SEAL under one task token -- and the refusal for
// a request nobody sent would then be classified by the operation that was intended.
type pinnedURL struct {
	scheme, host, path, query string
}

// pinURL snapshots a request URL. It has to be called ONCE, before the decorator is handed
// the mutable request: anything derived afterwards -- a re-parse of the caller's string,
// the request's own Host field -- describes the request the decorator chose to make, not
// the one this package set out to send.
func pinURL(u *url.URL) pinnedURL {
	return pinnedURL{scheme: u.Scheme, host: u.Host, path: u.EscapedPath(), query: u.RawQuery}
}

// matches reports whether the request still points where it was built to point. The
// comparison is exact, and the refusal it drives says nothing about what the request says
// NOW: on a presigned URL the query is the credential, so neither half may be echoed into
// an error string.
func (p pinnedURL) matches(u *url.URL) bool {
	return u != nil && p.scheme == u.Scheme && p.host == u.Host && p.path == u.EscapedPath() && p.query == u.RawQuery
}

// forbiddenHeaderKey names the first header the decorator put on the request that this
// package owns, in any spelling, or "" when it put none. The map is read directly rather
// than probed with Header.Get: Get canonicalises its ARGUMENT, so Get("Authorization") is
// blind to Header["authorization"], while Header.writeSubset writes the map under whatever
// key it holds and excludes only the canonical names in reqWriteExcludeHeader -- so a
// lowercase "host" bypasses that exclusion too. A credential smuggled past a
// case-sensitive check is the same leak as one sent straight, and on the owner path it is
// worse: it travels beside the task Bearer token the SDK sets afterwards.
func forbiddenHeaderKey(req *http.Request, names, neutralized map[string]bool) string {
	for key := range req.Header {
		if !names[strings.ToLower(key)] {
			continue
		}
		if neutralized[key] {
			continue
		}
		return key
	}
	return ""
}

// WithTokenSource returns a new task-bound view without changing this client.
func (c *Client) WithTokenSource(source TokenSource) *Client {
	copy := *c
	copy.token = source
	return &copy
}

// request performs one owner call. mutation marks calls whose answer, once lost,
// cannot be assumed absent: for those, an interrupted transport or an unreadable
// answer is classified UnknownOutcomeError instead of INVALID_ARGUMENT.
func (c *Client) request(ctx context.Context, op string, in any, rangeHeader string, mutation bool) (*http.Response, error) {
	if ctx == nil {
		return nil, invalid("context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.token == nil {
		return nil, contracts.NewError(contracts.ErrForbidden, "artifacts sdk: task token required")
	}
	tok, err := c.token(ctx)
	if err != nil || strings.TrimSpace(tok) == "" {
		return nil, contracts.NewError(contracts.ErrForbidden, "artifacts sdk: task authority unavailable")
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, invalid("cannot encode request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/artifacts/"+op, bytes.NewReader(raw))
	if err != nil {
		return nil, invalid("cannot build request")
	}
	// The decorator runs BEFORE the credential is set, so it can neither replace
	// the task authority this call is made with nor remove it.
	if c.decorate != nil {
		// Snapshot before the hook, once, from the request itself: this is the URL whose
		// operation the answer will be classified by, so it is what has to survive.
		built := pinURL(req.URL)
		c.decorate(req)
		// And it runs before the credential is set for a second reason: a decorator
		// that moved the request to another authority must be refused while the task
		// token is still only in hand, never on the wire. The message stays static --
		// neither the token nor a URL with a signature may reach an error string.
		if req.URL.Scheme != c.baseScheme || req.URL.Host != c.baseHost {
			return nil, invalid("decorator must not change request authority")
		}
		// Where the connection goes is only half of the request line. net/http takes the
		// Host: header from this FIELD, in preference to the URL (net/http/request.go:601-605
		// prefers r.Host over r.URL.Host, and :679 writes it), so a decorator that leaves the
		// URL, the TLS authority and the header map alone and sets r.Host to another name
		// arrives at the right peer over a verified mTLS channel carrying a fresh task Bearer
		// while claiming to be about someone else -- behind any vhost routing or reverse
		// proxy that IS another destination. The field belongs to net/http, which fills it
		// from the URL (request.go:921), so any other value -- the empty string included --
		// means the hook took ownership of it.
		if req.Host != c.baseHost {
			return nil, invalid("decorator must not change request authority")
		}
		// An authority HEADER is the same claim by another route: the connection still goes
		// where it was told while the request line names someone else. Same refusal, and the
		// map has to be read for it -- see forbiddenHeaderKey.
		if forbiddenHeaderKey(req, authorityHeaderNames, ownerNeutralizedHeaders) != "" {
			return nil, invalid("decorator must not change request authority")
		}
		// Authority is not the destination: the same host serves every operation, so a
		// decorator that keeps Scheme and Host and rewrites the path reaches another
		// operation with the same task authority. Refusing here also keeps the answer
		// honest -- past this point the request that goes out is the request whose op name
		// an unanswered call is allowed to carry.
		if !built.matches(req.URL) {
			return nil, invalid("decorator must not rewrite the request")
		}
		// And a third: setting Authorization afterwards neutralises the canonical
		// spelling, so it does not neutralise a credential the decorator spelled another
		// way. That one would go out beside the task token, which is why the map is
		// inspected here, while the token is still only in hand.
		if forbiddenHeaderKey(req, credentialHeaderNames, ownerNeutralizedHeaders) != "" {
			return nil, invalid("decorator must not add credentials")
		}
		// And the request keeps the framing net/http computed from the body it was handed.
		// See framingHeaderNames: a smuggled content-length or transfer-encoding reaches the
		// wire beside the transport's own line, which is how one request becomes two.
		if forbiddenHeaderKey(req, framingHeaderNames, nil) != "" {
			return nil, invalid("decorator must not own framing headers")
		}
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if mutation {
			// The request was handed to the transport. Whether the owner recorded
			// anything before the answer died is not knowable from here, and a
			// canceled context does not prove the request never left either -- only
			// a cancel observed before the call does, which returned above.
			return nil, unanswered(op)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, invalid("owner request failed")
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		return nil, responseError(resp, op, mutation)
	}
	return resp, nil
}
func responseError(resp *http.Response, op string, mutation bool) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	var e struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(raw, &e) == nil && len(e.Code) > 0 && len(e.Code) < 65 {
		// The owner answered, so normally the outcome IS known. Two answers are
		// the exception: a 5xx says the server acted on the request and could not
		// finish reporting, and transfer_outcome_unknown is the owner's own way of
		// saying a transfer may have landed.
		if mutation && (resp.StatusCode >= 500 || e.Reason == "transfer_outcome_unknown") {
			// Build the normal error first and mark it: the code and the recovery
			// reason are what the caller reconciles with.
			var answered error
			if e.Reason != "" && len(e.Reason) < 129 {
				answered = contracts.NewReasoned(e.Code, e.Reason, "artifacts owner rejected request")
			} else {
				answered = contracts.NewError(e.Code, "artifacts owner rejected request")
			}
			return uncertainAnswer(op, answered)
		}
		if e.Reason != "" && len(e.Reason) < 129 {
			return contracts.NewReasoned(e.Code, e.Reason, "artifacts owner rejected request")
		}
		return contracts.NewError(e.Code, "artifacts owner rejected request")
	}
	if mutation && (resp.StatusCode < 400 || resp.StatusCode >= 500) {
		// An error body this client cannot parse, from a server that already took
		// the bytes, says nothing reliable about the recording.
		return unanswered(op)
	}
	return invalid(fmt.Sprintf("unexpected HTTP status %d", resp.StatusCode))
}
func (c *Client) call(ctx context.Context, op string, in, out any, mutation bool) error {
	resp, err := c.request(ctx, op, in, "", mutation)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		if mutation {
			return unanswered(op)
		}
		return invalid("cannot read response")
	}
	if len(raw) > 1<<20 {
		if mutation {
			return unanswered(op)
		}
		return invalid("metadata response too large")
	}
	if err = json.Unmarshal(raw, out); err != nil {
		if mutation {
			// The owner answered something this client could not read; the
			// recording it describes may exist.
			return unanswered(op)
		}
		return invalid("invalid owner response")
	}
	return nil
}

// BeginUpload registers an upload under the current token's scope and context.
func (c *Client) BeginUpload(ctx context.Context, in UploadRequest) (Upload, error) {
	var out Upload
	err := c.call(ctx, "BeginUpload", in, &out, true)
	return out, err
}

// Upload sends exactly the declared bytes without task or workload credentials.
func (c *Client) Upload(ctx context.Context, up Upload, data []byte) error {
	sum := sha256.Sum256(data)
	if int64(len(data)) != up.ExpectedSize || hex.EncodeToString(sum[:]) != up.ExpectedSHA || len(data) == 0 || int64(len(data)) > MaxSize {
		return invalid("upload bytes do not match declaration")
	}
	u, err := url.Parse(up.PresignedURL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return invalid("invalid upload URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.PresignedURL, bytes.NewReader(data))
	if err != nil {
		return invalid("cannot build upload")
	}
	// The authority net/http dials for this URL, in the form it puts in the request. It is
	// not u.Host: url.Parse keeps an explicit empty port ("host:") where NewRequestWithContext
	// puts the stripped one ("host"), so a guard comparing against the raw parse refuses the
	// very URL it was handed -- see authority.
	wantHost := authority(u)
	// The destination as this method built it, snapshotted before any caller hook can reach
	// the request. Taken from req rather than re-derived from up.PresignedURL so there is
	// exactly one answer to "what was this request meant to be".
	built := pinURL(req.URL)
	for key, value := range up.Headers {
		switch strings.ToLower(key) {
		case "authorization", "cookie", "host", "proxy-authorization":
			return invalid("unsafe upload header")
		case "content-length":
			if value != fmt.Sprint(len(data)) {
				return invalid("upload content length mismatch")
			}
		default:
			req.Header.Set(key, value)
		}
	}
	if c.decorate != nil {
		c.decorate(req)
		// A presigned PUT goes to a blob store that is not the owner, and it must
		// stay credential-free whatever the caller's decorator does. Owned headers are read
		// from the map rather than probed with Get, so every spelling counts -- a credential
		// under a spelling Header.Get cannot see still goes on the wire (forbiddenHeaderKey).
		if forbiddenHeaderKey(req, credentialHeaderNames, nil) != "" {
			return invalid("upload decorator must not add credentials")
		}
		// The Host field and the Host header are the authority the request line is claimed
		// for, and both are refused: net/http honours Header["Host"] when the field is empty
		// and honours the field outright when it is set. The field is compared against the
		// authority of the presigned URL -- in the form net/http puts requests in, see
		// authority -- and not against the empty string: NewRequestWithContext sets the field
		// TO the URL's authority, and that authority is required non-empty above, so comparing
		// with "" rejects every legitimate upload as soon as a deployment configures a
		// decorator. What must not happen is a rewrite to another authority.
		if req.Host != wantHost || forbiddenHeaderKey(req, authorityHeaderNames, nil) != "" {
			return invalid("upload decorator must not change request authority")
		}
		// Headers are not the destination. The transport dials req.URL, so a decorator
		// that touches no header at all and only rewrites req.URL would ship a validly
		// signed presigned PUT to another authority. Compare what gets dialed -- and compare
		// the whole URL, because the signature in the query is bound to the object in the
		// path: a decorator that keeps the host and swaps either one ships a signature that
		// was never meant to cover those bytes or that object.
		if req.URL.Scheme != u.Scheme || req.URL.Host != wantHost {
			return invalid("upload decorator must not change request authority")
		}
		if !built.matches(req.URL) {
			return invalid("upload decorator must not rewrite the request")
		}
		// Same rule as the owner path (framingHeaderNames), and sharper here: this PUT carries
		// a signature in its query and no credential of its own, so the only thing deciding how
		// the peer frames the body is the framing net/http derived from the bytes.
		if forbiddenHeaderKey(req, framingHeaderNames, nil) != "" {
			return invalid("upload decorator must not own framing headers")
		}
	}
	resp, err := c.upload.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return unansweredBlob("Upload", "the blob store gave no answer that proves the PUT landed")
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		if resp.StatusCode >= 500 {
			return unansweredBlob("Upload", fmt.Sprintf("the blob store answered %d", resp.StatusCode))
		}
		return invalid(fmt.Sprintf("upload status %d", resp.StatusCode))
	}
	return nil
}

// Seal freezes one verified object version at the owner.
func (c *Client) Seal(ctx context.Context, id string) (ArtifactRef, error) {
	var ref ArtifactRef
	err := c.call(ctx, "Seal", map[string]string{"id": id}, &ref, true)
	return ref, err
}

// Get checks current authority and returns immutable metadata.
func (c *Client) Get(ctx context.Context, id string) (ArtifactRef, error) {
	var ref ArtifactRef
	err := c.call(ctx, "Get", map[string]string{"id": id}, &ref, false)
	return ref, err
}

// Put performs an idempotent owner-mediated upload and verifies the sealed result.
func (c *Client) Put(ctx context.Context, in PutRequest) (ArtifactRef, error) {
	if len(in.Data) == 0 || int64(len(in.Data)) > MaxSize {
		return ArtifactRef{}, invalid("invalid artifact size")
	}
	sum := sha256.Sum256(in.Data)
	sha := hex.EncodeToString(sum[:])
	up, err := c.BeginUpload(ctx, UploadRequest{
		Key: in.Key, Name: in.Name, MIME: in.MIME, SHA256: sha, Size: int64(len(in.Data)),
		Labels: in.Labels, JournalStage: in.JournalStage,
	})
	if err != nil {
		return ArtifactRef{}, err
	}
	if err = c.Upload(ctx, up, in.Data); err != nil {
		if !IsOutcomeUnknown(err) {
			return ArtifactRef{}, err
		}
		// A lost answer is not a lost write. Seal verifies at the owner the
		// bytes that actually landed (SHA-256, size, pinned version), so a
		// confirmed Seal IS the upload confirmation and a re-send would be
		// exactly the duplicate the contract forbids. Only when the owner
		// cannot confirm does the SAME upload get one more presentation -
		// bounded, observable; if that answer is lost too the caller keeps
		// reconciling by the same ID/key.
		ref, sealErr := c.Seal(ctx, up.ID)
		if sealErr == nil {
			if ref.ID != up.ID || ref.Version == "" || ref.SHA256 != sha || ref.Size != int64(len(in.Data)) {
				return ArtifactRef{}, invalid("sealed artifact differs from upload")
			}
			return ref, nil
		}
		if contracts.Code(sealErr) == contracts.ErrNotFound {
			return ArtifactRef{}, sealErr
		}
		if err = c.Upload(ctx, up, in.Data); err != nil {
			return ArtifactRef{}, err
		}
		ref, err = c.Seal(ctx, up.ID)
		if err != nil {
			return ArtifactRef{}, err
		}
		if ref.ID != up.ID || ref.Version == "" || ref.SHA256 != sha || ref.Size != int64(len(in.Data)) {
			return ArtifactRef{}, invalid("sealed artifact differs from upload")
		}
		return ref, nil
	}
	ref, err := c.Seal(ctx, up.ID)
	if err != nil {
		return ArtifactRef{}, err
	}
	if ref.ID != up.ID || ref.Version == "" || ref.SHA256 != sha || ref.Size != int64(len(in.Data)) {
		return ArtifactRef{}, invalid("sealed artifact differs from upload")
	}
	return ref, nil
}

// Download reauthorizes both metadata and bytes and checks the full digest/size.
func (c *Client) Download(ctx context.Context, id string) ([]byte, error) {
	ref, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if ref.ID != id || ref.Version == "" || ref.Size <= 0 || ref.Size > MaxSize {
		return nil, invalid("invalid artifact metadata")
	}
	resp, err := c.request(ctx, "Download", map[string]string{"id": id}, "", false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, ref.Size+1))
	if err != nil {
		return nil, invalid("cannot read artifact")
	}
	sum := sha256.Sum256(raw)
	if resp.StatusCode != http.StatusOK || int64(len(raw)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return nil, invalid("artifact digest or size mismatch")
	}
	return raw, nil
}

// ReadRange fetches bounded bytes after fresh authorization and exact-version checks.
func (c *Client) ReadRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 || length > MaxSize {
		return nil, invalid("invalid range")
	}
	ref, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if ref.ID != id || ref.Version == "" || ref.Size <= 0 || ref.Size > MaxSize || offset >= ref.Size {
		return nil, invalid("range outside artifact")
	}
	if length > ref.Size-offset {
		length = ref.Size - offset
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := c.request(ctx, "Download", map[string]string{"id": id}, rangeHeader, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		return nil, invalid("cannot read range")
	}
	want := fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, ref.Size)
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != want || strings.Trim(resp.Header.Get("ETag"), "\"") != ref.SHA256 || int64(len(raw)) != length {
		return nil, invalid("range version or size mismatch")
	}
	return raw, nil
}
