package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// The task graph is owned by the Identity service. The execution plane may
// never read its schema directly: every task-tree query goes over the Identity
// HTTP API, carrying the caller's task-scoped token so Identity itself
// enforces tenant isolation. This file is that client; the gateway keeps only
// its own parent_task metadata for routing, never a parallel authority over
// the graph.

// DefaultIdentityTreePrefix is the path prefix the Identity service mounts its
// task API under.
const DefaultIdentityTreePrefix = "/v1/identity"

// Defaults for the defensive bounds a client applies to a returned tree.
const (
	DefaultIdentityMaxNodes = 4096
	DefaultIdentityMaxDepth = 64
)

// ErrIdentityTree reports a malformed or over-broad tree response. It is
// deliberately distinct from a transport error: a well-formed HTTP success that
// violates the contract (cross-tenant data, a forged root, an absurd fan-out)
// must not be mistaken for a transient failure and retried.
var ErrIdentityTree = errors.New("gateway: identity task-tree contract violated")

// TaskNode is one task in a tree. Children are the tasks that name this node
// as their parent, already resolved and ordered by task id for determinism.
type TaskNode struct {
	TaskID       string      `json:"taskId"`
	ParentTaskID string      `json:"parentTaskId,omitempty"`
	State        string      `json:"state,omitempty"`
	Tenant       string      `json:"tenant,omitempty"`
	Children     []*TaskNode `json:"-"`
}

type wireTaskNode struct {
	TaskID       string         `json:"taskId"`
	ParentTaskID string         `json:"parentTaskId,omitempty"`
	State        string         `json:"state,omitempty"`
	Tenant       string         `json:"tenant,omitempty"`
	Children     []wireTaskNode `json:"children,omitempty"`
}

// TaskTree is a rooted subtree returned by Identity. Tenant is the isolation
// domain the whole tree lives in; it is cross-checked against the caller's
// scope so a misbehaving Identity cannot smuggle another tenant's graph.
type TaskTree struct {
	Tenant string    `json:"tenant"`
	Root   *TaskNode `json:"root"`
}

type wireTaskTree struct {
	Tenant   string       `json:"tenant"`
	RootTask string       `json:"rootTaskId"`
	Root     wireTaskNode `json:"task"`
}

// IdentityTaskTree resolves a task subtree through the Identity API. Implement
// ations must fail closed on any tenant mismatch or contract violation.
type IdentityTaskTree interface {
	TaskTree(ctx context.Context, scope contracts.Scope, taskID string) (*TaskTree, error)
}

// Find returns the node with the given id in the tree, or nil.
func (t *TaskTree) Find(taskID string) *TaskNode {
	if t == nil || t.Root == nil {
		return nil
	}
	var found *TaskNode
	walk(t.Root, func(n *TaskNode) bool {
		if n.TaskID == taskID {
			found = n
			return false
		}
		return true
	})
	return found
}

// All returns every node in breadth-first order (root first).
func (t *TaskTree) All() []*TaskNode {
	if t == nil || t.Root == nil {
		return nil
	}
	var out []*TaskNode
	walk(t.Root, func(n *TaskNode) bool { out = append(out, n); return true })
	return out
}

// Count returns the number of nodes in the tree.
func (t *TaskTree) Count() int { return len(t.All()) }

// IsDescendant reports whether descendant names taskID descendantID anywhere
// strictly below taskID ancestorID. A task is never its own descendant.
func (t *TaskTree) IsDescendant(ancestorID, descendantID string) bool {
	anc := t.Find(ancestorID)
	if anc == nil || ancestorID == descendantID {
		return false
	}
	desc := false
	walk(anc, func(n *TaskNode) bool {
		if n == anc {
			return true
		}
		if n.TaskID == descendantID {
			desc = true
			return false
		}
		return true
	})
	return desc
}

// walk visits the subtree rooted at n, stopping early when fn returns false.
func walk(n *TaskNode, fn func(*TaskNode) bool) {
	if n == nil {
		return
	}
	if !fn(n) {
		return
	}
	for _, c := range n.Children {
		walk(c, fn)
	}
}

// IdentityClientConfig configures the HTTP task-tree client.
type IdentityClientConfig struct {
	// Base is the Identity service origin (optionally with a path prefix).
	Base string
	// Prefix overrides DefaultIdentityTreePrefix.
	Prefix string
	// MaxNodes bounds the total nodes accepted in one tree (DoS guard).
	MaxNodes int
	// MaxDepth bounds tree depth (recursion/DoS guard).
	MaxDepth int
}

// HTTPIdentityClient fetches task trees from the Identity service over a
// netpolicy-gated HTTP client, so egress is restricted to the registered
// Identity host and a redirect can never escape that boundary.
type HTTPIdentityClient struct {
	base     *url.URL
	prefix   string
	maxNodes int
	maxDepth int
	client   *http.Client
	tokens   TokenSource
}

// NewHTTPIdentityClient builds the production client. policy must allow the
// Identity host; because loopback is denied unconditionally, tests inject a
// resolver/dialer or use newHTTPIdentityClientWithHTTP.
func NewHTTPIdentityClient(cfg IdentityClientConfig, policy netpolicy.Policy, tokens TokenSource, opts ...netpolicy.Option) (*HTTPIdentityClient, error) {
	base, err := parseIdentityBase(cfg.Base)
	if err != nil {
		return nil, err
	}
	client, err := netpolicy.NewClient(policy, opts...)
	if err != nil {
		return nil, fmt.Errorf("gateway: identity client: %w", err)
	}
	c, err := newHTTPIdentityClientWithHTTP(cfg, client, tokens)
	if err != nil {
		return nil, err
	}
	c.base = base
	return c, nil
}

// newHTTPIdentityClientWithHTTP is the shared constructor used by tests with a
// pre-built client (an httptest-backed http.Client), mirroring the model
// package's *WithHTTP convention.
func newHTTPIdentityClientWithHTTP(cfg IdentityClientConfig, client *http.Client, tokens TokenSource) (*HTTPIdentityClient, error) {
	if client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: identity client needs an http client")
	}
	base, err := parseIdentityBase(cfg.Base)
	if err != nil {
		return nil, err
	}
	prefix := strings.TrimRight(strings.TrimSpace(cfg.Prefix), "/")
	if prefix == "" {
		prefix = DefaultIdentityTreePrefix
	}
	if !strings.HasPrefix(prefix, "/") {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: identity prefix must start with /")
	}
	maxNodes := cfg.MaxNodes
	if maxNodes <= 0 {
		maxNodes = DefaultIdentityMaxNodes
	}
	maxDepth := cfg.MaxDepth
	if maxDepth <= 0 {
		maxDepth = DefaultIdentityMaxDepth
	}
	return &HTTPIdentityClient{base: base, prefix: prefix, maxNodes: maxNodes, maxDepth: maxDepth, client: client, tokens: tokens}, nil
}

func parseIdentityBase(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: identity base: "+err.Error())
	}
	if u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: identity base must be an http(s) origin")
	}
	return u, nil
}

// treeURL builds the GET URL for one subtree.
func (c *HTTPIdentityClient) treeURL(taskID string) string {
	u := *c.base
	u.Path = strings.TrimRight(c.base.Path, "/") + c.prefix + "/tasks/" + url.PathEscape(taskID) + "/tree"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// TaskTree implements IdentityTaskTree.
func (c *HTTPIdentityClient) TaskTree(ctx context.Context, scope contracts.Scope, taskID string) (*TaskTree, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(taskID) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: identity task-tree needs a task id")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.treeURL(taskID), nil)
	if err != nil {
		return nil, fmt.Errorf("gateway: identity request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.tokens != nil {
		tok, err := c.tokens.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("gateway: identity token: %w", err)
		}
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway: identity request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, drainErr := netpolicy.Drain(resp, netpolicy.DefaultMaxResponseBytes)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, contracts.NewError(contracts.ErrNotFound, "gateway: identity task not found")
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, contracts.NewError(contracts.ErrForbidden, "gateway: identity refused the task-tree request")
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, fmt.Errorf("%w: identity endpoint %d: %s", ErrIdentityTree, resp.StatusCode, truncate(string(raw), 200))
	}
	if drainErr != nil {
		return nil, fmt.Errorf("%w: identity body unreadable: %v", ErrIdentityTree, drainErr)
	}
	return parseTaskTree(raw, scope, taskID, c.maxNodes, c.maxDepth)
}

// parseTaskTree validates a wire tree against the caller's contract before it
// becomes a TaskTree: the root must be the task that was asked for, every node
// must belong to the caller's tenant, and both fan-out and depth must stay
// within bounds. A violation is ErrIdentityTree, never a silent success.
func parseTaskTree(raw []byte, scope contracts.Scope, wantRoot string, maxNodes, maxDepth int) (*TaskTree, error) {
	var w wireTaskTree
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIdentityTree, err)
	}
	if w.Tenant == "" {
		return nil, fmt.Errorf("%w: response omits tenant", ErrIdentityTree)
	}
	if w.Tenant != scope.Tenant {
		return nil, fmt.Errorf("%w: response tenant %q is outside the caller scope", ErrIdentityTree, w.Tenant)
	}
	if w.RootTask != "" && w.RootTask != wantRoot {
		return nil, fmt.Errorf("%w: root %q does not match the requested task %q", ErrIdentityTree, w.RootTask, wantRoot)
	}
	root, err := w.Root.toNode(w.Tenant, scope.Tenant, 1, maxNodes, maxDepth)
	if err != nil {
		return nil, err
	}
	return &TaskTree{Tenant: w.Tenant, Root: root}, nil
}

// toNode validates and converts one wire node. budget and depth are threaded
// down so the whole tree is bounded, and tenant is checked at every level.
func (w wireTaskNode) toNode(treeTenant, scopeTenant string, depth, maxNodes, maxDepth int) (*TaskNode, error) {
	if maxNodes <= 0 {
		return nil, fmt.Errorf("%w: tree exceeds %d nodes", ErrIdentityTree, maxNodes)
	}
	if depth > maxDepth {
		return nil, fmt.Errorf("%w: tree exceeds depth %d", ErrIdentityTree, maxDepth)
	}
	if strings.TrimSpace(w.TaskID) == "" {
		return nil, fmt.Errorf("%w: node without a task id", ErrIdentityTree)
	}
	if w.Tenant != "" && w.Tenant != scopeTenant {
		return nil, fmt.Errorf("%w: node %q belongs to tenant %q", ErrIdentityTree, w.TaskID, w.Tenant)
	}
	if w.Tenant == "" {
		w.Tenant = treeTenant
	}
	node := &TaskNode{TaskID: w.TaskID, ParentTaskID: w.ParentTaskID, State: w.State, Tenant: w.Tenant}
	budget := maxNodes - 1
	for _, child := range w.Children {
		cn, err := child.toNode(treeTenant, scopeTenant, depth+1, budget, maxDepth)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, cn)
		budget--
	}
	return node, nil
}

// StubIdentityClient is an in-memory IdentityTaskTree for tests and local
// wiring. It enforces tenant isolation the way Identity does: a task added
// under one tenant is invisible to another tenant's scope.
type StubIdentityClient struct {
	tasks map[contracts.Scope]map[string]*TaskNode
}

// NewStubIdentityClient returns an empty stub.
func NewStubIdentityClient() *StubIdentityClient {
	return &StubIdentityClient{tasks: map[contracts.Scope]map[string]*TaskNode{}}
}

// AddTask records one task in a tenant under the given scope. parentID may be
// empty for a root. Re-adding the same id updates its parent/state.
func (s *StubIdentityClient) AddTask(scope contracts.Scope, taskID, parentID, state string) {
	if s.tasks == nil {
		s.tasks = map[contracts.Scope]map[string]*TaskNode{}
	}
	per := s.tasks[scope]
	if per == nil {
		per = map[string]*TaskNode{}
		s.tasks[scope] = per
	}
	node := per[taskID]
	if node == nil {
		node = &TaskNode{TaskID: taskID}
		per[taskID] = node
	}
	node.ParentTaskID = parentID
	node.State = state
	node.Tenant = scope.Tenant
}

// TaskTree implements IdentityTaskTree. It fails NotFound when the task does
// not exist in the caller's tenant, which is how Identity hides cross-tenant
// ids (no existence oracle).
func (s *StubIdentityClient) TaskTree(_ context.Context, scope contracts.Scope, taskID string) (*TaskTree, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	per := s.tasks[scope]
	if per == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "gateway: identity task not found")
	}
	root := per[taskID]
	if root == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "gateway: identity task not found")
	}
	tree := &TaskTree{Tenant: scope.Tenant, Root: s.expand(per, root)}
	return tree, nil
}

// expand rebuilds a node's subtree from the flat parent map, guarding against
// cycles (a malformed graph must not hang the client).
func (s *StubIdentityClient) expand(per map[string]*TaskNode, root *TaskNode) *TaskNode {
	seen := map[string]bool{root.TaskID: true}
	return s.expandSeen(per, root, seen)
}

func (s *StubIdentityClient) expandSeen(per map[string]*TaskNode, root *TaskNode, seen map[string]bool) *TaskNode {
	node := &TaskNode{TaskID: root.TaskID, ParentTaskID: root.ParentTaskID, State: root.State, Tenant: root.Tenant}
	for _, cand := range per {
		if cand.ParentTaskID != root.TaskID || seen[cand.TaskID] {
			continue
		}
		seen[cand.TaskID] = true
		node.Children = append(node.Children, s.expandSeen(per, cand, seen))
	}
	return node
}

var (
	_ IdentityTaskTree = (*HTTPIdentityClient)(nil)
	_ IdentityTaskTree = (*StubIdentityClient)(nil)
)
