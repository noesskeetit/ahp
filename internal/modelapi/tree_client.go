package modelapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TreeBudgetClient invokes the Model owner; it never maintains local balances.
type TreeBudgetClient struct {
	base string
	http *http.Client
}

// TreeRun carries task authority and the caller's bound. The owner derives its task.
type TreeRun struct {
	client *TreeBudgetClient
	token  string
	bound  int64
	runID  string
}

// NewTreeBudgetClient requires a verified workload TLS client and HTTPS origin.
func NewTreeBudgetClient(origin string, hc *http.Client) (*TreeBudgetClient, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || strings.ContainsAny(origin, "%?#") || hc == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "model tree API requires HTTPS origin and mTLS client")
	}
	tr, ok := hc.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil || tr.TLSClientConfig.InsecureSkipVerify || (len(tr.TLSClientConfig.Certificates) == 0 && tr.TLSClientConfig.GetClientCertificate == nil) {
		return nil, contracts.NewError(contracts.ErrInvalid, "model tree API requires verified mTLS")
	}
	copy := *hc
	copy.Transport = tr.Clone()
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout == 0 {
		copy.Timeout = 30 * time.Second
	}
	return &TreeBudgetClient{base: strings.TrimSuffix(origin, "/"), http: &copy}, nil
}

// ForRun returns an independent view for one authenticated task.
func (c *TreeBudgetClient) ForRun(token, runID string, bound int64) *TreeRun {
	return &TreeRun{client: c, token: token, runID: runID, bound: bound}
}
func (r *TreeRun) call(ctx context.Context, op string, in treeRequest, out any) error {
	if ctx == nil {
		return contracts.NewError(contracts.ErrInvalid, "context required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(r.token) == "" {
		return contracts.NewError(contracts.ErrForbidden, "task token required")
	}
	in.Bound = r.bound
	in.RunID = r.runID
	raw, err := json.Marshal(in)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid tree request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.client.base+"/v1/model/"+op, bytes.NewReader(raw))
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid tree endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return contracts.NewError(contracts.ErrInvalid, "model tree owner unavailable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return contracts.NewError(contracts.ErrInvalid, "invalid tree response")
	}
	if resp.StatusCode/100 != 2 {
		var e contracts.Error
		if json.Unmarshal(data, &e) == nil && e.Code != "" {
			return contracts.NewError(e.Code, "model tree owner rejected operation")
		}
		return contracts.NewError(contracts.ErrInvalid, "model tree operation failed")
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid tree response")
	}
	return nil
}

// ChargeStep idempotently charges one consumed model step at the owner.
func (r *TreeRun) ChargeStep(ctx context.Context, step int) error {
	return r.call(ctx, "ChargeStep", treeRequest{Step: step}, nil)
}

// Reserve atomically carves a child grant out of this task's remainder.
func (r *TreeRun) Reserve(ctx context.Context, ref string, requested int64) (DelegationGrant, error) {
	var out DelegationGrant
	err := r.call(ctx, "ReserveDelegation", treeRequest{Ref: ref, Requested: requested}, &out)
	return out, err
}

// Settle reconciles a previously reserved logical grant. Unknown delivery keeps
// the conservative charge at the owner; only an unsent call refunds its hold.
func (r *TreeRun) Settle(ctx context.Context, grant DelegationGrant, out DelegationOutcome) error {
	return r.call(ctx, "SettleDelegation", treeRequest{Ref: grant.Ref, Outcome: out}, nil)
}
