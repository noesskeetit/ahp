package runcoord

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// CallerPrincipal combines the signed caller claims and the verified mTLS peer.
type CallerPrincipal struct {
	Claims auth.Claims
	Peer   contracts.Scope
}

type AgentAuthorizer func(context.Context, CallerPrincipal, contracts.Scope) error

type AccessPolicy struct {
	Verifier                *auth.Verifier
	UserAudience            string
	BrokerWorkloadIDs       []string
	PlatformEdgeWorkloadIDs []string
	UserEdgeDeployments     []string
	AuthorizeAgent          AgentAuthorizer
}

func (p AccessPolicy) authorize(r *http.Request, agent string) (contracts.Scope, error) {
	denied := contracts.NewError(contracts.ErrForbidden, "coordinator agent access denied")
	peer, ok := transport.FromContext(r.Context())
	if !ok || p.Verifier == nil || p.AuthorizeAgent == nil {
		return contracts.Scope{}, denied
	}
	token, ok := bearer(r)
	if !ok {
		return contracts.Scope{}, denied
	}
	peerID := auth.WorkloadID(peer.Scope)
	claims, taskErr := p.Verifier.Verify(token, "ahp", auth.KindTask, peerID)
	if taskErr == nil {
		if claims.Tenant != peer.Scope.Tenant || claims.Deployment != peer.Scope.Deployment ||
			!slices.Contains(claims.Capabilities, "run.coordinate") ||
			(agent != claims.Deployment && !slices.Contains(p.BrokerWorkloadIDs, peerID)) {
			return contracts.Scope{}, denied
		}
	} else {
		platformEdge := slices.Contains(p.PlatformEdgeWorkloadIDs, peerID)
		userEdge := slices.Contains(p.UserEdgeDeployments, peer.Scope.Deployment)
		if p.UserAudience == "" || !(platformEdge || userEdge) {
			return contracts.Scope{}, denied
		}
		var err error
		claims, err = p.Verifier.Verify(token, p.UserAudience, auth.KindHuman, "")
		if err != nil || (!platformEdge && claims.Tenant != peer.Scope.Tenant) {
			return contracts.Scope{}, denied
		}
	}

	target := contracts.Scope{Tenant: claims.Tenant, Deployment: agent, Subject: claims.Subject}
	if target.Validate() != nil {
		return contracts.Scope{}, denied
	}
	if err := p.AuthorizeAgent(r.Context(), CallerPrincipal{Claims: claims, Peer: peer.Scope}, target); err != nil {
		return contracts.Scope{}, denied
	}
	return target, nil
}

func bearer(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	token = strings.TrimSpace(token)
	return token, ok && strings.EqualFold(scheme, "Bearer") && token != "" && !strings.ContainsAny(token, " \t")
}

// ReadAgentAccess loads the active agent installations and their explicit
// subject and broker grants. Broker names in each record refer to deployments
// in that record's tenant and are separate from the ingress workload allowlist.
func ReadAgentAccess(r io.Reader) (AgentAuthorizer, error) {
	type agentKey struct{ tenant, deployment string }
	type grant struct {
		Tenant     string   `json:"tenant"`
		Deployment string   `json:"deployment"`
		Subjects   []string `json:"subjects"`
		Brokers    []string `json:"brokers,omitempty"`
	}
	var file struct {
		Agents []grant `json:"agents"`
	}
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("read agent access policy: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("agent access policy requires one JSON object")
	}
	agents := make(map[agentKey]grant, len(file.Agents))
	for _, g := range file.Agents {
		key := agentKey{tenant: g.Tenant, deployment: g.Deployment}
		if _, exists := agents[key]; exists {
			return nil, fmt.Errorf("duplicate agent access entry for %q/%q", g.Tenant, g.Deployment)
		}
		for _, subject := range append([]string{"deployment"}, g.Subjects...) {
			if err := (contracts.Scope{Tenant: g.Tenant, Deployment: g.Deployment, Subject: subject}).Validate(); err != nil {
				return nil, fmt.Errorf("invalid agent access entry for %q/%q: %w", g.Tenant, g.Deployment, err)
			}
		}
		for _, broker := range g.Brokers {
			if err := (contracts.Scope{Tenant: g.Tenant, Deployment: broker, Subject: "deployment"}).Validate(); err != nil {
				return nil, fmt.Errorf("invalid broker entry for %q/%q: %w", g.Tenant, g.Deployment, err)
			}
		}
		agents[key] = g
	}
	return func(_ context.Context, caller CallerPrincipal, target contracts.Scope) error {
		g, installed := agents[agentKey{tenant: target.Tenant, deployment: target.Deployment}]
		if !installed || !slices.Contains(g.Subjects, target.Subject) {
			return contracts.NewError(contracts.ErrForbidden, "subject has no grant to the installed agent")
		}
		if caller.Claims.Kind == auth.KindTask && caller.Claims.Deployment != target.Deployment &&
			(caller.Peer.Tenant != target.Tenant || !slices.Contains(g.Brokers, caller.Peer.Deployment)) {
			return contracts.NewError(contracts.ErrForbidden, "broker has no grant to the installed agent")
		}
		return nil
	}, nil
}
