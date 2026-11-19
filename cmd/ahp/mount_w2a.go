package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// This file mounts the accepted Registry and Triggers packages onto the composed
// front (role "all"). Both packages were accepted in W2-A as DB Store/Service
// packages WITHOUT an HTTP face, so the front could not serve the catalog or
// the trigger CRUD the product PDF requires. The mounts below add the missing
// face behind the SAME authorize + verifier + mTLS contract the accepted State
// face uses, gated by AHP_ENABLE_REGISTRY / AHP_ENABLE_TRIGGERS (default OFF)
// so the default build and every existing role stay byte-identical.

// tryMountRegistry migrates the Registry store and mounts its HTTP face at
// /v1/registry/. It is untagged: registry builds in every binary, so this path
// can only be reached when the operator sets AHP_ENABLE_REGISTRY=1.
func (s *server) tryMountRegistry(ctx context.Context, mux *http.ServeMux,
	verifier *auth.Verifier, authorize transport.Authorize) error {
	// Publication is gated on a passing evaluation verdict ("оценки перед
	// публикацией"), so the registry store is built over the evals plane rather
	// than left ungated: an ungated store would fail closed and reject every
	// version, which is the safe default for a bare store but not for a deployment
	// that has the evals plane available. Migrate evals first so the verdict table
	// the gate consults exists before the registry store is used.
	evalStore := evals.NewStore(s.pool)
	if err := evalStore.Migrate(ctx); err != nil {
		return fmt.Errorf("evals migrate: %w", err)
	}
	// The freshness half of "оценки перед публикацией" (row 24: актуальный
	// policy) is an operator declaration, not a caller argument: AHP_EVAL_POLICY_FILE
	// holds the policies verdicts are earned under and aged against. Without one
	// the plane stays closed -- publication and promotion deny with "no policy is
	// declared" -- and that is the loud fail-closed default, never a silent policy.
	var evalPolicies []evals.Policy
	if err := readOperatorJSON(envOr("AHP_EVAL_POLICY_FILE", ""), &evalPolicies); err != nil {
		return fmt.Errorf("eval policy configuration: %w", err)
	}
	for _, pol := range evalPolicies {
		if _, err := evalStore.SetPolicy(ctx, pol); err != nil {
			return fmt.Errorf("declare eval policy %s: %w", pol.ID, err)
		}
	}
	if len(evalPolicies) == 0 {
		s.logger.Warn("NO EVAL POLICY DECLARED (AHP_EVAL_POLICY_FILE): publication and promotion stay blocked until an operator declares one")
	} else {
		s.logger.Info("eval policies declared", "count", len(evalPolicies))
	}
	// Deploy admission (PDF rev 3.2, c. 41): with operator trust roots installed,
	// installing or promoting an image-bearing version requires a signature that
	// chains to a root, an SBOM matching that signature, a passing evaluation and
	// -- for stage/prod -- a passing security clearance, all bound to the image's
	// real @sha256 digest. The mode is mandatory (no silent off); "disabled" is a
	// loud operator election that leaves the plane unadmitted, used only locally.
	supplyRoots, supplyMode, err := registrySupplyChainOption()
	if err != nil {
		return err
	}
	if supplyMode == "synthetic" {
		s.logger.Warn("SYNTHETIC SUPPLY-CHAIN STAND: registry accepts images WITHOUT signature or SBOM verification -- evidence from this mode must be marked synthetic, never for stage/prod")
	}
	storeOpts := []registry.Option{registry.WithEvalGate(evalStore)}
	if supplyRoots != nil {
		storeOpts = append(storeOpts, registry.WithSupplyChain(supplyRoots))
	} else if supplyMode == "synthetic" {
		storeOpts = append(storeOpts, registry.WithSyntheticSupplyChain("AHP_REGISTRY_SUPPLY_CHAIN="+envOr("AHP_REGISTRY_SUPPLY_CHAIN", "")))
	}
	// The same operator mTLS client that dispatches invocations probes runtime
	// readiness before traffic is switched (promote gate) and by the auto-rollback
	// supervisor. Without a runtime client there is nothing to probe, so the gate
	// stays inactive -- the dispatcher likewise never routes a task.
	var runtimeClient *http.Client
	if cert := envOr("AHP_RUNTIME_CLIENT_CERT", ""); cert != "" {
		runtimeClient, err = space.NewMTLSClient(cert, envOr("AHP_RUNTIME_CLIENT_KEY", ""), envOr("AHP_RUNTIME_CLIENT_CA", ""))
		if err != nil {
			return fmt.Errorf("runtime client: %w", err)
		}
		storeOpts = append(storeOpts, registry.WithPromoteReadinessGate(registry.NewHTTPReadinessProber(runtimeClient)))
	}
	store := registry.New(s.pool, storeOpts...)
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("registry migrate: %w", err)
	}
	handler, err := registry.NewHTTPHandler(store, verifier, authorize)
	if err != nil {
		return fmt.Errorf("registry handler: %w", err)
	}
	if err := attachRegistryManagement(handler, verifier, s.identityStore(), strings.Split(envOr("AHP_IDENTITY_EXCHANGER_DEPLOYMENTS", ""), ","), envOr("AHP_BROWSER_AUDIENCE", "ahp-browser")); err != nil {
		return err
	}
	// Security clearances are written only by a live human security authority, a
	// strictly narrower grant than registry administration and never by a task.
	if err := registry.AttachSecurityAuthority(handler, verifier, envOr("AHP_BROWSER_AUDIENCE", "ahp-browser"), registrySecurityAuthority(s.identityStore(), strings.Split(envOr("AHP_IDENTITY_EXCHANGER_DEPLOYMENTS", ""), ","))); err != nil {
		return fmt.Errorf("registry security authority: %w", err)
	}
	mux.Handle("/v1/registry/", handler)
	s.logger.Info("registry plane mounted", "prefix", "/v1/registry/", "eval_gate", "required", "supply_chain", supplyMode)

	var bindings []registry.RuntimeBinding
	if err := readOperatorJSON(envOr("AHP_REGISTRY_RUNTIMES_FILE", ""), &bindings); err != nil {
		return fmt.Errorf("runtime configuration: %w", err)
	}
	for _, binding := range bindings {
		if err := store.RegisterRuntime(ctx, binding); err != nil {
			return err
		}
	}
	_, signer := devIdentityAuth()
	authority := runtimeAuthority{ids: s.identityStore(), registry: store, verifier: verifier, signer: signer}
	var observers []registryStateContract
	if err := readOperatorJSON(envOr("AHP_REGISTRY_STATE_OBSERVERS_FILE", ""), &observers); err != nil {
		return err
	}
	if len(observers) > 0 {
		images := map[string]bool{}
		for _, o := range observers {
			if o.Image == "" || o.Protocol != registryStateProtocol || images[o.Image] {
				return fmt.Errorf("invalid registry owner history contract")
			}
			images[o.Image] = true
		}
		owner := registryStateObserver{ids: authority.ids, registry: store, state: state.New(s.pool), images: images}
		if err := registry.AttachOwnerObserver(handler, owner, verifier, envOr("AHP_BROWSER_AUDIENCE", "ahp-browser"), registryObservationAuthority(authority.ids, strings.Split(envOr("AHP_IDENTITY_EXCHANGER_DEPLOYMENTS", ""), ","))); err != nil {
			return err
		}
	}

	authority.budgets, err = newRuntimeBudgetAdmitter(ctx, s.pool, authority.ids)
	if err != nil {
		return fmt.Errorf("runtime budget admission: %w", err)
	}
	// A single gateway workload reaches only operator-registered version endpoints.
	if runtimeClient != nil {
		dispatcher, err := registry.NewRoutedA2AInvokeDispatcher(store, runtimeClient)
		if err != nil {
			return err
		}
		dispatcher.SetRuntimeTokenIssuer(authority)
		if err := registry.AttachInvokeDispatcher(handler, dispatcher); err != nil {
			return err
		}
	}
	eh, err := evals.NewHTTPHandler(evalStore, verifier, authorize)
	if err != nil {
		return err
	}
	var suites []evals.Suite
	if err := readOperatorJSON(envOr("AHP_EVAL_SUITES_FILE", ""), &suites); err != nil {
		return fmt.Errorf("eval configuration: %w", err)
	}
	if len(suites) > 0 {
		if runtimeClient == nil {
			return fmt.Errorf("eval suites require the runtime mTLS client")
		}
		if len(evalPolicies) == 0 {
			return fmt.Errorf("eval suites require a declared eval policy (AHP_EVAL_POLICY_FILE): a run without a policy certifies nothing")
		}
		runner, err := evals.NewRunner(evalStore, suites, runtimeClient)
		if err != nil {
			return err
		}
		runner.SetTokenIssuer(authority)
		if err := evals.AttachRunner(eh, runner); err != nil {
			return err
		}
	}
	mux.Handle("/v1/evals/", eh)

	return nil
}

// tryMountTriggers migrates the Triggers schema, wires the Service to its
// collaborators (live Identity grants, selected Registry runtimes and scoped
// Vault signing secrets), mounts its HTTP face at /v1/triggers/, and starts
// the dispatcher cycle (Service.ProcessDue) on a ticker so due triggers fire.
func (s *server) tryMountTriggers(ctx context.Context, mux *http.ServeMux, _ string,
	verifier *auth.Verifier, _ transport.Authorize, signer *auth.Signer) error {
	schema := envOr("AHP_TRIGGERS_SCHEMA", "ahp_triggers_all")
	ids := s.identityStore()
	if err := ids.Migrate(ctx); err != nil {
		return fmt.Errorf("triggers identity migrate: %w", err)
	}
	dispatcher, err := s.registryTriggerDispatcher(ctx, ids, verifier, signer)
	if err != nil {
		return fmt.Errorf("triggers selected runtime dispatcher: %w", err)
	}
	secrets, err := triggerSecretResolverFromEnv()
	if err != nil {
		return fmt.Errorf("triggers Vault configuration: %w", err)
	}
	authority := identityTriggerAuthority{store: ids, signer: signer, verifier: verifier}
	svc, err := triggers.New(ctx, s.pool, schema, triggers.Config{
		Issuer:     authority,
		Dispatcher: dispatcher,
		Secrets:    secrets,
		Tasks:      authority,
		FireTTL:    10 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("triggers service: %w", err)
	}
	s.registerTenantDeletionOwner("triggers", svc)
	s.registerSubjectDeletionOwner("triggers", svc)
	handler, err := triggers.NewHTTPHandler(svc, verifier, ids.Authorize)
	if err != nil {
		return fmt.Errorf("triggers handler: %w", err)
	}
	mux.Handle("/v1/triggers/", handler)
	ingress, err := triggers.NewWebhookHandler(svc, strings.Split(envOr("AHP_TRIGGERS_WEBHOOK_INGRESS_DEPLOYMENTS", ""), ","))
	if err != nil {
		return fmt.Errorf("triggers webhook ingress: %w", err)
	}
	mux.Handle(triggers.WebhookPathPrefix, ingress)

	// The dispatcher cycle: run ProcessDue on a ticker inside role "all" behind
	// the same trigger flag. A fire is the accepted Service cycle, so the ticker
	// and the /v1/triggers/FireNow endpoint drive exactly one shared code path.
	tick := 2 * time.Second
	if raw := strings.TrimSpace(envOr("AHP_TRIGGERS_TICKER", "")); raw != "" {
		if d, perr := time.ParseDuration(raw); perr == nil && d > 0 {
			tick = d
		}
	}
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, derr := svc.ProcessDue(ctx, time.Now().UTC(), 0); derr != nil {
					s.logger.Warn("trigger dispatcher pass failed", "err", derr)
				}
			}
		}
	}()
	s.logger.Info("triggers plane mounted", "prefix", "/v1/triggers/", "schema", schema, "ticker", tick.String())
	return nil
}

// selfA2ADispatcher points the trigger dispatcher at THIS front's public A2A
// entry. The composed front answers /a2a/public under mTLS, so the client
// presents the front's own certificate (signed by the stand CA the server
// trusts) over the loopback. AHP_TRIGGERS_A2A_URL overrides the derived URL.
func (s *server) selfA2ADispatcher(listenAddr string) (*triggers.A2ADispatcher, error) {
	url := strings.TrimSpace(envOr("AHP_TRIGGERS_A2A_URL", ""))
	if url == "" {
		port := listenAddr
		if i := strings.LastIndex(listenAddr, ":"); i >= 0 {
			port = listenAddr[i+1:]
		}
		if s.tlsCfg != nil && len(s.tlsCfg.Certificates) > 0 {
			url = "https://localhost:" + port + "/a2a/public"
		} else {
			url = "http://127.0.0.1:" + port + "/a2a/public"
		}
	}
	cfg := triggers.A2AConfig{URL: url, Timeout: 120 * time.Second}
	// The platform's A2A entry is mTLS and verifies the client certificate's
	// key usage, so the dispatcher must present a CLIENT certificate (clientAuth
	// EKU) bound to the workload whose fires it dispatches -- never the server
	// certificate. The operator names that certificate via env; without it the
	// dispatcher falls back to the server identity (which the entry rejects) so
	// misconfiguration fails loudly rather than silently mis-dispatching.
	if cert := envOr("AHP_TRIGGERS_CLIENT_CERT", ""); cert != "" {
		key := envOr("AHP_TRIGGERS_CLIENT_KEY", "")
		ca := envOr("AHP_TRIGGERS_CLIENT_CA", "")
		pair, perr := tls.LoadX509KeyPair(cert, key)
		if perr != nil {
			return nil, fmt.Errorf("triggers: load client cert: %w", perr)
		}
		client := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, ServerName: "localhost"}
		if ca != "" {
			pem, rerr := os.ReadFile(ca)
			if rerr != nil {
				return nil, fmt.Errorf("triggers: read client CA: %w", rerr)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("triggers: client CA did not parse")
			}
			client.RootCAs = pool
		}
		cfg.TLS = client
	} else if s.tlsCfg != nil && len(s.tlsCfg.Certificates) > 0 {
		client := &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: s.tlsCfg.Certificates,
			RootCAs:      s.tlsCfg.ClientCAs,
			ServerName:   "localhost",
		}
		cfg.TLS = client
	}
	return triggers.NewA2ADispatcher(cfg)
}
