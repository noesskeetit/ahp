package execbridge

// One provider copy, one secret, one client.
//
// The audit finding (ARCH-02, source path gateway_model.go -> model.Service ->
// s.inf.Stream): the router decided (model, provider) and the provider was used
// only to attribute a quota window, while every generation was dialled by the one
// client built from one secret. Two copies of one upstream -- one external, one
// in-tenant -- therefore had the SAME physical destination, and the ledger could
// not tell an operator that what it labelled "in-tenant" had in fact left. The
// provider table is useless as a routing policy if it does not choose the socket.
//
// This file makes the decision physical. Every copy named in the operator's table
// gets its client from ITS OWN secret_ref; the copy this runtime was configured
// with keeps the client the environment mounted. A copy whose secret cannot be
// resolved is refused at admission, before any budget hold, quota booking or
// generation row -- and it is never served by the default client. Substitution is
// the defect, not the fallback.
//
// Resolution is lazy on purpose. Mounting a table must not require that every
// declared copy's secret be reachable at start-up: the copy that is never routed
// to costs nothing, while the copy that IS routed to must resolve or refuse. That
// keeps the default acceptance path (one mounted copy) green and makes an
// unmounted second copy a visible refusal instead of a silent misdelivery.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Refusal reasons of the physical provider selection.
const (
	// ReasonProviderNotDeclared: the router chose a copy the mounted table does not
	// contain. Unreachable while one table feeds both; refused anyway.
	ReasonProviderNotDeclared = "provider_not_declared"
	// ReasonQuotaRequiredUnconfigured: the operator declared provider quotas
	// mandatory, and this deployment named no provider to bound. Mounting would
	// advertise bounded tenants while admitting every request.
	ReasonQuotaRequiredUnconfigured = "provider_quota_unconfigured"
	// ReasonProviderSecretUnresolved: the copy's secret_ref could not be read (no
	// secret store mounted, unknown reference, unreadable path). The generation is
	// refused; no other copy's credential is tried.
	ReasonProviderSecretUnresolved = "provider_secret_unresolved"
	// ReasonProviderCopyUnreachable: the copy the router chose cannot be served
	// from this process. The wrapper the transport can classify -- without it a
	// configuration fact arrives as a bare 500 (DEF-DOTE2E-2 discipline).
	ReasonProviderCopyUnreachable = "provider_copy_unreachable"
)

// envRefPattern mirrors the provider table's own rule for a secret reference that
// names an environment variable rather than a key-value path.
var envRefPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,64}$`)

// SecretResolver reads one provider runtime secret by key-value path.
type SecretResolver func(ctx context.Context, kvPath string) (secretstore.QwenSecret, error)

// inferenceFactory turns a resolved secret into the client that dials it.
// Production uses model.NewQwenClient, which pins egress to that secret's host;
// a local regression passes a factory that dials a test sink.
type inferenceFactory func(secret secretstore.QwenSecret) (model.Inference, error)

// providerClients is the model.ProviderClients implementation of this plane.
type providerClients struct {
	router  *routing.Router
	resolve SecretResolver
	build   inferenceFactory
	log     *slog.Logger

	mu     sync.Mutex
	ready  map[string]model.Inference
	failed map[string]error
	// dialed records, per copy, the endpoint its client reaches -- so the
	// operator journal can name the destination without carrying its credential.
	dialed map[string]string
}

func newProviderClients(router *routing.Router, resolve SecretResolver, build inferenceFactory, log *slog.Logger) *providerClients {
	if log == nil {
		log = slog.Default()
	}
	return &providerClients{
		router: router, resolve: resolve, build: build, log: log,
		ready:  map[string]model.Inference{},
		failed: map[string]error{},
		dialed: map[string]string{},
	}
}

// register binds the client this runtime already holds to the provider copy it
// belongs to. Only a declared copy may be registered: an env name that is not in
// the table must not gain a client the operator never wrote a route for.
func (c *providerClients) register(provider string, inf model.Inference) {
	if _, ok := c.router.Table().ByName()[provider]; !ok {
		return
	}
	if provider == "" || inf == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready[provider] = inf
	c.dialed[provider] = endpointOf(inf)
}

// InferenceFor implements model.ProviderClients: the client that may run a
// generation recorded against this copy.
func (c *providerClients) InferenceFor(ctx context.Context, provider string) (model.Inference, error) {
	return c.resolveCopy(ctx, provider)
}

// Resolve is the pre-admission form the gateway uses: asking BEFORE a budget hold
// or a quota booking whether this copy can actually be served.
func (c *providerClients) Resolve(ctx context.Context, provider string) (model.Inference, error) {
	return c.resolveCopy(ctx, provider)
}

// EndpointOf reports the redacted dial target recorded for a copy ("" unknown).
func (c *providerClients) EndpointOf(provider string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dialed[provider]
}

func (c *providerClients) resolveCopy(ctx context.Context, provider string) (model.Inference, error) {
	if provider == "" {
		return nil, errors.New("execbridge: no provider named for this generation")
	}
	c.mu.Lock()
	if inf, ok := c.ready[provider]; ok {
		c.mu.Unlock()
		return inf, nil
	}
	c.mu.Unlock()

	declared, ok := c.router.Table().ByName()[provider]
	if !ok {
		return nil, c.fail(provider, fmt.Errorf("%s: provider %q is not declared in the mounted provider table",
			ReasonProviderNotDeclared, provider))
	}
	if c.resolve == nil {
		return nil, c.fail(provider, fmt.Errorf("%s: no secret store is mounted, so secret_ref %q cannot be read",
			ReasonProviderSecretUnresolved, declared.SecretRef))
	}
	kvPath, err := secretRefToKVPath(declared.SecretRef)
	if err != nil {
		return nil, c.fail(provider, fmt.Errorf("%s: %w", ReasonProviderSecretUnresolved, err))
	}
	secret, err := c.resolve(ctx, kvPath)
	if err != nil {
		// The reference is named, never its value or the store's error text beyond
		// what the store itself redacts: a credential never reaches this journal.
		return nil, c.fail(provider, fmt.Errorf("%s: secret_ref %q unreadable",
			ReasonProviderSecretUnresolved, declared.SecretRef))
	}
	inf, err := c.build(secret)
	if err != nil {
		// A refused base_url is a configuration fact, not a secret-store failure:
		// the store answered, and the value it answered with is unservable. Folding
		// that into provider_secret_unresolved erases the branch an operator has to
		// fix and misattributes the fault to the vault. The model's own refusal text
		// travels because it quotes no part of the configured value.
		if model.IsBaseURLConfigError(err) {
			return nil, c.fail(provider, contracts.NewReasoned(contracts.ErrConflict,
				model.BaseURLConfigReason(err),
				"execbridge: provider "+provider+" is misconfigured: "+err.Error()))
		}
		return nil, c.fail(provider, fmt.Errorf("%s: cannot build client for provider %q",
			ReasonProviderSecretUnresolved, provider))
	}
	c.mu.Lock()
	c.ready[provider] = inf
	c.dialed[provider] = endpointOf(inf)
	c.mu.Unlock()
	c.log.Info("execbridge: provider client resolved",
		"provider", provider, "secret_ref", declared.SecretRef,
		"dial_host_sha256", shortHash(endpointOf(inf)))
	return inf, nil
}

// fail records why a copy cannot be served and hands the same refusal back to the
// caller. The cached and the returned error are one value on purpose: an operator
// who reads the journal and a caller who gets the refusal must see the same reason,
// and the reason must survive to the terminal message (DEF-H25-1 discipline).
func (c *providerClients) fail(provider string, err error) error {
	c.mu.Lock()
	c.failed[provider] = err
	c.mu.Unlock()
	c.log.Error("execbridge: provider copy cannot be served",
		"provider", provider, "reason", err.Error())
	return err
}

// secretRefToKVPath turns a provider table secret_ref into a store path: either
// the path an environment variable names, or the path itself, mounted under the
// KV engine the platform's own default uses (secretstore.DefaultQwenPath).
func secretRefToKVPath(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", errors.New("empty secret_ref")
	}
	if envRefPattern.MatchString(ref) {
		v := strings.TrimSpace(os.Getenv(ref))
		if v == "" {
			return "", fmt.Errorf("environment variable %s named by secret_ref is unset", ref)
		}
		ref = v
	}
	if !strings.Contains(ref, "/data/") {
		ref = "secret/data/" + strings.TrimPrefix(ref, "/")
	}
	return ref, nil
}

// endpointOf reports the host:port a client dials, for the operator journal and
// for reconciliation against the provider table. Empty when the client does not
// say. Credentials are never part of it: only the URL host.
func endpointOf(inf model.Inference) string {
	type endpointReporter interface{ Endpoint() string }
	if e, ok := inf.(endpointReporter); ok {
		return e.Endpoint()
	}
	return ""
}

// shortHash keeps an endpoint comparable across log lines without publishing the
// upstream address in a plane whose traces must stay content-free.
func shortHash(v string) string {
	if v == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.ToLower(v)))
	return hex.EncodeToString(sum[:])[:12]
}

// vaultSecretResolver is the production resolver: the same scoped Vault handle the
// single-provider wiring uses, reading a caller-named path.
func vaultSecretResolver() (SecretResolver, error) {
	client, err := secretstore.NewFromEnv()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, kvPath string) (secretstore.QwenSecret, error) {
		return client.ReadQwen(ctx, kvPath)
	}, nil
}

// productionInferenceFactory builds the real client: egress pinned to the host in
// the secret, POST chat/completions only.
func productionInferenceFactory() inferenceFactory {
	return func(secret secretstore.QwenSecret) (model.Inference, error) {
		return model.NewQwenClient(context.Background(), secret)
	}
}

// resolverKind names, for the mount banner, whether copies other than the
// configured one can be resolved at all.
func resolverKind(r SecretResolver) string {
	if r == nil {
		return "none-configured-copy-only"
	}
	return "secret-store"
}
