package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// RuntimeBinding is installed by the operator after inspecting the running
// container image. It is not accepted by the tenant HTTP API. The digest binds
// the complete registry manifest; Image separately identifies the OCI artifact.
// Rebinding a digest to a different image/endpoint is prohibited: publish a new
// version instead. Old bindings remain available to tasks pinned before rollback.
type RuntimeBinding struct {
	Deployment  string `json:"deployment"`
	Tenant      string `json:"tenant"`
	Environment string `json:"environment"`
	AgentID     string `json:"agent_id"`
	Digest      string `json:"digest"`
	Image       string `json:"image"`
	Endpoint    string `json:"endpoint"`
	ReadyURL    string `json:"ready_url"`
	// Attestation is the CI-signed provenance document (a DSSE envelope) that
	// binds this image's digest to its SBOM and verdicts. When the operator has
	// installed supply-chain trust roots it is verified cryptographically; the
	// empty string it once was is exactly the defect this closes.
	Attestation string `json:"attestation"`
	// SBOM is the software bill of materials the attestation signature binds;
	// it must hash to the digest the publisher signed, or admission is refused.
	SBOM []byte `json:"sbom,omitempty"`
}

const runtimeDDL = `CREATE TABLE IF NOT EXISTS ahp_registry.runtimes (
 tenant text NOT NULL, environment text NOT NULL, agent_id text NOT NULL,
 digest text NOT NULL, binding jsonb NOT NULL,
 PRIMARY KEY (tenant, environment, agent_id, digest)
);`

func runtimeURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, invalid("runtime URL must have a host and no credentials/query/fragment")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1")) {
		return nil, invalid("runtime requires HTTPS (HTTP allowed only on loopback for local containers)")
	}
	return u, nil
}

func (b RuntimeBinding) Validate() error {
	for name, value := range map[string]string{"tenant": b.Tenant, "deployment": b.Deployment, "environment": b.Environment, "agent": b.AgentID} {
		if err := requireID(name, value); err != nil {
			return err
		}
	}
	if err := ValidateVersionDigest(b.Digest); err != nil {
		return err
	}
	if !imageRefPattern.MatchString(b.Image) {
		return invalid("runtime image must be an immutable repository@sha256 reference")
	}
	// Shape only: a document must be present and bounded. Presence is NOT trust
	// -- RegisterRuntime decides trust, by verifying the document against the
	// operator's trust roots. Validate deliberately does not claim otherwise.
	if strings.TrimSpace(b.Attestation) == "" || len(b.Attestation) > 1<<20 {
		return invalid("operator image attestation document required (max 1 MiB)")
	}
	u, err := runtimeURL(b.Endpoint)
	if err != nil {
		return err
	}
	r, err := runtimeURL(b.ReadyURL)
	if err != nil {
		return err
	}
	if u.Scheme != r.Scheme || u.Host != r.Host {
		return invalid("runtime readiness must have same origin as endpoint")
	}
	return nil
}

// RegisterRuntime is an operator-only write API. Calling it does not itself
// claim Kubernetes deployment or readiness; each dispatch verifies readiness.
func (s *Store) RegisterRuntime(ctx context.Context, b RuntimeBinding) error {
	if err := b.Validate(); err != nil {
		return err
	}
	// A runtime binding is an operator claim about an immutable image, and the
	// image's provenance is what the pipeline signs ("SBOM и подпись", rev 3.2
	// c. 41). So registering one is an admission decision, on EVERY level:
	//
	//   - trust roots installed: the binding's attestation must verify against
	//     those roots and bind this exact image digest, version and SBOM;
	//   - no roots and no explicit synthetic election: refuse. Accepting a
	//     binding because its attestation string is merely non-empty was the
	//     defect this closes -- an unchecked document proves nothing, and the
	//     Registry will not route a tenant's workload on that basis.
	//   - an explicitly elected SYNTHETIC stand: accept, with the mode left
	//     observable through SupplyChainMode so no evidence can read it as
	//     verified.
	if !s.supplyChainEnforced() && s.SupplyChainMode() != SupplyChainModeSynthetic {
		return contracts.NewError(contracts.ErrUnavailable,
			"runtime registration: no operator supply-chain trust roots installed (fail closed) -- install AHP_REGISTRY_TRUST_ROOTS_PEM or elect the synthetic stand explicitly")
	}
	// The admission decision needs the stored version, and only an enforced
	// store may make it: a synthetic stand verifies nothing, so it must not
	// pretend to have admitted anything either.
	if s.supplyChainEnforced() {
		version, err := s.findVersionByAgentDigest(ctx, b.AgentID, b.Digest)
		if err != nil {
			return err
		}
		var att Attestation
		if err := json.Unmarshal([]byte(b.Attestation), &att); err != nil {
			return invalid("runtime attestation is not a valid signed attestation document")
		}
		if _, err := s.AdmitImage(ctx, AdmitImageInput{
			AgentID: b.AgentID, VersionID: version.ID, Image: b.Image, Attestation: att, SBOM: b.SBOM,
		}); err != nil {
			return err
		}
	}
	doc, err := json.Marshal(b)
	if err != nil {
		return err
	}
	var affected int64
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-deployment/"+b.Tenant+"/"+b.Deployment); err != nil {
			return err
		}
		var other bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_registry.runtimes WHERE tenant=$1 AND binding->>'deployment'=$2 AND (environment<>$3 OR agent_id<>$4 OR digest<>$5))`, b.Tenant, b.Deployment, b.Environment, b.AgentID, b.Digest).Scan(&other); err != nil {
			return err
		}
		if other {
			return contracts.NewError(contracts.ErrConflict, "runtime deployment must identify one immutable version")
		}
		tag, err := tx.Exec(ctx, `INSERT INTO ahp_registry.runtimes(tenant,environment,agent_id,digest,binding) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING`, b.Tenant, b.Environment, b.AgentID, b.Digest, doc)
		if err != nil {
			return err
		}
		affected = tag.RowsAffected()
		return nil
	})
	if err != nil {
		return storageError("register runtime", err)
	}
	if affected == 1 {
		return nil
	}
	old, err := s.ResolveRuntime(ctx, b.Tenant, b.Environment, b.AgentID, b.Digest)
	if err != nil {
		return err
	}
	// RuntimeBinding carries byte fields, so compare by canonical serialization
	// rather than struct equality; a re-registration must reproduce the binding
	// field-for-field or it is an attempt to mutate an immutable binding.
	oldDoc, _ := json.Marshal(old)
	if string(oldDoc) != string(doc) {
		return contracts.NewError(contracts.ErrConflict, "runtime binding is immutable")
	}
	return nil
}

func (s *Store) ResolveRuntime(ctx context.Context, tenant, environment, agentID, digest string) (RuntimeBinding, error) {
	var doc []byte
	err := s.pool.QueryRow(ctx, `SELECT binding FROM ahp_registry.runtimes WHERE tenant=$1 AND environment=$2 AND agent_id=$3 AND digest=$4`, tenant, environment, agentID, digest).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeBinding{}, contracts.NewError(contracts.ErrUnavailable, "selected immutable version has no registered runtime")
	}
	if err != nil {
		return RuntimeBinding{}, storageError("resolve runtime", err)
	}
	var b RuntimeBinding
	if err = json.Unmarshal(doc, &b); err != nil {
		return b, storageError("decode runtime", err)
	}
	return b, b.Validate()
}

// NewRoutedA2AInvokeDispatcher never accepts a caller-controlled endpoint. A
// supplied client may carry workload mTLS credentials. Redirects are disabled
// to prevent forwarding those credentials or the caller bearer elsewhere.
func NewRoutedA2AInvokeDispatcher(store *Store, client *http.Client) (*A2AInvokeDispatcher, error) {
	if store == nil {
		return nil, invalid("invoke requires registry runtime resolver")
	}
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	copy := *client
	if copy.Timeout == 0 || copy.Timeout > 60*time.Second {
		copy.Timeout = 60 * time.Second
	}
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &A2AInvokeDispatcher{store: store, client: &copy}, nil
}

// Routed creates the secure TLS client using the existing certificate loader,
// then replaces the legacy common URL with the immutable runtime resolver.
func NewTLSRoutedA2AInvokeDispatcher(store *Store, cert, key, ca string) (*A2AInvokeDispatcher, error) {
	old, err := NewA2AInvokeDispatcher("https://localhost", cert, key, ca)
	if err != nil {
		return nil, err
	}
	return NewRoutedA2AInvokeDispatcher(store, old.client)
}

func (d *A2AInvokeDispatcher) ready(ctx context.Context, b RuntimeBinding) error {
	return probeRuntimeReady(ctx, d.client, b)
}

// probeRuntimeReady is the single readiness probe the Registry trusts before it
// lets a task reach an immutable runtime. Both the per-invocation dispatcher and
// the promote-time gate use it, so "ready" means the same thing everywhere: the
// runtime answers on its bound ReadyURL, reports ready, and names exactly the
// agent, version digest and image the binding pins. Anything else -- unreachable,
// not ready, or a different identity -- is refused.
func probeRuntimeReady(ctx context.Context, client *http.Client, b RuntimeBinding) error {
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probe, http.MethodGet, b.ReadyURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "runtime readiness probe failed")
	}
	defer resp.Body.Close()
	var identity struct {
		AgentID string `json:"agent_id"`
		Digest  string `json:"version_digest"`
		Image   string `json:"image"`
		Ready   bool   `json:"ready"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 16384)).Decode(&identity) != nil || !identity.Ready || identity.AgentID != b.AgentID || identity.Digest != b.Digest || identity.Image != b.Image {
		return contracts.NewError(contracts.ErrUnavailable, "runtime readiness identity does not match immutable binding")
	}
	return nil
}

// AliasTaskPin copies an already captured config/version snapshot to the task ID
// returned by the runtime. It never re-resolves the current installation.
func (s *Store) AliasTaskPin(ctx context.Context, tenant, installationID, source, target string) error {
	for name, value := range map[string]string{"tenant": tenant, "installation": installationID, "source task": source, "runtime task": target} {
		if err := requireID(name, value); err != nil {
			return err
		}
	}
	if source == target {
		return nil
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		// A repeated alias can arrive after the caller lost its first ACK. Only
		// this exact original grant may replay an already-persisted alias.
		var existing string
		err := tx.QueryRow(ctx, `SELECT grant_task_id FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3`, tenant, installationID, target).Scan(&existing)
		if err == nil {
			if existing == source {
				return nil
			}
			return contracts.NewError(contracts.ErrConflict, "runtime task ID belongs to another invocation")
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return storageError("read invocation alias", err)
		}
		var owner string
		if err := tx.QueryRow(ctx, `SELECT subject FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3`, tenant, installationID, source).Scan(&owner); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrConflict, "invocation reservation is missing")
			}
			return storageError("lock invoked task", err)
		}
		if err := checkSubjectWritable(ctx, tx, tenant, owner); err != nil {
			return err
		}
		// Erasure takes the subject gate before task rows. Use that same order
		// and recheck the owner while locking the reservation after the gate.
		var lockedOwner string
		if err := tx.QueryRow(ctx, `SELECT subject FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3 AND subject=$4 FOR UPDATE`, tenant, installationID, source, owner).Scan(&lockedOwner); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrConflict, "invocation reservation changed before alias publication")
			}
			return storageError("lock invocation reservation", err)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO ahp_registry.task_pins(task_id,tenant,installation_id,version_id,config_data,config_revision,subject,grant_task_id)
 SELECT $4,tenant,installation_id,version_id,config_data,config_revision,subject,COALESCE(NULLIF(grant_task_id,''),task_id)
 FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3 ON CONFLICT DO NOTHING`, tenant, installationID, source, target)
		if err != nil {
			return storageError("bind invoked task", err)
		}
		if tag.RowsAffected() != 1 {
			return contracts.NewError(contracts.ErrConflict, "runtime task ID already exists")
		}
		_, err = tx.Exec(ctx, `DELETE FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3`, tenant, installationID, source)
		return err
	})
}

// invocationGrant resolves a native task to the original Identity grant, under
// the pin's authenticated owner. No task token or runtime state is persisted.
func (s *Store) invocationGrant(ctx context.Context, scope contracts.Scope, installation, remote string) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	var grant string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `SELECT COALESCE(NULLIF(grant_task_id,''),task_id) FROM ahp_registry.task_pins WHERE tenant=$1 AND installation_id=$2 AND task_id=$3 AND subject=$4`, scope.Tenant, installation, remote, scope.Subject).Scan(&grant)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "invoked task not found")
		}
		if err != nil {
			return storageError("read invocation grant", err)
		}
		return nil
	})
	return grant, err
}

// RuntimeTokenIssuer creates a child grant for the selected deployment. The
// caller bearer is input to authorization only and is never sent to the agent.
type RuntimeTokenIssuer interface {
	IssueRuntimeToken(context.Context, InvokeRequest, RuntimeBinding) (string, error)
}

func (d *A2AInvokeDispatcher) SetRuntimeTokenIssuer(issuer RuntimeTokenIssuer) { d.issuer = issuer }

// InstalledTarget resolves an operator runtime deployment back to the current
// tenant installation and its immutable manifest/capability ceiling.
type InstalledTarget struct {
	Installation Installation
	Version      Version
}

func (s *Store) FindInstalledTarget(ctx context.Context, tenant, deployment string) (InstalledTarget, error) {
	if err := requireID("tenant", tenant); err != nil {
		return InstalledTarget{}, err
	}
	if err := requireID("deployment", deployment); err != nil {
		return InstalledTarget{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT i.id FROM ahp_registry.installations i JOIN ahp_registry.versions v ON v.id=i.version_id
 JOIN ahp_registry.runtimes r ON r.tenant=i.tenant AND r.environment=i.environment AND r.agent_id=i.agent_id AND r.digest=v.digest
 WHERE i.tenant=$1 AND r.binding->>'deployment'=$2 LIMIT 2`, tenant, deployment)
	if err != nil {
		return InstalledTarget{}, storageError("resolve installed runtime", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return InstalledTarget{}, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return InstalledTarget{}, err
	}
	if len(ids) == 0 {
		return InstalledTarget{}, contracts.NewError(contracts.ErrNotFound, "runtime deployment is not an active installation")
	}
	if len(ids) != 1 {
		return InstalledTarget{}, contracts.NewError(contracts.ErrConflict, "runtime deployment is ambiguous")
	}
	var out InstalledTarget
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		inst, err := lockInstallation(ctx, tx, tenant, ids[0])
		if err != nil {
			return err
		}
		v, err := getVersion(ctx, tx, inst.VersionID)
		if err != nil {
			return err
		}
		var bound string
		if err = tx.QueryRow(ctx, `SELECT binding->>'deployment' FROM ahp_registry.runtimes WHERE tenant=$1 AND environment=$2 AND agent_id=$3 AND digest=$4`, tenant, inst.Environment, inst.AgentID, v.Digest).Scan(&bound); err != nil {
			return storageError("runtime changed during resolution", err)
		}
		if bound != deployment {
			return contracts.NewError(contracts.ErrConflict, "runtime changed during resolution")
		}
		out = InstalledTarget{Installation: inst.Installation, Version: v}
		return nil
	})
	return out, err
}
