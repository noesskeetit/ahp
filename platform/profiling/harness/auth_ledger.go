package harness

// AuthorizationLedger records, in every artifact, exactly how each plane and each
// bootstrapped session was authorized, so acceptance never infers it and never
// sees "the last session won". It used to be a single composition overwritten by
// every session()/worker on the same report; a multi-worker ladder then reported
// only its last session. The ledger keeps one entry per session and one entry per
// plane, and reports the live rotation count of each session's credential at write
// time.
type AuthorizationLedger struct {
	HarnessSelfSigned bool                   `json:"harness_self_signed"`
	Planes            []PlaneAuthorization   `json:"planes"`
	Sessions          []SessionAuthorization `json:"sessions"`

	planeSeen map[string]bool
}

// PlaneAuthorization is the per-plane mechanism: how the account is provisioned,
// who signs the token, through which exchange, and which capability the plane
// carries. It is static per plane (state / model) and recorded once.
type PlaneAuthorization struct {
	Plane            string   `json:"plane"`
	ProvisioningSeam string   `json:"provisioning_seam"`
	TokenIssuer      string   `json:"token_issuer"`
	ExchangeURL      string   `json:"exchange_url"`
	Capabilities     []string `json:"capabilities"`
}

// SessionAuthorization is one bootstrapped session's authorization: the plane it
// belongs to, the task its token is bound to, the exchanger deployment, its
// capabilities, and how many times its credential was rotated off-window.
type SessionAuthorization struct {
	Tag                              string   `json:"tag"`
	Plane                            string   `json:"plane"`
	Task                             string   `json:"task"`
	ExchangerDeployment              string   `json:"exchanger_deployment"`
	Capabilities                     []string `json:"capabilities"`
	TokenRotations                   int      `json:"token_rotations"`
	RotationError                    string   `json:"rotation_error,omitempty"`
	RotationOutsideMeasurementWindow bool     `json:"rotation_outside_measurement_window"`

	rotations func() int64
	rotErr    func() string
	outside   func() bool
}

// NewAuthorizationLedger returns an empty ledger. Harness authorization is never
// self-signed, so HarnessSelfSigned starts and stays false.
func NewAuthorizationLedger() *AuthorizationLedger {
	return &AuthorizationLedger{HarnessSelfSigned: false, planeSeen: map[string]bool{}}
}

// RecordPlane records a plane's mechanism once (idempotent per plane name).
func (l *AuthorizationLedger) RecordPlane(p PlaneAuthorization) {
	if l.planeSeen == nil {
		l.planeSeen = map[string]bool{}
	}
	if l.planeSeen[p.Plane] {
		return
	}
	l.planeSeen[p.Plane] = true
	l.Planes = append(l.Planes, p)
}

// RecordSession appends one session's authorization and keeps the live accessors
// for its credential so materialize() can read the true rotation state at write
// time (the count grows as a long phase runs, so it cannot be fixed at record time).
func (l *AuthorizationLedger) RecordSession(s SessionAuthorization, rotations func() int64, rotErr func() string, outside func() bool) {
	s.rotations, s.rotErr, s.outside = rotations, rotErr, outside
	l.Sessions = append(l.Sessions, s)
}

// materialize fills each session's exported rotation fields from its live accessors.
// WriteOutputs calls it just before marshalling so the published artifact reflects
// the whole run, not the instant the session was created.
func (l *AuthorizationLedger) materialize() {
	for i := range l.Sessions {
		if l.Sessions[i].rotations != nil {
			l.Sessions[i].TokenRotations = int(l.Sessions[i].rotations())
		}
		if l.Sessions[i].rotErr != nil {
			l.Sessions[i].RotationError = l.Sessions[i].rotErr()
		}
		if l.Sessions[i].outside != nil {
			l.Sessions[i].RotationOutsideMeasurementWindow = l.Sessions[i].outside()
		}
	}
}

// planeMechanism returns the static mechanism description shared by every plane;
// the capability differs per plane and is set by the caller.
func PlaneMechanism(plane string, capabilities []string) PlaneAuthorization {
	return PlaneAuthorization{
		Plane:            plane,
		ProvisioningSeam: "identity.SeedBrowserUser via AHP_TEST_DATABASE_URL (documented one-shot operator role; product reference cmd/ahp/provision.go AHP_PROVISION_USERS_JSON)",
		TokenIssuer:      "platform identity signer reached through /v1/identity/control/exchange; the harness holds no task-signing seed and self-signs nothing",
		ExchangeURL:      "POST /v1/identity/control/exchange (mTLS; caller's deployment allowlisted as exchanger)",
		Capabilities:     capabilities,
	}
}
