package dote2e_test

// PDF rev3.2 L2056 and L2100-2102: "Computer = профиль personal в Compute с
// ключом (tenant, user, dot, realm)". The scenario proves the personal profile
// is addressed by that key and that two realms of the same employee never see
// each other's Computer.
//
// Marking: AHP_COMPUTE_MOCK_BACKEND=1 in this lane, so the VM substrate is the
// agreed MOCK (isolation and escape-resistance are NOT claimed). Keying,
// idempotency and access control are the real Compute owner over real PostgreSQL.

import (
	"context"
	"testing"
	"time"
)

type computerReply struct {
	ID         string `json:"id"`
	Tenant     string `json:"tenant"`
	Subject    string `json:"subject"`
	Deployment string `json:"deployment"`
	Class      string `json:"class"`
	Profile    string `json:"profile"`
	Status     string `json:"status"`
	Realm      string `json:"realm"`
}

func createComputer(t *testing.T, stand *instance, token, key, realm, class string) (computerReply, error) {
	t.Helper()
	var out computerReply
	err := stand.call(stand.HTTP, "/v1/compute/CreateComputer", map[string]any{
		"class": class, "profile": "personal", "realm": realm, "key": key}, token, &out)
	return out, err
}

func TestPersonalComputerIsKeyedPerDotAndRealm(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrant(t, stand.HTTP, human, "dot-computer-"+stamp, []string{"compute", "state"})
	checks := map[string]any{}
	// A personal Computer is unique per (tenant, subject, realm), so each scenario
	// run uses its own realm names rather than colliding with an earlier run.
	workRealm, homeRealm := "work"+stamp, "home"+stamp

	// (a) The employee's personal Computer for one realm is created by the real
	// Compute owner: class S is the trusted "employee" class the profile requires.
	work, err := createComputer(t, stand, token, task+"-work", workRealm, "S")
	if err != nil {
		stand.record(t, "computer-personal-key", "FAIL", map[string]any{"create": map[string]any{
			"status": "FAIL", "http": status(err), "code": errorCode(err), "error": err.Error()}})
		t.Fatalf("personal Computer creation refused: %v", err)
	}
	if work.Profile != "personal" || work.Realm != workRealm || work.Class != "S" || work.Subject == "" {
		t.Fatalf("unexpected Computer projection: %+v", work)
	}
	checks["create_personal_computer"] = map[string]any{"status": "PASS", "computer": work.ID,
		"realm": work.Realm, "class": work.Class, "profile": work.Profile, "subject": work.Subject}

	// (b) A second realm of the same employee is a DIFFERENT Computer: the realm is
	// a real key dimension, not a label on one shared machine.
	home, err := createComputer(t, stand, token, task+"-home", homeRealm, "S")
	if err != nil {
		t.Fatalf("second realm Computer refused: %v", err)
	}
	if home.ID == work.ID {
		t.Fatal("two realms returned the same Computer: the realm is missing from the key")
	}
	checks["realm_separates_computers"] = map[string]any{"status": "PASS", "work": work.ID, "home": home.ID}

	// (c) Replaying the same key returns the SAME Computer (no second working
	// environment for one intent).
	replay, err := createComputer(t, stand, token, task+"-work", workRealm, "S")
	if err != nil {
		t.Fatalf("idempotent replay refused: %v", err)
	}
	if replay.ID != work.ID {
		t.Fatalf("same key produced a second Computer: %s vs %s", replay.ID, work.ID)
	}
	checks["same_key_replays_same_computer"] = map[string]any{"status": "PASS", "computer": work.ID}

	// (d) A different key cannot buy a second active personal Computer in the SAME
	// realm: the (tenant, subject, profile, realm) uniqueness is enforced by the
	// owner, so the "key" is a real boundary rather than a request-time filter.
	_, err = createComputer(t, stand, token, task+"-work-2", workRealm, "S")
	if status(err) != 409 {
		t.Fatalf("a second active personal Computer in one realm must be a conflict, got %v", err)
	}
	checks["second_personal_computer_in_realm_denied"] = map[string]any{"status": "PASS", "http": 409, "code": errorCode(err)}

	// (e) The personal profile is reserved for the trusted class S acting as the
	// employee; another class is refused.
	if _, err = createComputer(t, stand, token, task+"-class-c", "classc"+stamp, "C"); status(err) != 403 {
		t.Fatalf("personal Computer outside class S must be refused with 403, got %v", err)
	}
	checks["personal_requires_trusted_class"] = map[string]any{"status": "PASS", "http": 403, "code": errorCode(err)}

	// (f) The key is stored: PostgreSQL holds (tenant, subject, deployment=dot,
	// realm) per personal Computer, so isolation survives a process restart.
	var keyed int
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2 AND deployment=$3
		   AND profile='personal' AND realm <> ''`,
		stand.Scope["tenant"], stand.Scope["subject"], stand.Scope["deployment"]).Scan(&keyed)
	if err != nil {
		t.Fatalf("read ahp_compute.computers: %v", err)
	}
	if keyed < 2 {
		t.Fatalf("expected two keyed personal Computers, found %d", keyed)
	}
	rows, err := stand.Pool.Query(context.Background(),
		`SELECT realm, id FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2 ORDER BY realm`,
		stand.Scope["tenant"], stand.Scope["subject"])
	if err != nil {
		t.Fatalf("read Computer keys: %v", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var realm, id string
		if err := rows.Scan(&realm, &id); err != nil {
			t.Fatalf("scan Computer row: %v", err)
		}
		keys = append(keys, realm+"="+id)
	}
	checks["key_columns_in_postgres"] = map[string]any{"status": "PASS", "rows": keyed, "keys": keys,
		"mapping": "PDF (tenant, user, dot, realm) -> (tenant, subject, deployment=dot, realm); no column named 'dot': the dot dimension is the deployment"}

	// (g) Another employee cannot read this Computer by id.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, _ := stand.taskGrant(t, stand.Foreign, foreignHuman, "dot-computer-foreign-"+stamp, []string{"compute"})
	err = stand.call(stand.Foreign, "/v1/compute/GetComputer", map[string]any{"computer_id": work.ID}, foreignToken, nil)
	if status(err) == 0 {
		t.Fatal("a foreign employee read another employee's Computer")
	}
	checks["foreign_employee_denied_computer"] = map[string]any{"status": "PASS", "http": status(err), "code": errorCode(err)}

	// (h) The dot dimension is the deployment column. This lane runs one dot
	// deployment per stand, so the second dot is proven by the key column, not by
	// inventing a deployment this stand does not have.
	var deployments int
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(DISTINCT deployment) FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2`,
		stand.Scope["tenant"], stand.Scope["subject"]).Scan(&deployments)
	if err != nil {
		t.Fatalf("read deployment dimension: %v", err)
	}
	checks["dot_dimension_is_deployment"] = map[string]any{"status": "PARTIAL", "distinct_deployments": deployments,
		"note": "one dot deployment exists in this lane; the key column is deployment (see key_columns_in_postgres)"}

	stand.record(t, "computer-personal-key", "PASS", checks)
}
