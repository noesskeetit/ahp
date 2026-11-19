package stand

// These tests need no database: they pin the safety surface of the public stand
// - which database it is willing to migrate, what it prints about it, and how
// its flags interact with an environment a harness already configured. That
// surface is the only thing standing between "run the migrations" and "run the
// migrations against somebody else's database", so it is tested directly.

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sentinelDSN = "postgres://SECRETUSER:SECRET%2FPass@127.0.0.1:5432/ahp_hive_state_owner?sslmode=require"

// clearDSNSources removes every variable that could carry a real database
// credential into a test, so a failure message can never print a live DSN.
// t.Setenv also restores them afterwards.
func clearDSNSources(t *testing.T) {
	t.Helper()
	for _, key := range []string{"AHP_STATE_DSN", "AHP_TEST_DATABASE_URL", "DATABASE_URL",
		"AHP_DATABASE_URL"} {
		t.Setenv(key, "")
	}
}

func TestDatabaseNameHandlesBothDSNForms(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"url with an encoded slash in the password", sentinelDSN, "ahp_hive_state_owner"},
		{"url without parameters", "postgres://u:p@h:5432/ahp_hive_state_owner", "ahp_hive_state_owner"},
		{"keyword/value form", "host=127.0.0.1 port=5432 dbname=ahp_hive_state_owner user=u", "ahp_hive_state_owner"},
		{"no database in the url", "postgres://u:p@h:5432", ""},
		{"keyword/value without dbname", "host=127.0.0.1 user=u", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := databaseName(tc.dsn); got != tc.want {
				t.Fatalf("databaseName(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}

func TestGuardDatabaseRefusesForeignAndUnnamedTargets(t *testing.T) {
	if err := guardDatabase(sentinelDSN, "ahp_hive_state_owner"); err != nil {
		t.Fatalf("own database refused: %v", err)
	}
	err := guardDatabase(sentinelDSN, "someone_elses_db")
	if err == nil {
		t.Fatal("a foreign database was accepted")
	}
	if !strings.Contains(err.Error(), "ahp_hive_state_owner") ||
		!strings.Contains(err.Error(), "someone_elses_db") {
		t.Fatalf("refusal must name both target and allowlist, got: %v", err)
	}
	if strings.Contains(err.Error(), "SECRETUSER") || strings.Contains(err.Error(), "SECRET%2FPass") ||
		strings.Contains(err.Error(), "SECRET/Pass") {
		t.Fatalf("the refusal leaked a credential: %v", err)
	}
	// A target that cannot be named is never migrated, whatever the allowlist.
	if err := guardDatabase("host=127.0.0.1 user=u", "ahp_hive_state_owner"); err == nil {
		t.Fatal("an unnamed target was accepted")
	}
	// The historical entry point passes an empty allowlist and keeps its old
	// behaviour: no guard at all.
	if err := guardDatabase(sentinelDSN, ""); err != nil {
		t.Fatalf("empty allowlist must disable the guard, got %v", err)
	}
}

func TestGuardDatabasePrintsOnlyTheDatabaseName(t *testing.T) {
	out := captureStdout(t, func() {
		if err := guardDatabase(sentinelDSN, "ahp_hive_state_owner"); err != nil {
			t.Fatalf("%v", err)
		}
	})
	if !strings.Contains(out, "ahp_hive_state_owner") {
		t.Fatalf("the target database must be reported, got %q", out)
	}
	for _, secret := range []string{"SECRETUSER", "SECRET%2FPass", "SECRET/Pass", "postgres://"} {
		if strings.Contains(out, secret) {
			t.Fatalf("stdout leaked %q: %q", secret, out)
		}
	}
}

func TestFlagsNeverOverwriteAHarnessEnvironment(t *testing.T) {
	clearDSNSources(t)
	t.Setenv("AHP_STATE_TENANT", "tenant-from-harness")
	t.Setenv("AHP_STATE_DEPLOYMENT", "dep-from-harness")
	t.Setenv("AHP_STATE_SUBJECT", "subject-from-harness")
	t.Setenv("AHP_STATE_DB_ALLOWLIST", "db-from-harness")
	t.Setenv("AHP_STATE_HANDSHAKE", "/tmp/hs-from-harness.json")

	// No flags at all: everything the harness set survives, including the
	// allowlist, and the run-scoped fallbacks do not replace real scopes.
	if err := setupFlags(nil, DefaultDBAllowlist); err != nil {
		t.Fatalf("env-only configuration must be accepted: %v", err)
	}
	for key, want := range map[string]string{
		"AHP_STATE_TENANT":       "tenant-from-harness",
		"AHP_STATE_DEPLOYMENT":   "dep-from-harness",
		"AHP_STATE_SUBJECT":      "subject-from-harness",
		"AHP_STATE_DB_ALLOWLIST": "db-from-harness",
		"AHP_STATE_HANDSHAKE":    "/tmp/hs-from-harness.json",
	} {
		if got := os.Getenv(key); got != want {
			t.Fatalf("%s = %q, want %q (flag defaults must not clobber the env)", key, got, want)
		}
	}

	// An explicitly passed flag does win over the environment.
	if err := setupFlags([]string{"-tenant", "tenant-from-flag", "-db-allowlist", "db-from-flag"}, DefaultDBAllowlist); err != nil {
		t.Fatalf("%v", err)
	}
	if got := os.Getenv("AHP_STATE_TENANT"); got != "tenant-from-flag" {
		t.Fatalf("-tenant was ignored: %q", got)
	}
	if got := os.Getenv("AHP_STATE_DB_ALLOWLIST"); got != "db-from-flag" {
		t.Fatalf("-db-allowlist was ignored: %q", got)
	}
}

func TestStandRequiresAHandshakeAndDefaultsAreRunScoped(t *testing.T) {
	for _, key := range []string{"AHP_STATE_HANDSHAKE", "AHP_STATE_DB_ALLOWLIST"} {
		t.Setenv(key, "")
	}
	clearDSNSources(t)
	t.Setenv("AHP_STATE_TENANT", "")
	t.Setenv("AHP_STATE_DEPLOYMENT", "")
	t.Setenv("AHP_STATE_SUBJECT", "")
	err := setupFlags(nil, DefaultDBAllowlist)
	if err == nil || !strings.Contains(err.Error(), "-handshake") {
		t.Fatalf("a stand without a handshake file must be refused, got %v", err)
	}
	// The handshake is also acceptable through the environment only.
	t.Setenv("AHP_STATE_HANDSHAKE", "/tmp/hs-env.json")
	if err := setupFlags(nil, DefaultDBAllowlist); err != nil {
		t.Fatalf("%v", err)
	}
	if os.Getenv("AHP_STATE_TENANT") == "" || os.Getenv("AHP_STATE_TENANT") != os.Getenv("AHP_STATE_DEPLOYMENT")[2:] {
		t.Fatalf("run-scoped defaults are inconsistent: %q / %q",
			os.Getenv("AHP_STATE_TENANT"), os.Getenv("AHP_STATE_DEPLOYMENT"))
	}
	if got := os.Getenv("AHP_STATE_DB_ALLOWLIST"); got != DefaultDBAllowlist {
		t.Fatalf("the public entry point must default to the lane allowlist, got %q", got)
	}
	// -help is a request for usage, not a failure: the command exits 0 on it.
	if err := setupFlags([]string{"-help"}, DefaultDBAllowlist); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-help must report flag.ErrHelp, got %v", err)
	}
}

func TestEnvFileNeverOverridesExistingValuesAndBridgesTheDSN(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lane.env")
	body := "# lane env\nexport AHP_TEST_DATABASE_URL='" + sentinelDSN + "'\nAHP_STATE_SUBJECT=user-from-env-file\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	// Nothing real may be inherited: the env file has to be the only source, and
	// a failure below must never be able to print a live credential.
	clearDSNSources(t)
	t.Setenv("AHP_STATE_DB_ALLOWLIST", "")
	t.Setenv("AHP_STATE_SUBJECT", "user-from-real-environment")
	if err := setupFlags([]string{"-env", path, "-handshake", filepath.Join(dir, "hs.json")}, DefaultDBAllowlist); err != nil {
		t.Fatalf("%v", err)
	}
	// The value is compared, never printed: a DSN must not reach a test log.
	if os.Getenv("AHP_STATE_DSN") != sentinelDSN {
		t.Fatal("AHP_TEST_DATABASE_URL from the env file was not bridged into AHP_STATE_DSN")
	}
	if got := os.Getenv("AHP_STATE_SUBJECT"); got != "user-from-real-environment" {
		t.Fatalf("the env file overwrote a real environment value: %q", got)
	}
	// Scope fallbacks are per variable: a caller that mounted a tenant and a
	// subject keeps both, and only the missing deployment is derived.
	t.Setenv("AHP_STATE_TENANT", "tenant-mounted-by-caller")
	t.Setenv("AHP_STATE_DEPLOYMENT", "")
	if err := setupFlags([]string{"-handshake", filepath.Join(dir, "hs.json")}, DefaultDBAllowlist); err != nil {
		t.Fatalf("%v", err)
	}
	if got := os.Getenv("AHP_STATE_TENANT"); got != "tenant-mounted-by-caller" {
		t.Fatalf("the run-scoped fallback replaced a mounted tenant: %q", got)
	}
	if got := os.Getenv("AHP_STATE_SUBJECT"); got != "user-from-real-environment" {
		t.Fatalf("the run-scoped fallback replaced a mounted subject: %q", got)
	}
	if got := os.Getenv("AHP_STATE_DEPLOYMENT"); got != "d-tenant-mounted-by-caller" {
		t.Fatalf("the missing deployment must be derived from the tenant, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "hs.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("setupFlags must not create the handshake file, the stand does")
	}
	// A DSN that cannot be named is refused before anything is opened.
	t.Setenv("AHP_STATE_DSN", "host=127.0.0.1 user=u")
	if err := guardDatabase(os.Getenv("AHP_STATE_DSN"), os.Getenv("AHP_STATE_DB_ALLOWLIST")); err == nil {
		t.Fatal("an unnamed target must be refused")
	}
}

// captureStdout runs fn with os.Stdout redirected to a pipe.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var b strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			b.Write(buf[:n])
			if err != nil {
				break
			}
		}
		done <- b.String()
	}()
	defer func() {
		os.Stdout = saved
	}()
	fn()
	_ = w.Close()
	return <-done
}
