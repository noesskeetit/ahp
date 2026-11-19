package stand

// Command-line surface of the public State stand. The environment stays the
// source of truth (the harness scripts and CI set it directly), so a flag only
// writes into the environment before the run reads it - one configuration
// path, no two ways to configure the same process. Flags never overwrite the
// environment with their defaults: only flags the caller actually passed are
// published, so an already-configured harness keeps its own values.
//
// A public tool must also be a safe one: booting a State server means running
// migrations against whatever database it is pointed at, so the target database
// name is checked against an allowlist before anything is opened, and only the
// database NAME is ever printed - never the DSN, never a credential.

import (
	"bufio"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// DefaultDBAllowlist is the database this lane may migrate. The compatibility
// entry point tests/state/serve passes an empty default so that its existing
// callers keep their previous behaviour.
const DefaultDBAllowlist = "ahp_hive_state_owner"

// Prefix labels every line this tool prints. The public command uses
// "state-stand"; the compatibility entry point keeps the historical
// "state-serve" so the logs of already-written harnesses do not change.
var Prefix = "state-stand"

// loadEnvFile applies KEY=VALUE lines of a lane env file without ever printing
// a value. Existing environment wins, so a caller can override one variable.
func loadEnvFile(path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read env file: %w", err)
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" || value == "" {
			continue
		}
		if cur := os.Getenv(key); cur == "" {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// databaseName extracts only the database name from a DSN: the credential part
// must never reach stdout, a log or an evidence file. Both accepted DSN forms
// are handled - the connection URL and the keyword/value form. Anything whose
// target cannot be named returns "", and the guard then refuses to run: an
// unnamed target is never migrated on the assumption that it is the right one.
func databaseName(dsn string) string {
	s := strings.TrimSpace(dsn)
	if s == "" {
		return ""
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil || u.Host == "" {
			return ""
		}
		// The database is the whole path: parsing instead of scanning for the
		// last '/' keeps a password that contains a slash from being mistaken
		// for part of the path.
		name := strings.TrimPrefix(u.Path, "/")
		if name == "" {
			return ""
		}
		return name
	}
	// keyword/value form: "host=... dbname=ahp_hive_state_owner user=..."
	for _, field := range strings.Fields(s) {
		key, value, ok := strings.Cut(field, "=")
		if ok && strings.EqualFold(strings.TrimSpace(key), "dbname") {
			return strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	return ""
}

// guardDatabase refuses to migrate a database that is not this lane's own. An
// empty allowlist means the caller does not want the guard (the historical
// behaviour of tests/state/serve).
func guardDatabase(dsn, allowlist string) error {
	if strings.TrimSpace(allowlist) == "" {
		return nil
	}
	name := databaseName(dsn)
	if name == "" {
		return fmt.Errorf("cannot tell the target database name from the DSN")
	}
	for _, allowed := range strings.Split(allowlist, ",") {
		if strings.TrimSpace(allowed) == name {
			fmt.Printf("%s: target database = %s\n", Prefix, name)
			return nil
		}
	}
	return fmt.Errorf("refusing to run: database %q is not on the allowlist %q", name, allowlist)
}

// setupFlags parses the public flags and publishes them as the environment the
// stand reads. defaultAllowlist is used when neither -db-allowlist nor
// AHP_STATE_DB_ALLOWLIST was given.
func setupFlags(args []string, defaultAllowlist string) error {
	fs := flag.NewFlagSet(Prefix, flag.ContinueOnError)
	envFile := fs.String("env", "", "lane env file to load (e.g. private/root-acceptance-pg/state-owner.env)")
	dsn := fs.String("dsn", "", "isolated database DSN (overrides the env file)")
	handshake := fs.String("handshake", "", "path of the handshake JSON to write (required)")
	addr := fs.String("addr", "", "listen address (default 127.0.0.1:0)")
	tenant := fs.String("tenant", "", "tenant of the mounted workload (default: run-scoped)")
	deployment := fs.String("deployment", "", "deployment of the mounted workload (default: run-scoped)")
	subject := fs.String("subject", "", "subject minted into the task token (default: run-scoped)")
	foreign := fs.String("foreign-tenant", "", "tenant of the foreign workload used to prove isolation")
	refresh := fs.String("refresh-seconds", "", "re-mint the handshake task token every N seconds")
	allowlist := fs.String("db-allowlist", "",
		"comma-separated database names this tool may migrate (default "+DefaultDBAllowlist+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *envFile != "" {
		if err := loadEnvFile(*envFile); err != nil {
			return err
		}
	}
	set := func(key, value string) error {
		if value == "" {
			return nil
		}
		return os.Setenv(key, value)
	}
	if err := set("AHP_STATE_DSN", *dsn); err != nil {
		return err
	}
	if err := set("AHP_STATE_HANDSHAKE", *handshake); err != nil {
		return err
	}
	for _, override := range []struct{ value, env string }{
		{*addr, "AHP_STATE_ADDR"},
		{*tenant, "AHP_STATE_TENANT"},
		{*deployment, "AHP_STATE_DEPLOYMENT"},
		{*subject, "AHP_STATE_SUBJECT"},
		{*foreign, "AHP_STATE_FOREIGN_TENANT"},
		{*refresh, "AHP_STATE_REFRESH_SECONDS"},
	} {
		if err := set(override.env, override.value); err != nil {
			return err
		}
	}
	// A flag beats the environment, an unset flag keeps it, and only when both
	// are silent does the entry point's default apply.
	if *allowlist != "" {
		if err := os.Setenv("AHP_STATE_DB_ALLOWLIST", *allowlist); err != nil {
			return err
		}
	} else if os.Getenv("AHP_STATE_DB_ALLOWLIST") == "" {
		if err := os.Setenv("AHP_STATE_DB_ALLOWLIST", defaultAllowlist); err != nil {
			return err
		}
	}
	// A lane env file carries AHP_TEST_DATABASE_URL; the stand reads
	// AHP_STATE_DSN, so bridge the two for the one-command case.
	if os.Getenv("AHP_STATE_DSN") == "" {
		for _, key := range []string{"AHP_TEST_DATABASE_URL", "DATABASE_URL"} {
			if v := os.Getenv(key); v != "" {
				if err := os.Setenv("AHP_STATE_DSN", v); err != nil {
					return err
				}
				break
			}
		}
	}
	// Run-scoped defaults let a reviewer boot the stand without hand-written
	// scope values, and keep ClaimRun (deployment-level) inside this run. They
	// fill each variable on its own: a caller that mounted one scope value (or
	// an env file that carried a subject) keeps it, and only the gaps are closed.
	tag := os.Getenv("AHP_STATE_TENANT")
	if tag == "" {
		tag = fmt.Sprintf("t-%d", os.Getpid())
		if err := os.Setenv("AHP_STATE_TENANT", tag); err != nil {
			return err
		}
	}
	for _, fill := range []struct{ env, value string }{
		{"AHP_STATE_DEPLOYMENT", "d-" + tag},
		{"AHP_STATE_SUBJECT", "user-" + tag},
	} {
		if os.Getenv(fill.env) != "" {
			continue
		}
		if err := os.Setenv(fill.env, fill.value); err != nil {
			return err
		}
	}
	if os.Getenv("AHP_STATE_HANDSHAKE") == "" {
		return fmt.Errorf("-handshake is required: it is how the consumer learns the address and the tokens")
	}
	return nil
}
