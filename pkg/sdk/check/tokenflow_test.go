package check

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTokenLeakToExec(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "os/exec"

func run(claim Claimed) {
	token := claim.TaskToken
	exec.Command("vm-run", token)
}

type Claimed struct{ TaskToken string }
var _ = exec.Command
`
	write(t, dir, "leak.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-leak violation, got %+v", violations)
	}
}

func TestTokenLeakToLog(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "log"

func process(claim Claimed) {
	token := claim.TaskToken
	log.Println(token)
}

type Claimed struct{ TaskToken string }
var _ = log.Println
`
	write(t, dir, "logleak.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-leak violation, got %+v", violations)
	}
}

func TestTokenLeakToSprintf(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "fmt"

func format(claim Claimed) string {
	token := claim.TaskToken
	return fmt.Sprintf("auth: %s", token)
}

type Claimed struct{ TaskToken string }
var _ = fmt.Sprintf
`
	write(t, dir, "fmtleak.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-leak violation, got %+v", violations)
	}
}

func TestNoFalsePositive(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

func safe() {
	msg := "hello"
	result := format(msg)
	_ = result
}

func format(s string) string { return s }
`
	write(t, dir, "safe.go", src)
	violations := runCheck(t, dir)
	if len(violations) != 0 {
		t.Fatalf("unexpected violations on safe code: %+v", violations)
	}
}

func TestDirectHTTPIsAnError(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "net/http"

func call() {
	resp, _ := http.Get("https://external.api/data")
	_ = resp
}
var _ = http.Get
`
	write(t, dir, "http.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleDirectNetwork) {
		t.Fatalf("expected direct-network violation, got %+v", violations)
	}
	for _, v := range violations {
		if v.Rule == RuleDirectNetwork && v.Severity != Error {
			t.Errorf("direct HTTP must block startup, got %v", v.Severity)
		}
	}
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runCheck(t *testing.T, dir string) []Violation {
	t.Helper()
	result, err := CheckAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	return result.Violations
}

func hasRule(vs []Violation, rule string) bool {
	for _, v := range vs {
		if v.Rule == rule {
			return true
		}
	}
	return false
}

func TestTokenLeakViaParam(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "os/exec"

func process(token string) {
	exec.Command("curl", token)
}
var _ = exec.Command
`
	write(t, dir, "param.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-as-param leak, got %+v", violations)
	}
}

func TestTokenLeakViaAlias(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "log"

func run(claim Claimed) {
	token := claim.TaskToken
	alias := token
	log.Print(alias)
}

type Claimed struct{ TaskToken string }
var _ = log.Print
`
	write(t, dir, "alias.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-via-alias leak, got %+v", violations)
	}
}

func TestHTTPClientGet(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "net/http"

func call() {
	client := &http.Client{}
	resp, _ := client.Get("https://evil.example.com")
	_ = resp
}
var _ = http.Client{}
`
	write(t, dir, "client.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected http.Client.Get warning, got %+v", violations)
	}
}

func TestBareLogPrint(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "log"

func process(claim Claimed) {
	token := claim.TaskToken
	log.Print("auth=", token)
}

type Claimed struct{ TaskToken string }
var _ = log.Print
`
	write(t, dir, "bareprint.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected log.Print leak, got %+v", violations)
	}
}

func TestTokenViaFunctionReturn(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "os/exec"

type Result struct{}

func getToken() (token string) {
	return "secret"
}

func run() {
	tok := getToken()
	exec.Command("vm", tok)
}
var _ = exec.Command
`
	write(t, dir, "ret.go", src)
	violations := runCheck(t, dir)
	if !hasRule(violations, RuleTokenLeak) {
		t.Fatalf("expected token-via-return leak, got %+v", violations)
	}
}
