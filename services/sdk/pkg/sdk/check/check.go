// Package check implements the ahp-sdk check lint: static analysis that verifies
// a custom agent's container code complies with the AHP platform contract (PDF rev 3.2,
// section 4.2). It detects forbidden patterns that would violate isolation, token
// handling, or telemetry boundaries.
package check

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Violation is one detected contract violation.
type Violation struct {
	File     string
	Line     int
	Rule     string
	Message  string
	Severity Severity
}

// Severity classifies violation impact.
type Severity int

const (
	Error Severity = iota
	Warning
)

func (s Severity) String() string {
	if s == Error {
		return "ERROR"
	}
	return "WARN"
}

// Rule identifiers.
const (
	RuleDirectDB     = "no-direct-db"
	RuleTokenLeak    = "no-token-to-external"
	RuleSandboxToken = "no-token-to-sandbox"
)

var forbiddenDBImports = map[string]string{
	"github.com/jackc/pgx":        "direct PostgreSQL: use State API instead",
	"database/sql":                "direct DB: use State API instead",
	"github.com/lib/pq":           "direct PostgreSQL: use State API instead",
	"gorm.io/gorm":                "direct DB ORM: use State API instead",
	"github.com/go-redis/redis":   "direct Redis: use State API instead",
	"github.com/redis/go-redis":   "direct Redis: use State API instead",
	"go.mongodb.org/mongo-driver": "direct MongoDB: use State API instead",
}

var forbiddenSandboxImports = map[string]string{
	"os/exec": "exec in agent container: side effects must go through Compute service with operation key",
}

// Result holds the outcome of a check run.
type Result struct {
	Violations []Violation
	Files      int
}

// CheckAll scans all Go files under root recursively.
func CheckAll(root string) (*Result, error) {
	result := &Result{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == ".git" || base == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		violations, err := checkFile(path)
		if err != nil {
			return err
		}
		result.Files++
		result.Violations = append(result.Violations, violations...)
		return nil
	})
	if err == nil && result.Files == 0 {
		err = fmt.Errorf("%s: no Go source files checked", root)
	}
	return result, err
}

func checkFile(path string) ([]Violation, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("%s: cannot parse Go source", path)
	}
	var violations []Violation
	for _, imp := range f.Imports {
		importPath := strings.Trim(imp.Path.Value, `"`)
		if msg, hit := matchPrefix(forbiddenDBImports, importPath); hit {
			pos := fset.Position(imp.Pos())
			violations = append(violations, Violation{
				File: path, Line: pos.Line, Rule: RuleDirectDB, Message: msg, Severity: Error,
			})
		}
		if msg, hit := matchPrefix(forbiddenSandboxImports, importPath); hit {
			pos := fset.Position(imp.Pos())
			violations = append(violations, Violation{
				File: path, Line: pos.Line, Rule: RuleSandboxToken, Message: msg, Severity: Error,
			})
		}
	}
	violations = append(violations, checkTokenFlow(path, fset, f)...)
	violations = append(violations, checkDirectIO(path, fset, f)...)
	return violations, nil
}

func matchPrefix(m map[string]string, importPath string) (string, bool) {
	for prefix, msg := range m {
		if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
			return msg, true
		}
	}
	return "", false
}
