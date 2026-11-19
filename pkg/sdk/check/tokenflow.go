package check

import (
	"go/ast"
	"go/token"
	"strings"
)

// Token-flow rules per PDF rev 3.2 §6.2 and §4.2:
//   - Agent does not pass task token to sandbox (exec, VM) or external calls.
//   - Agent does not include token in response to caller.
//   - Agent does not log or telemetry-emit token content.
//   - Agent uses no direct outbound HTTP (must go through gateway).

// tokenSourceFuncs are function names whose return value is a token.
var tokenSourceFuncs = map[string]bool{
	"Token":     true,
	"TaskToken": true,
	"GetToken":  true,
}

// tokenSourceFields are struct field names whose value is a task token.
var tokenSourceFields = map[string]bool{
	"TaskToken":     true,
	"token":         true,
	"Token":         true,
	"Bearer":        true,
	"Authorization": true,
}

// forbiddenTokenSinkFuncs are functions/methods that indicate leaking a token
// when a token variable appears in their arguments.
var forbiddenTokenSinkFuncs = map[string]string{
	"Command":        "token passed to os/exec (sandbox)",
	"CommandContext": "token passed to os/exec (sandbox)",
	"Log":            "token passed to log output",
	"Logf":           "token passed to log output",
	"Print":          "token passed to log output",
	"Printf":         "token passed to stdout/stderr",
	"Println":        "token passed to stdout/stderr",
	"Info":           "token passed to telemetry/log",
	"Infof":          "token passed to telemetry/log",
	"Debug":          "token passed to telemetry/log",
	"Debugf":         "token passed to telemetry/log",
	"Warn":           "token passed to telemetry/log",
	"Warnf":          "token passed to telemetry/log",
	"Error":          "token passed to telemetry/log",
	"Errorf":         "token passed to telemetry/log",
	"Fprintf":        "token passed to writer (external sink)",
	"Sprintf":        "token embedded in format string (potential leak)",
	"Header":         "token passed to HTTP header construction",
	"Add":            "token passed to collection (URL params, headers)",
	"Set":            "token passed to map/header set",
}

// directHTTPFuncs detect direct outbound HTTP via package-level calls.
var directHTTPFuncs = map[string]string{
	"Get":         "direct HTTP GET: use Model/Tool Gateway via SDK",
	"Post":        "direct HTTP POST: use Model/Tool Gateway via SDK",
	"Head":        "direct HTTP HEAD: use gateway via SDK",
	"Dial":        "direct network dial: agent must not have raw egress",
	"DialTimeout": "direct network dial: agent must not have raw egress",
}

// httpClientMethods are methods on *http.Client that perform outbound requests.
var httpClientMethods = map[string]string{
	"Do":    "direct HTTP via http.Client.Do: use gateway via SDK",
	"Get":   "direct HTTP via http.Client.Get: use gateway via SDK",
	"Post":  "direct HTTP via http.Client.Post: use gateway via SDK",
	"Head":  "direct HTTP via http.Client.Head: use gateway via SDK",
	"Close": "",
}

// checkTokenFlow performs the token-leak analysis on a parsed file.
func checkTokenFlow(path string, fset *token.FileSet, f *ast.File) []Violation {
	tokenVars := findTokenVars(f)

	var violations []Violation
	if len(tokenVars) > 0 {
		ast.Inspect(f, func(n ast.Node) bool {
			callExpr, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fnName := calleeName(callExpr)
			pos := fset.Position(callExpr.Pos())

			if sinkMsg, isSink := forbiddenTokenSinkFuncs[fnName]; isSink && sinkMsg != "" {
				for _, arg := range callExpr.Args {
					if usesTokenVar(arg, tokenVars) {
						violations = append(violations, Violation{
							File: path, Line: pos.Line, Rule: RuleTokenLeak,
							Message: sinkMsg, Severity: Error,
						})
						break
					}
				}
			}

			return true
		})
	}

	// Check direct HTTP calls: package-level (http.Get, http.Post) and
	// method calls on variables of type http.Client / *http.Client.
	ast.Inspect(f, func(n ast.Node) bool {
		selExpr, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pos := fset.Position(selExpr.Pos())

		// Package-level: http.Get, http.Post, etc.
		if pkgIdent, isIdent := selExpr.X.(*ast.Ident); isIdent {
			if pkgIdent.Name == "http" {
				if msg, isDirect := directHTTPFuncs[selExpr.Sel.Name]; isDirect {
					violations = append(violations, Violation{
						File: path, Line: pos.Line, Rule: RuleTokenLeak,
						Message: msg, Severity: Warning,
					})
				}
			}
		}

		// Method on variable named like a client: client.Get(), client.Do()
		if recvIdent, isIdent := selExpr.X.(*ast.Ident); isIdent {
			name := strings.ToLower(recvIdent.Name)
			if strings.Contains(name, "client") || strings.Contains(name, "httpclient") {
				if msg, isHTTP := httpClientMethods[selExpr.Sel.Name]; isHTTP && msg != "" {
					violations = append(violations, Violation{
						File: path, Line: pos.Line, Rule: RuleTokenLeak,
						Message: msg, Severity: Warning,
					})
				}
			}
		}

		// Method on composite: (&http.Client{}).Get() — check embedded type
		if unary, isUnary := selExpr.X.(*ast.UnaryExpr); isUnary {
			if comp, isComp := unary.X.(*ast.CompositeLit); isComp {
				if sel, isSel := comp.Type.(*ast.SelectorExpr); isSel {
					if pkgId, isId := sel.X.(*ast.Ident); isId && pkgId.Name == "http" {
						if msg, isHTTP := httpClientMethods[selExpr.Sel.Name]; isHTTP && msg != "" {
							violations = append(violations, Violation{
								File: path, Line: pos.Line, Rule: RuleTokenLeak,
								Message: msg, Severity: Warning,
							})
						}
					}
				}
			}
		}

		return true
	})

	return violations
}

// findTokenVars identifies variables that hold a token value by analyzing
// assignments, function parameters, and return values.
func findTokenVars(f *ast.File) map[string]bool {
	vars := make(map[string]bool)

	// Pass 1: direct assignments from token sources.
	ast.Inspect(f, func(n ast.Node) bool {
		switch stmt := n.(type) {
		case *ast.AssignStmt:
			for i, rhs := range stmt.Rhs {
				if isTokenSourceExpr(rhs) && i < len(stmt.Lhs) {
					if ident, ok := stmt.Lhs[i].(*ast.Ident); ok {
						vars[ident.Name] = true
					}
				}
				// Alias: x := existingTokenVar
				if ident, ok := rhs.(*ast.Ident); ok {
					if vars[ident.Name] && i < len(stmt.Lhs) {
						if lhsIdent, ok := stmt.Lhs[i].(*ast.Ident); ok {
							vars[lhsIdent.Name] = true
						}
					}
				}
			}
			// Selector assignment: x := obj.TaskToken
			for i, rhs := range stmt.Rhs {
				if sel, ok := rhs.(*ast.SelectorExpr); ok {
					if tokenSourceFields[sel.Sel.Name] && i < len(stmt.Lhs) {
						if ident, ok := stmt.Lhs[i].(*ast.Ident); ok {
							vars[ident.Name] = true
						}
					}
				}
			}
		}
		return true
	})

	// Pass 2: function parameters named like tokens (heuristic: parameter
	// names that match tokenSourceFields or contain "token").
	for _, decl := range f.Decls {
		fnDecl, ok := decl.(*ast.FuncDecl)
		if !ok || fnDecl.Type.Params == nil {
			continue
		}
		for _, param := range fnDecl.Type.Params.List {
			for _, name := range param.Names {
				if tokenSourceFields[name.Name] || strings.Contains(strings.ToLower(name.Name), "token") {
					vars[name.Name] = true
				}
			}
		}
	}

	// Pass 3: return values from functions that return tokens.
	// If a function's return type or body suggests it returns a token,
	// track the assignment from its call.
	for _, decl := range f.Decls {
		fnDecl, ok := decl.(*ast.FuncDecl)
		if !ok || fnDecl.Type.Results == nil {
			continue
		}
		returnsToken := false
		for _, result := range fnDecl.Type.Results.List {
			if result.Names != nil {
				for _, name := range result.Names {
					if tokenSourceFields[name.Name] || strings.Contains(strings.ToLower(name.Name), "token") {
						returnsToken = true
					}
				}
			}
		}
		if returnsToken {
			// Mark calls to this function as token sources in assignments.
			ast.Inspect(f, func(n ast.Node) bool {
				if stmt, ok := n.(*ast.AssignStmt); ok {
					for i, rhs := range stmt.Rhs {
						if call, isCall := rhs.(*ast.CallExpr); isCall {
							if calleeName(call) == fnDecl.Name.Name && i < len(stmt.Lhs) {
								if ident, ok := stmt.Lhs[i].(*ast.Ident); ok {
									vars[ident.Name] = true
								}
							}
						}
					}
				}
				return true
			})
		}
	}

	// Pass 4: iterative alias propagation (handles chains: t := token; t2 := t)
	changed := true
	for changed {
		changed = false
		ast.Inspect(f, func(n ast.Node) bool {
			if stmt, ok := n.(*ast.AssignStmt); ok {
				for i, rhs := range stmt.Rhs {
					if ident, ok := rhs.(*ast.Ident); ok {
						if vars[ident.Name] && i < len(stmt.Lhs) {
							if lhsIdent, ok := stmt.Lhs[i].(*ast.Ident); ok {
								if !vars[lhsIdent.Name] {
									vars[lhsIdent.Name] = true
									changed = true
								}
							}
						}
					}
				}
			}
			return true
		})
	}

	return vars
}

// isTokenSourceExpr checks if an expression yields a token.
func isTokenSourceExpr(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.CallExpr:
		name := calleeName(e)
		return tokenSourceFuncs[name]
	case *ast.SelectorExpr:
		return tokenSourceFields[e.Sel.Name]
	}
	return false
}

// usesTokenVar checks if an expression references any token variable.
func usesTokenVar(expr ast.Expr, tokenVars map[string]bool) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok {
			if tokenVars[ident.Name] {
				found = true
				return false
			}
		}
		return !found
	})
	return found
}

// calleeName extracts the function name from a CallExpr.
func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}
