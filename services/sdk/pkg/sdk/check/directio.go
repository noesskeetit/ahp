package check

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

const (
	RuleDirectNetwork = "no-direct-network"
	RuleLocalWrite    = "no-local-durable-write"
)

// checkDirectIO recognizes standard-library IO, including import aliases. It is
// a source lint, not a proof of confinement: dynamic or dependency code still
// requires the platform's network/filesystem policy.
func checkDirectIO(path string, fset *token.FileSet, f *ast.File) []Violation {
	imports := make(map[string]string)
	for _, imp := range f.Imports {
		name, _ := strconv.Unquote(imp.Path.Value)
		alias := name[strings.LastIndex(name, "/")+1:]
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		imports[alias] = name
	}
	var findings []Violation
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if isHTTPClient(sel.X, imports, 0) {
			switch sel.Sel.Name {
			case "Do", "Get", "Head", "Post", "PostForm":
				findings = append(findings, Violation{File: path, Line: fset.Position(call.Pos()).Line, Rule: RuleDirectNetwork, Severity: Error, Message: "direct HTTP client bypasses the AHP service contract"})
			}
		}
		receiver, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		// A shadowed local named like an imported package is not that package.
		if receiver.Obj != nil {
			return true
		}
		pkg, method := imports[receiver.Name], sel.Sel.Name
		rule := ""
		switch pkg {
		case "net/http":
			switch method {
			case "Get", "Head", "Post", "PostForm":
				rule = RuleDirectNetwork
			}
		case "net", "net/tls":
			if strings.HasPrefix(method, "Dial") || strings.HasPrefix(method, "Listen") {
				rule = RuleDirectNetwork
			}
		case "os":
			switch method {
			case "WriteFile", "Create", "CreateTemp", "Write", "Mkdir", "MkdirAll", "MkdirTemp", "Remove", "RemoveAll", "Rename", "Truncate", "Link", "Symlink":
				rule = RuleLocalWrite
			case "OpenFile":
				// Unknown flags can write. Only a statically read-only open is exempt.
				readonly := false
				if len(call.Args) > 1 {
					if flag, ok := call.Args[1].(*ast.BasicLit); ok {
						readonly = flag.Kind == token.INT && flag.Value == "0"
					}
					if flag, ok := call.Args[1].(*ast.SelectorExpr); ok {
						if id, ok := flag.X.(*ast.Ident); ok {
							readonly = imports[id.Name] == "os" && flag.Sel.Name == "O_RDONLY"
						}
					}
				}
				if !readonly {
					rule = RuleLocalWrite
				}
			}
		case "io/ioutil":
			switch method {
			case "WriteFile", "TempFile", "TempDir":
				rule = RuleLocalWrite
			}
		}
		if rule != "" {
			findings = append(findings, Violation{File: path, Line: fset.Position(call.Pos()).Line, Rule: rule, Severity: Error, Message: "direct IO bypasses the AHP service contract"})
		}
		return true
	})
	return findings
}

// Resolve only lexical standard-library Client bindings, not arbitrary objects
// whose variable happens to be called client. Unresolved dynamic types remain
// outside this lint's guarantees.
func isHTTPClient(expr ast.Expr, imports map[string]string, depth int) bool {
	if depth > 8 {
		return false
	}
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return isHTTPClient(e.X, imports, depth+1)
	case *ast.UnaryExpr:
		return isHTTPClient(e.X, imports, depth+1)
	case *ast.StarExpr:
		return isHTTPClient(e.X, imports, depth+1)
	case *ast.CompositeLit:
		return isHTTPClient(e.Type, imports, depth+1)
	case *ast.SelectorExpr:
		id, ok := e.X.(*ast.Ident)
		return ok && id.Obj == nil && imports[id.Name] == "net/http" && (e.Sel.Name == "Client" || e.Sel.Name == "DefaultClient")
	case *ast.Ident:
		if e.Obj == nil {
			return false
		}
		switch declaration := e.Obj.Decl.(type) {
		case *ast.Field:
			return isHTTPClient(declaration.Type, imports, depth+1)
		case *ast.ValueSpec:
			if declaration.Type != nil {
				return isHTTPClient(declaration.Type, imports, depth+1)
			}
			for i, name := range declaration.Names {
				if name.Name == e.Name && i < len(declaration.Values) {
					return isHTTPClient(declaration.Values[i], imports, depth+1)
				}
			}
		case *ast.AssignStmt:
			for i, left := range declaration.Lhs {
				if id, ok := left.(*ast.Ident); ok && id.Name == e.Name && i < len(declaration.Rhs) {
					return isHTTPClient(declaration.Rhs[i], imports, depth+1)
				}
			}
		}
	}
	return false
}
