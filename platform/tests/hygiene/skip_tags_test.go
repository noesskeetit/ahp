package hygiene

// The skip-tag dictionary is data, and data drifts. This leg is the reason it
// cannot: every tag a leg in this tree is able to EMIT must be registered in
// tools/skip-tags.json, so introducing a kind of skip is a registry change
// (reviewed for form and class) rather than a green run whose counter cannot see
// what was skipped.
//
// COND-5 is the concrete case this closes: internal/execbridge grew
// SKIP_ENV[tag=database] while the dictionary carried only real_deps,
// artifact_plane and artifact_parser, so the first rollout of the acceptance
// mechanism refuses an honestly-labelled skip. The mechanism's own reaction is
// right and is not softened here -- an unregistered tag is UNKNOWN_SKIP_TAG,
// which is red. What was missing is a place where the alphabet is declared, and
// a check that moves the failure from "discovered during a rollout" to
// "discovered in review".
//
// This leg validates the dictionary. It does not count skips and decides no
// verdict: that is scripts/skip-verdict.mjs, owned by Russell.

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type skipTagRegistry struct {
	Version       int      `json:"version"`
	Owner         string   `json:"owner"`
	ClosedClasses []string `json:"closed_classes"`
	Tags          map[string]struct {
		Class        string   `json:"class"`
		Status       string   `json:"status"`
		SupersededBy string   `json:"superseded_by"`
		EmittedBy    []string `json:"emitted_by"`
	} `json:"tags"`
}

var (
	// A tag written out in full inside one string value: SKIP_ENV[tag=artifact_plane].
	tagLiteralRE = regexp.MustCompile(`SKIP_(?:ENV|EXPLICIT)\[tag=([a-z0-9_]+)\]`)
	// The leading half of a token assembled by concatenation:
	// "SKIP_EXPLICIT[tag=" + someTagConstant + "] ...".
	tagPrefixes = []string{"SKIP_ENV[tag=", "SKIP_EXPLICIT[tag="}
	// A tag value is a bare lowercase name; used to accept a constant as one.
	tagValueRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	tagShapeRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

	// Python composes the SAME token in a string, and until 2026-11-10 this leg read
	// only .go -- so a tag named in the Dot harness (tests/dote2e/harness/*.py) or in a
	// tools/ script could never reach the registry check at all. There is no python
	// compiler to fail first on a file that does not parse, so the shapes accepted here
	// are stated out loud and anything else that looks like a token is REPORTED.
	//
	// NAME = "tag": a python tag constant. Only a value that is itself a tag name
	// counts, which is the same rule the Go side applies with tagValueRE.
	pythonTagConstRE = regexp.MustCompile(`(?m)^[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*"([a-z][a-z0-9_]{0,31})"[ \t]*(?:#.*)?$`)
	// what sits between `SKIP_ENV[tag=` and `]` in python source: a literal tag, an
	// f-string interpolation, or a `" + NAME + "` concatenation.
	pythonLiteralTagRE = regexp.MustCompile(`^([a-z][a-z0-9_]{0,31})\]`)
	pythonInterpRE     = regexp.MustCompile(`^\{ *([A-Za-z_][A-Za-z0-9_]*) *\}`)
	pythonConcatRE     = regexp.MustCompile(`^["'] *\+ *([A-Za-z_][A-Za-z0-9_]*) *\+ *["']`)
)

func loadRegistry(t *testing.T, root string) skipTagRegistry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "tools", "skip-tags.json"))
	if err != nil {
		t.Fatalf("read tools/skip-tags.json: %v -- the dictionary is the contract; without it no skip token is verifiable", err)
	}
	var registry skipTagRegistry
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatalf("tools/skip-tags.json is not valid JSON: %v", err)
	}
	if len(registry.Tags) == 0 || len(registry.ClosedClasses) == 0 {
		t.Fatal("the dictionary carries no tags or no classes: this leg would prove nothing")
	}
	return registry
}

// goFilesWithConsts parses every tracked Go file twice: once to learn which
// constants hold a tag value (a tag is often assembled as
// "SKIP_ENV[tag=" + tagConstant + "]"), and once to collect emissions. Parsing is
// used instead of grep for a reason measured the first time this leg ran: the
// leg's own doc comment named a token, and a text scan read that comment as an
// emission. A comment describes; only a string literal emits.
func parseGoFiles(t *testing.T, root string, names []string) (perFile map[string]map[string]string, global map[string]string, ambiguous map[string]bool) {
	t.Helper()
	perFile = map[string]map[string]string{}
	ambiguous = map[string]bool{}
	global = map[string]string{}
	for _, name := range names {
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, name), nil, parser.SkipObjectResolution)
		if err != nil {
			// A file that does not parse cannot be scanned, and cannot be compiled
			// either: the compiler is the authority and fails the build first.
			continue
		}
		consts := map[string]string{}
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Values) != 1 {
					continue
				}
				literal, ok := value.Values[0].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(literal.Value)
				if err != nil || !tagValueRE.MatchString(unquoted) {
					continue
				}
				for _, ident := range value.Names {
					consts[ident.Name] = unquoted
					if previous, seen := global[ident.Name]; seen && previous != unquoted {
						ambiguous[ident.Name] = true
					}
					global[ident.Name] = unquoted
				}
			}
		}
		perFile[name] = consts
	}
	return perFile, global, ambiguous
}

// emittedTagsIn returns the tags a file can emit and the tag expressions it could
// not resolve to a name.
func emittedTagsIn(t *testing.T, root, name string, local map[string]string, global map[string]string, ambiguous map[string]bool) ([]string, []string) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, name), nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil
	}
	var tags, unresolved []string
	seenTag := map[string]bool{}
	seenUnresolved := map[string]bool{}
	recordTag := func(tag string) {
		if seenTag[tag] {
			return
		}
		seenTag[tag] = true
		tags = append(tags, tag)
	}
	recordUnresolved := func(what string) {
		if seenUnresolved[what] {
			return
		}
		seenUnresolved[what] = true
		unresolved = append(unresolved, what)
	}
	resolve := func(expr ast.Expr) (string, bool) {
		switch value := expr.(type) {
		case *ast.BasicLit:
			if value.Kind != token.STRING {
				return "", false
			}
			unquoted, err := strconv.Unquote(value.Value)
			if err != nil || !tagValueRE.MatchString(unquoted) {
				return "", false
			}
			return unquoted, true
		case *ast.Ident:
			if got, ok := local[value.Name]; ok {
				return got, true
			}
			if ambiguous[value.Name] {
				return "", false
			}
			got, ok := global[value.Name]
			return got, ok
		}
		return "", false
	}
	describe := func(expr ast.Expr) string {
		switch value := expr.(type) {
		case *ast.Ident:
			return value.Name
		case *ast.BasicLit:
			return "<literal>"
		default:
			return "<expression>"
		}
	}
	var flatten func(expr ast.Expr, into *[]ast.Expr)
	flatten = func(expr ast.Expr, into *[]ast.Expr) {
		if binary, ok := expr.(*ast.BinaryExpr); ok && binary.Op == token.ADD {
			flatten(binary.X, into)
			flatten(binary.Y, into)
			return
		}
		*into = append(*into, expr)
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch expr := node.(type) {
		case *ast.BasicLit:
			if expr.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(expr.Value)
			if err != nil {
				return true
			}
			for _, match := range tagLiteralRE.FindAllStringSubmatch(value, -1) {
				recordTag(match[1])
			}
		case *ast.BinaryExpr:
			if expr.Op != token.ADD {
				return true
			}
			var operands []ast.Expr
			flatten(expr, &operands)
			for i, operand := range operands {
				literal, ok := operand.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING || i+1 >= len(operands) {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}
				prefixed := false
				for _, prefix := range tagPrefixes {
					if strings.HasSuffix(value, prefix) {
						prefixed = true
					}
				}
				if !prefixed {
					continue
				}
				if resolved, ok := resolve(operands[i+1]); ok {
					recordTag(resolved)
					continue
				}
				recordUnresolved(name + ":" + describe(operands[i+1]))
			}
		}
		return true
	})
	return tags, unresolved
}

// parsePythonFiles collects `NAME = "tag"` assignments the way parseGoFiles collects Go
// constants. Unlike Go there is no compiler waiting behind this scanner, so a python
// file that cannot be READ is reported as unresolvable rather than passed over: a tag
// nobody could read must never look like a tag nobody emitted.
func parsePythonFiles(root string, names []string) (perFile map[string]map[string]string, global map[string]string, ambiguous map[string]bool, unreadable []string) {
	perFile = map[string]map[string]string{}
	global = map[string]string{}
	ambiguous = map[string]bool{}
	for _, name := range names {
		if !strings.HasSuffix(name, ".py") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			unreadable = append(unreadable, name+": unreadable ("+err.Error()+")")
			continue
		}
		consts := map[string]string{}
		for _, match := range pythonTagConstRE.FindAllStringSubmatch(string(raw), -1) {
			consts[match[1]] = match[2]
			if previous, seen := global[match[1]]; seen && previous != match[2] {
				ambiguous[match[1]] = true
			}
			global[match[1]] = match[2]
		}
		perFile[name] = consts
	}
	return perFile, global, ambiguous, unreadable
}

// emittedTagsInPython returns the tags a python file can emit. Three shapes are read:
// a literal tag inside the token, an f-string interpolation `{NAME}`, and a
// `" + NAME + "` concatenation. Anything else between the prefix and the closing bracket
// is reported as unresolvable -- "the scanner did not understand" is not the same thing
// as "the file emits nothing", and conflating them is how a tag escapes the registry.
func emittedTagsInPython(root, name string, local, global map[string]string, ambiguous map[string]bool) (tags, unresolved []string) {
	raw, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		return nil, []string{name + ": unreadable (" + err.Error() + ")"}
	}
	text := string(raw)
	seenTag, seenUnresolved := map[string]bool{}, map[string]bool{}
	recordTag := func(tag string) {
		if !seenTag[tag] {
			seenTag[tag] = true
			tags = append(tags, tag)
		}
	}
	recordUnresolved := func(what string) {
		if !seenUnresolved[what] {
			seenUnresolved[what] = true
			unresolved = append(unresolved, what)
		}
	}
	resolveIdent := func(ident string) (string, bool) {
		if ambiguous[ident] {
			return "", false
		}
		if got, ok := local[ident]; ok {
			return got, true
		}
		got, ok := global[ident]
		return got, ok
	}
	for _, prefix := range tagPrefixes {
		for offset := 0; ; {
			at := strings.Index(text[offset:], prefix)
			if at < 0 {
				break
			}
			start := offset + at + len(prefix)
			rest := text[start:]
			offset = start
			if match := pythonLiteralTagRE.FindStringSubmatch(rest); match != nil {
				recordTag(match[1])
				continue
			}
			ident := ""
			if match := pythonInterpRE.FindStringSubmatch(rest); match != nil {
				ident = match[1]
			} else if match := pythonConcatRE.FindStringSubmatch(rest); match != nil {
				ident = match[1]
			}
			if ident != "" {
				if got, ok := resolveIdent(ident); ok {
					recordTag(got)
					continue
				}
				recordUnresolved(name + ": {" + ident + "} does not resolve to one tag name")
				continue
			}
			firstLine := strings.SplitN(rest, "\n", 2)[0]
			if cut := strings.IndexByte(firstLine, '"'); cut >= 0 {
				firstLine = firstLine[:cut]
			}
			recordUnresolved(name + ": " + strings.TrimSpace(firstLine) + " is a token this leg cannot read")
		}
	}
	return tags, unresolved
}

// nonEmitterSuffixes lists the file types that quote a token without being able to emit
// one, each with the reason. Naming them is the point: the census below is fail-closed,
// so the next language that carries a token turns this leg RED instead of quietly
// joining the list of things nobody scans.
var nonEmitterSuffixes = map[string]string{
	".md":  "prose quoting a token is a report, not a leg",
	".mjs": "the gate's own fixture corpus: token strings in scripts/skip-verdict.test.mjs are classifier inputs, not skips a lane takes",
}

// failClosedCarrierCensus asks the one question a per-suffix scan cannot ask: is there a
// tracked file that carries a token prefix and is read by NEITHER scanner?
func failClosedCarrierCensus(t *testing.T, root string) {
	t.Helper()
	out, err := exec.Command("git", "-C", root, "grep", "-l", "-F", "-e", tagPrefixes[0], "-e", tagPrefixes[1], "--").Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 {
			return // no carrier anywhere in the tracked tree
		}
		t.Fatalf("carrier census (git grep) failed: %v -- a census that cannot run is not a census", err)
	}
	var unread []string
	for _, name := range strings.Fields(string(out)) {
		if strings.HasSuffix(name, ".go") || strings.HasSuffix(name, ".py") {
			continue // read by the two scanners above
		}
		if reason, allowed := nonEmitterSuffixes[filepath.Ext(name)]; allowed {
			_ = reason
			continue
		}
		unread = append(unread, name)
	}
	if len(unread) > 0 {
		sort.Strings(unread)
		t.Fatalf("%d tracked file(s) carry a skip token in a language this leg does not read: %v -- extend the scanner or add the suffix to nonEmitterSuffixes WITH A REASON; an unscanned carrier reported as clean is the exact defect this leg exists to close", len(unread), unread)
	}
}

// TestEveryEmittedSkipTagIsRegistered is the dictionary leg.
func TestEveryEmittedSkipTagIsRegistered(t *testing.T) {
	root := repoRoot(t)
	registry := loadRegistry(t, root)
	names := trackedFiles(t, root)

	perFile, global, ambiguous := parseGoFiles(t, root, names)
	pyPerFile, pyGlobal, pyAmbiguous, pyUnreadable := parsePythonFiles(root, names)

	emitted := map[string][]string{}
	var unresolvable []string
	unresolvable = append(unresolvable, pyUnreadable...)
	for _, name := range names {
		switch {
		case strings.HasSuffix(name, ".go"):
			tags, unresolved := emittedTagsIn(t, root, name, perFile[name], global, ambiguous)
			for _, tag := range tags {
				emitted[tag] = append(emitted[tag], name)
			}
			unresolvable = append(unresolvable, unresolved...)
		case strings.HasSuffix(name, ".py"):
			// A tag named only in python was invisible to this leg before 2026-11-10:
			// the Dot harness and the tools/ scripts compose the SAME token, and the
			// registry check never saw it. Same Fatalf, same counting, no separate grep.
			tags, unresolved := emittedTagsInPython(root, name, pyPerFile[name], pyGlobal, pyAmbiguous)
			for _, tag := range tags {
				emitted[tag] = append(emitted[tag], name)
			}
			unresolvable = append(unresolvable, unresolved...)
		}
	}
	failClosedCarrierCensus(t, root)
	sort.Strings(unresolvable)
	if len(unresolvable) > 0 {
		t.Fatalf("skip tags assembled from something this leg cannot resolve to a name: %v -- declare the tag as a plain `Name = \"tag\"` constant (Go) or `NAME = \"tag\"` assignment (python); a tag nobody can name is a tag nobody can count", unresolvable)
	}

	var unknown []string
	for tag, where := range emitted {
		if _, registered := registry.Tags[tag]; registered {
			continue
		}
		sort.Strings(where)
		unknown = append(unknown, tag+" ("+where[0]+")")
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		t.Fatalf("emitted skip tags missing from tools/skip-tags.json: %v -- register the tag (name the dependency, pick a class from closed_classes); until then the acceptance mechanism reports UNKNOWN_SKIP_TAG and the run is red", unknown)
	}
	if len(emitted) < 3 {
		t.Fatalf("only %d distinct tags collected: this leg would be reading the wrong tree", len(emitted))
	}
}

// TestSkipTagRegistryIsWellFormed keeps the registry itself honest: a class the
// mechanism has never heard of cannot colour a verdict, a retired tag must name
// its successor, and a tag name has to be nameable.
func TestSkipTagRegistryIsWellFormed(t *testing.T) {
	root := repoRoot(t)
	registry := loadRegistry(t, root)

	allowed := map[string]bool{}
	for _, class := range registry.ClosedClasses {
		allowed[class] = true
	}

	var badShape, badClass, dangling []string
	for tag, entry := range registry.Tags {
		if !tagShapeRE.MatchString(tag) {
			badShape = append(badShape, tag)
		}
		if !allowed[entry.Class] {
			badClass = append(badClass, tag+" class="+entry.Class)
		}
		switch entry.Status {
		case "canonical":
		case "transition", "deprecated":
			if strings.TrimSpace(entry.SupersededBy) == "" {
				dangling = append(dangling, tag+" status="+entry.Status+" has no superseded_by")
			}
		default:
			dangling = append(dangling, tag+" status="+entry.Status)
		}
	}
	if len(badShape) > 0 {
		sort.Strings(badShape)
		t.Fatalf("tag names outside ^[a-z][a-z0-9_]{0,31}$: %v", badShape)
	}
	if len(badClass) > 0 {
		sort.Strings(badClass)
		t.Fatalf("tags carrying a class outside closed_classes: %v -- the class decides the verdict colour, so it must be one the mechanism implements", badClass)
	}
	if len(dangling) > 0 {
		sort.Strings(dangling)
		t.Fatalf("dictionary entries with an unknown status, or retired without naming a successor: %v", dangling)
	}
}

// mechanismView is the shape scripts/skip-verdict.mjs can consume: its --tags flag
// reads a JSON ARRAY of {tag, class} (readTagFile) and refuses a tag that is absent
// from it as UNKNOWN_SKIP_TAG. The registry is an object keyed by tag, so it cannot
// be handed to the mechanism directly -- and a dictionary the mechanism refuses is a
// gate that cannot read, which fails closed and looks like every lane is broken.
type mechanismView []struct {
	Tag    string `json:"tag"`
	Class  string `json:"class"`
	Status string `json:"status"`
}

// TestMechanismViewOfTheRegistry re-derives the generated view in Go, without calling
// the generator. Two independent implementations of one rule means a bug in either
// one turns a leg red instead of turning a gate silently blind: the generator can be
// wrong about ordering or field names, and the mechanism can be wrong about what it
// accepts, and both cases land here.
func TestMechanismViewOfTheRegistry(t *testing.T) {
	root := repoRoot(t)
	registry := loadRegistry(t, root)
	raw, err := os.ReadFile(filepath.Join(root, "tools", "skip-tags.mechanism.json"))
	if err != nil {
		t.Fatalf("read tools/skip-tags.mechanism.json: %v -- regenerate with python3 tools/gen-skip-tags-mechanism.py", err)
	}
	var view mechanismView
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("the mechanism view must be a JSON array of {tag,class}: %v", err)
	}
	if len(view) != len(registry.Tags) {
		t.Fatalf("the mechanism view carries %d tags, the registry %d -- a tag invisible to the mechanism is UNKNOWN_SKIP_TAG for every leg that emits it", len(view), len(registry.Tags))
	}
	seen := map[string]string{}
	previous := ""
	for _, entry := range view {
		if entry.Tag == "" || entry.Class == "" {
			t.Fatalf("view entry without tag/class: %+v -- scripts/skip-verdict.mjs readTagFile rejects the whole file on one such entry", entry)
		}
		if entry.Tag < previous {
			t.Fatalf("view is not sorted by tag (%s after %s): a generated file must be byte-stable, otherwise every regeneration is a diff", entry.Tag, previous)
		}
		previous = entry.Tag
		if _, twice := seen[entry.Tag]; twice {
			t.Fatalf("duplicate tag in the mechanism view: %s", entry.Tag)
		}
		want, known := registry.Tags[entry.Tag]
		if !known {
			t.Fatalf("the mechanism view carries %s, which the registry does not know: the view is generated, not hand-edited", entry.Tag)
		}
		if want.Class != entry.Class {
			t.Fatalf("%s: view says class %s, the registry says %s", entry.Tag, entry.Class, want.Class)
		}
		seen[entry.Tag] = entry.Class
	}
	for tag, entry := range registry.Tags {
		if _, delivered := seen[tag]; !delivered {
			t.Fatalf("%s is in the registry but not in the mechanism view: legs emitting it would be refused as UNKNOWN_SKIP_TAG", tag)
		}
		if !contains(registry.ClosedClasses, entry.Class) {
			t.Fatalf("%s: class %s is outside closed_classes %v", tag, entry.Class, registry.ClosedClasses)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
