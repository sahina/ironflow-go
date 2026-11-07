package ironflow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// ADR 0165: a timestamp a client returns is a time.Time. These owners are the
// exceptions it names: payloads kept as sent, and yields the SDK writes.
var stringTimestampOwners = map[string]bool{
	"PushEvent": true,
	"RunEvent":  true,
	"YieldInfo": true,
}

var timestampField = regexp.MustCompile(`(At|Timestamp|Time|Until|Since|Heartbeat|Expiry|Expires|Deadline|Date|LastSeen)$`)

// stringTimestamps lists exported struct fields that look like a timestamp and
// are typed string or *string, as Owner.Field. It parses the source because a
// reflect walk cannot see a type no value of which is reachable from a test.
func stringTimestamps(file *ast.File) []string {
	var found []string
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.TypeSpec)
		if !ok || !spec.Name.IsExported() || stringTimestampOwners[spec.Name.Name] {
			return true
		}
		ast.Inspect(spec.Type, func(m ast.Node) bool {
			field, ok := m.(*ast.Field)
			if !ok {
				return true
			}
			if !isStringType(field.Type) {
				return true
			}
			for _, name := range field.Names {
				if name.IsExported() && timestampField.MatchString(name.Name) {
					found = append(found, spec.Name.Name+"."+name.Name)
				}
			}
			return true
		})
		return false
	})
	return found
}

func isStringType(expr ast.Expr) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "string"
}

func TestNoStringResultTimestamps(t *testing.T) {
	fset := token.NewFileSet()
	var found []string
	err := filepath.WalkDir(".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			return err
		}
		found = append(found, stringTimestamps(file)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(found)
	if len(found) != 0 {
		t.Errorf("string timestamps outside the ADR 0165 exceptions:\n  %s", strings.Join(found, "\n  "))
	}
}

func TestStringTimestampGuardFlagsAField(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", `package x
import "time"
type Bad struct { CreatedAt string; ExpiresAt *string; Name string; hiddenAt string }
type Good struct { CreatedAt time.Time; UpdatedAt *time.Time }
type RunEvent struct { CreatedAt string }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := stringTimestamps(file)
	if want := []string{"Bad.CreatedAt", "Bad.ExpiresAt"}; !slices.Equal(got, want) {
		t.Errorf("flagged %v, want %v", got, want)
	}
}
