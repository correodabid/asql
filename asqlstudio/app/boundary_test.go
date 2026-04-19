package studioapp

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestStudioPackageDoesNotImportPrivateEngineOrServerPackages(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file")
	}
	studioDir := filepath.Dir(currentFile)
	matches, err := filepath.Glob(filepath.Join(studioDir, "*.go"))
	if err != nil {
		t.Fatalf("glob studio sources: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("expected studio Go files")
	}

	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports for %s: %v", filepath.Base(path), err)
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, "\"")
			if strings.HasPrefix(value, "github.com/correodabid/asql/internal/engine/") {
				t.Fatalf("%s imports forbidden engine-private package %s", filepath.Base(path), value)
			}
			if strings.HasPrefix(value, "github.com/correodabid/asql/internal/server/") {
				t.Fatalf("%s imports forbidden server-private package %s", filepath.Base(path), value)
			}
		}
	}
}
