// Package fixtures provides the engine-backed dry-run helper for ASQL
// fixtures. Pure types and I/O helpers live in
// github.com/correodabid/asql/pkg/fixtures; this package adds
// ValidateDryRun, which spins up an in-process engine to verify that a
// fixture applies cleanly without persisting side effects.
//
// External clients that cannot (or should not) depend on engine internals
// must use pkg/fixtures directly and perform validation against a live
// pgwire connection.
package fixtures

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
	pkgfixtures "github.com/correodabid/asql/pkg/fixtures"
)

// File is re-exported so callers of this package can keep using a single
// fixtures import when they also need ValidateDryRun. External clients
// should prefer github.com/correodabid/asql/pkg/fixtures directly.
type (
	File             = pkgfixtures.File
	Step             = pkgfixtures.Step
	ExportOptions    = pkgfixtures.ExportOptions
	SQLExecutor      = pkgfixtures.SQLExecutor
	ValidationError  = pkgfixtures.ValidationError
)

const CurrentVersion = pkgfixtures.CurrentVersion

// LoadFile is re-exported for backwards compatibility with existing callers.
var (
	LoadFile         = pkgfixtures.LoadFile
	SaveFile         = pkgfixtures.SaveFile
	ExportFromPGWire = pkgfixtures.ExportFromPGWire
	ValidateSpec     = pkgfixtures.ValidateSpec
	Apply            = pkgfixtures.Apply
)

// ValidateDryRun validates a fixture's spec and then executes it against a
// throw-away in-process engine backed by a temporary WAL, guaranteeing the
// fixture applies cleanly before it touches a real cluster.
func ValidateDryRun(ctx context.Context, fixture *File) error {
	if err := ValidateSpec(fixture); err != nil {
		return err
	}

	walPath := filepath.Join(os.TempDir(), fmt.Sprintf("asql-fixture-%s.wal", sanitizeForPath(fixture.Name)))
	walPath = uniqueTempPath(walPath)
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		return fmt.Errorf("create validation wal: %w", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(filepath.Dir(walPath))
	}()

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		return fmt.Errorf("create validation engine: %w", err)
	}

	return Apply(ctx, fixture, newEngineExecutor(engine))
}

type engineExecutor struct {
	engine  *executor.Engine
	session *executor.Session
}

func newEngineExecutor(engine *executor.Engine) *engineExecutor {
	return &engineExecutor{engine: engine, session: engine.NewSession()}
}

func (e *engineExecutor) Exec(ctx context.Context, sql string) error {
	_, err := e.engine.Execute(ctx, e.session, sql)
	return err
}

func sanitizeForPath(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	trimmed = strings.ReplaceAll(trimmed, " ", "-")
	trimmed = strings.ReplaceAll(trimmed, "/", "-")
	trimmed = strings.ReplaceAll(trimmed, "\\", "-")
	if trimmed == "" {
		return "fixture"
	}
	return trimmed
}

func uniqueTempPath(base string) string {
	dir, err := os.MkdirTemp("", "asql-fixture-")
	if err != nil {
		return base
	}
	return filepath.Join(dir, filepath.Base(base))
}
