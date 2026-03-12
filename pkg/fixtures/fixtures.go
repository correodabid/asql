package fixtures

import (
	"context"

	internalfixtures "asql/internal/fixtures"

	"github.com/jackc/pgx/v5"
)

const CurrentVersion = internalfixtures.CurrentVersion

type File = internalfixtures.File
type Step = internalfixtures.Step
type ExportOptions = internalfixtures.ExportOptions
type SQLExecutor = internalfixtures.SQLExecutor
type ValidationError = internalfixtures.ValidationError

func LoadFile(path string) (*File, error) {
	return internalfixtures.LoadFile(path)
}

func SaveFile(path string, fixture *File) error {
	return internalfixtures.SaveFile(path, fixture)
}

func ExportFromPGWire(ctx context.Context, conn *pgx.Conn, options ExportOptions) (*File, error) {
	return internalfixtures.ExportFromPGWire(ctx, conn, options)
}

func ValidateSpec(fixture *File) error {
	return internalfixtures.ValidateSpec(fixture)
}

func ValidateDryRun(ctx context.Context, fixture *File) error {
	return internalfixtures.ValidateDryRun(ctx, fixture)
}

func Apply(ctx context.Context, fixture *File, exec SQLExecutor) error {
	return internalfixtures.Apply(ctx, fixture, exec)
}
