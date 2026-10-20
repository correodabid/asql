package servertest_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/correodabid/asql/pkg/servertest"
)

func TestStartForTestingServesAnEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	srv := servertest.StartForTesting(t, servertest.Options{})
	conn, err := pgx.Connect(ctx, srv.DSN)
	if err != nil {
		t.Fatal(err)
	}
	// Left open on purpose: the cleanup must not wait for it.
	for _, sql := range []string{"BEGIN DOMAIN app", "CREATE TABLE t (id INT PRIMARY KEY)", "COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
}
