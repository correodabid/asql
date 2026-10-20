package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestPlaceholdersInsideLiteralsAreText: "$1" in a string literal, a quoted
// identifier or a comment is not a parameter, and a parameter's value is
// not scanned for placeholders again.
func TestPlaceholdersInsideLiteralsAreText(t *testing.T) {
	sql := `SELECT "$9" FROM t WHERE note = 'costs $1 or ''$2''' AND id = $1 -- $3
		AND kind = $2 /* $4 */`
	if got := len(inferParamOIDCount(sql)); got != 2 {
		t.Fatalf("%d placeholders, want 2", got)
	}
	got, err := substituteParams(sql, [][]byte{[]byte("7"), []byte("$1 off")}, nil, []uint32{20, 25})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "$9" FROM t WHERE note = 'costs $1 or ''$2''' AND id = 7 -- $3
		AND kind = '$1 off' /* $4 */`
	if got != want {
		t.Fatalf("substituted:\n%s\nwant:\n%s", got, want)
	}
}

func TestPlaceholderValuesAreStoredAsTheyAre(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{"BEGIN DOMAIN shop", "CREATE TABLE notes (id INT PRIMARY KEY, a TEXT, b TEXT)", "COMMIT", "BEGIN DOMAIN shop"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if _, err := conn.Exec(ctx, "INSERT INTO notes (id, a, b) VALUES ($1, $2, $3)", 1, "pay $3", "$2"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	var a, b string
	if err := conn.QueryRow(ctx, "SELECT a, b FROM shop.notes WHERE id = $1 AND a = 'pay $3'", 1).Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a != "pay $3" || b != "$2" {
		t.Fatalf("stored %q and %q, want \"pay $3\" and \"$2\"", a, b)
	}
}
