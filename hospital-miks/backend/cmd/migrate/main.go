// migrate.go – runs all SQL migrations against ASQL in order.
//
// ASQL requires each statement to be sent individually on the same connection,
// so we split migration files by ";" and execute each statement sequentially
// on a single pgx connection (not a pool).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	dsn := "postgres://asql:asql@127.0.0.1:5432/hospital_miks?sslmode=disable"
	if v := os.Getenv("DB_DSN"); v != "" {
		dsn = v
	}

	migrationsDir := "migrations"
	if len(os.Args) > 1 {
		migrationsDir = os.Args[1]
	}

	ctx := context.Background()

	// Use a single connection so the domain transaction stays open across statements.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		log.Fatalf("read migrations dir: %v", err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, f := range files {
		path := filepath.Join(migrationsDir, f)
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("read %s: %v", f, err)
		}

		if err := execStatements(ctx, conn, f, string(data)); err != nil {
			log.Fatalf("migration %s failed: %v", f, err)
		}
		fmt.Printf("✓ %s\n", f)
	}

	fmt.Println("\nAll migrations applied successfully.")
}

// execStatements splits a SQL file by semicolons and runs each statement
// sequentially on the same connection so BEGIN DOMAIN … COMMIT stays paired.
func execStatements(ctx context.Context, conn *pgx.Conn, file, sql string) error {
	stmts := splitStatements(sql)
	for i, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("statement %d (%s…): %w", i+1, truncate(stmt, 60), err)
		}
	}
	return nil
}

// splitStatements splits raw SQL by ";" while respecting SQL comments (--)
// and string literals ('...') so that semicolons inside those are not treated
// as statement separators. Comment lines are stripped from the output.
func splitStatements(sql string) []string {
	var stmts []string
	var buf strings.Builder
	inComment := false // inside a -- line comment
	inString := false  // inside a '...' string literal

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// End of -- comment at newline
		if inComment {
			if c == '\n' {
				inComment = false
			}
			// Drop comment characters from output
			continue
		}

		// Inside string literal
		if inString {
			buf.WriteByte(c)
			if c == '\'' {
				// Check for escaped quote ''
				if i+1 < len(sql) && sql[i+1] == '\'' {
					buf.WriteByte(sql[i+1])
					i++
				} else {
					inString = false
				}
			}
			continue
		}

		// Start of -- comment
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inComment = true
			i++ // skip the second '-'
			continue
		}

		// Start of string literal
		if c == '\'' {
			inString = true
			buf.WriteByte(c)
			continue
		}

		// Statement separator
		if c == ';' {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			buf.Reset()
			continue
		}

		buf.WriteByte(c)
	}

	// Trailing content without final semicolon
	if stmt := strings.TrimSpace(buf.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}

	return stmts
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n]
	}
	return s
}
