package main

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@127.0.0.1:5433/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT * FROM asql_admin.engine_stats`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		log.Fatal("no engine stats row returned")
	}
	fields := rows.FieldDescriptions()
	raw := rows.RawValues()
	values := make(map[string]string, len(fields))
	for i, field := range fields {
		if i >= len(raw) || raw[i] == nil {
			values[string(field.Name)] = ""
			continue
		}
		values[string(field.Name)] = string(raw[i])
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	keys := []string{
		"total_commits",
		"total_snapshots",
		"commit_latency_p50_ms",
		"commit_latency_p95_ms",
		"commit_apply_p50_ms",
		"commit_apply_p95_ms",
		"commit_write_hold_p50_ms",
		"commit_write_hold_p95_ms",
		"fsync_latency_p50_ms",
		"fsync_latency_p95_ms",
		"wal_file_size_bytes",
		"snapshot_file_size_bytes",
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s=%s ", key, values[key])
	}
	fmt.Println()
}
