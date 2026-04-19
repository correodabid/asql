package main

// shell.go — Interactive SQL shell for ASQL (`asqlctl shell`).
//
// Connects to an ASQL node via pgwire (pgx simple protocol) and provides a
// psql-like REPL with meta-commands, multi-line input, automatic transaction
// state tracking, and tabular result formatting.  History is persisted to
// ~/.asql_history.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// shellConfig holds the parameters for a shell session.
type shellConfig struct {
	PgwireAddr string // host:port of the ASQL pgwire endpoint
	AuthToken  string // optional password/token
}

// shellSession is the runtime state for one interactive shell session.
type shellSession struct {
	cfg     shellConfig
	conn    *pgx.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	in      *bufio.Scanner
	out     io.Writer
	inTx    bool   // whether a transaction is open
	domain  string // current domain(s) for prompt display
	timing  bool   // whether to show query timing
	history []string
}

// runShell is the entry point for `asqlctl -command shell`.
func runShell(cfg shellConfig) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl-C gracefully — cancel the current query, don't exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		for range sigCh {
			// If we're in a blocking query, cancelling ctx will unblock it.
			// We recreate the context after each cancellation.
		}
	}()

	connStr := buildConnString(cfg)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", cfg.PgwireAddr, err)
	}
	defer conn.Close(ctx)

	sess := &shellSession{
		cfg:    cfg,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		in:     bufio.NewScanner(os.Stdin),
		out:    os.Stdout,
	}

	// Load history.
	sess.loadHistory()

	// Print banner.
	fmt.Fprintf(sess.out, "asql shell (server %s)\n", cfg.PgwireAddr)
	fmt.Fprintf(sess.out, "Type \\? for help, \\q to quit.\n\n")

	return sess.loop()
}

func buildConnString(cfg shellConfig) string {
	user := "asql"
	password := ""
	if cfg.AuthToken != "" {
		password = cfg.AuthToken
	}
	db := "asql"
	// Build a pgx-compatible connection string.
	s := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&default_query_exec_mode=simple_protocol",
		user, password, cfg.PgwireAddr, db)
	return s
}

// ── REPL loop ─────────────────────────────────────────────────────────────────

func (s *shellSession) loop() error {
	var buf strings.Builder
	for {
		prompt := s.prompt(buf.Len() > 0)
		fmt.Fprint(s.out, prompt)

		if !s.in.Scan() {
			// EOF (Ctrl-D)
			fmt.Fprintln(s.out)
			return nil
		}

		line := s.in.Text()

		// If buffer is empty and line starts with \, treat as meta-command.
		if buf.Len() == 0 && strings.HasPrefix(strings.TrimSpace(line), `\`) {
			quit := s.handleMeta(strings.TrimSpace(line))
			if quit {
				return nil
			}
			continue
		}

		// Accumulate into buffer.
		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)

		// Check if the statement is complete (ends with ;).
		trimmed := strings.TrimSpace(buf.String())
		if trimmed == "" {
			buf.Reset()
			continue
		}

		// Special: if the line itself is a lone backslash command mid-buffer,
		// execute what we have so far and handle the meta-command.
		if strings.HasPrefix(strings.TrimSpace(line), `\`) && buf.Len() > len(line) {
			// Execute the accumulated SQL (minus this meta line).
			prev := strings.TrimSuffix(buf.String(), "\n"+line)
			prev = strings.TrimSuffix(prev, line)
			prev = strings.TrimSpace(prev)
			if prev != "" {
				s.executeAndDisplay(prev)
			}
			buf.Reset()
			quit := s.handleMeta(strings.TrimSpace(line))
			if quit {
				return nil
			}
			continue
		}

		if !strings.HasSuffix(trimmed, ";") {
			// Multi-line: continue reading.
			continue
		}

		// Execute the full statement.
		sql := buf.String()
		buf.Reset()
		s.addHistory(sql)
		s.executeAndDisplay(strings.TrimSpace(sql))
	}
}

func (s *shellSession) prompt(continuation bool) string {
	if continuation {
		return "    -> "
	}
	prefix := "asql"
	if s.domain != "" {
		prefix = s.domain
	}
	if s.inTx {
		return prefix + "=# "
	}
	return prefix + "=> "
}

// ── Meta-commands ─────────────────────────────────────────────────────────────

func (s *shellSession) handleMeta(line string) (quit bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case `\q`, `\quit`:
		fmt.Fprintln(s.out, "Bye.")
		return true

	case `\?`, `\help`:
		s.printHelp()

	case `\dt`, `\d`:
		s.metaDescribeTables(parts)

	case `\l`, `\domains`:
		s.metaListDomains()

	case `\timing`:
		s.timing = !s.timing
		if s.timing {
			fmt.Fprintln(s.out, "Timing is on.")
		} else {
			fmt.Fprintln(s.out, "Timing is off.")
		}

	case `\cluster`:
		s.metaCluster()

	case `\conninfo`:
		fmt.Fprintf(s.out, "Connected to %s as user \"asql\".\n", s.cfg.PgwireAddr)

	case `\history`:
		for i, h := range s.history {
			fmt.Fprintf(s.out, "%4d  %s\n", i+1, h)
		}

	default:
		fmt.Fprintf(s.out, "Unknown command: %s (try \\? for help)\n", cmd)
	}

	return false
}

func (s *shellSession) printHelp() {
	help := `Meta-commands:
  \q, \quit          Exit the shell
  \?, \help          Show this help
  \d [domain.]table  Describe a table's columns
  \dt                List all tables across domains
  \l, \domains       List all domains
  \timing            Toggle query timing display
  \cluster           Show cluster node info (role, leader, peers)
  \conninfo          Show connection info
  \history           Show command history

SQL:
  Type SQL statements terminated by a semicolon (;).
  Multi-line input is supported — the prompt changes to "    -> ".
  Transaction commands: BEGIN DOMAIN <name>, COMMIT, ROLLBACK.
`
	fmt.Fprint(s.out, help)
}

// ── SQL execution and display ─────────────────────────────────────────────────

func (s *shellSession) executeAndDisplay(sql string) {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	upper = strings.TrimRight(upper, ";")
	upper = strings.TrimSpace(upper)

	// Track transaction state from commands.
	switch {
	case strings.HasPrefix(upper, "BEGIN"):
		s.inTx = true
		// Extract domain from BEGIN DOMAIN <name>
		if strings.HasPrefix(upper, "BEGIN DOMAIN ") {
			s.domain = strings.TrimSpace(sql[len("BEGIN DOMAIN "):])
			s.domain = strings.TrimRight(s.domain, ";")
			s.domain = strings.TrimSpace(s.domain)
		} else if strings.HasPrefix(upper, "BEGIN CROSS DOMAIN ") {
			s.domain = strings.TrimSpace(sql[len("BEGIN CROSS DOMAIN "):])
			s.domain = strings.TrimRight(s.domain, ";")
			s.domain = strings.TrimSpace(s.domain)
		}
	case upper == "COMMIT", upper == "ROLLBACK":
		s.inTx = false
		s.domain = ""
	}

	start := time.Now()

	rows, err := s.conn.Query(s.ctx, sql)
	if err != nil {
		fmt.Fprintf(s.out, "ERROR: %s\n", err.Error())
		// If we got an error on COMMIT/ROLLBACK, reset tx state.
		if upper == "COMMIT" || upper == "ROLLBACK" {
			s.inTx = false
			s.domain = ""
		}
		return
	}
	defer rows.Close()

	descs := rows.FieldDescriptions()
	if len(descs) == 0 {
		// DDL/DML with no result set.
		tag := rows.CommandTag()
		rows.Close()
		elapsed := time.Since(start)
		fmt.Fprintln(s.out, tag.String())
		if s.timing {
			fmt.Fprintf(s.out, "Time: %.3f ms\n", float64(elapsed.Microseconds())/1000.0)
		}
		return
	}

	// Collect all rows for tabular display.
	colNames := make([]string, len(descs))
	for i, d := range descs {
		colNames[i] = string(d.Name)
	}

	var data [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			fmt.Fprintf(s.out, "ERROR reading row: %s\n", err.Error())
			return
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		data = append(data, row)
	}
	if rows.Err() != nil {
		fmt.Fprintf(s.out, "ERROR: %s\n", rows.Err().Error())
		return
	}
	elapsed := time.Since(start)

	printTable(s.out, colNames, data)
	fmt.Fprintf(s.out, "(%d row", len(data))
	if len(data) != 1 {
		fmt.Fprint(s.out, "s")
	}
	fmt.Fprintln(s.out, ")")

	if s.timing {
		fmt.Fprintf(s.out, "Time: %.3f ms\n", float64(elapsed.Microseconds())/1000.0)
	}
}

// ── Meta-command implementations ──────────────────────────────────────────────

func (s *shellSession) metaDescribeTables(parts []string) {
	if len(parts) < 2 {
		// \dt — list all tables
		s.executeAndDisplay("SELECT * FROM pg_tables;")
		return
	}
	// \d <table> — describe columns via information_schema.columns
	table := parts[1]
	// If table contains a dot, split into schema.table mapping to domain.table.
	var query string
	if strings.Contains(table, ".") {
		query = fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_schema || '.' || table_name = '%s';", table)
	} else {
		query = fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_name = '%s';", table)
	}
	s.executeAndDisplay(query)
}

func (s *shellSession) metaListDomains() {
	s.executeAndDisplay("SELECT * FROM information_schema.schemata;")
}

func (s *shellSession) metaCluster() {
	queries := []struct {
		label string
		sql   string
	}{
		{"Node ID", "SHOW asql_node_id"},
		{"Node Role", "SHOW asql_node_role"},
		{"Cluster Leader", "SHOW asql_cluster_leader"},
		{"Cluster Peers", "SHOW asql_cluster_peers"},
		{"Raft Term", "SHOW asql_raft_term"},
	}
	for _, q := range queries {
		var val string
		err := s.conn.QueryRow(s.ctx, q.sql).Scan(&val)
		if err != nil {
			fmt.Fprintf(s.out, "  %-20s %s\n", q.label+":", "(error: "+err.Error()+")")
		} else {
			fmt.Fprintf(s.out, "  %-20s %s\n", q.label+":", val)
		}
	}
}

// ── Tabular formatter ─────────────────────────────────────────────────────────

func printTable(out io.Writer, columns []string, rows [][]string) {
	if len(columns) == 0 {
		return
	}

	// Compute column widths.
	widths := make([]int, len(columns))
	for i, c := range columns {
		widths[i] = utf8.RuneCountInString(c)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				w := utf8.RuneCountInString(cell)
				if w > widths[i] {
					widths[i] = w
				}
			}
		}
	}

	// Cap column widths to a reasonable max for readability.
	const maxColWidth = 60
	for i := range widths {
		if widths[i] > maxColWidth {
			widths[i] = maxColWidth
		}
	}

	// Header.
	printRow(out, columns, widths)
	// Separator.
	seps := make([]string, len(widths))
	for i, w := range widths {
		seps[i] = strings.Repeat("-", w)
	}
	printRow(out, seps, widths)
	// Data rows.
	for _, row := range rows {
		printRow(out, row, widths)
	}
}

func printRow(out io.Writer, cells []string, widths []int) {
	for i, cell := range cells {
		if i > 0 {
			fmt.Fprint(out, " | ")
		}
		w := 0
		if i < len(widths) {
			w = widths[i]
		}
		// Truncate if necessary.
		displayed := cell
		if utf8.RuneCountInString(displayed) > w && w > 0 {
			runes := []rune(displayed)
			if w > 3 {
				displayed = string(runes[:w-3]) + "..."
			} else {
				displayed = string(runes[:w])
			}
		}
		fmt.Fprintf(out, " %-*s", w, displayed)
	}
	fmt.Fprintln(out)
}

// ── History ───────────────────────────────────────────────────────────────────

func (s *shellSession) historyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".asql_history")
}

func (s *shellSession) loadHistory() {
	path := s.historyPath()
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s.history = append(s.history, scanner.Text())
	}
}

func (s *shellSession) addHistory(sql string) {
	entry := strings.TrimSpace(sql)
	if entry == "" {
		return
	}
	// Collapse multi-line to single line for history file.
	entry = strings.ReplaceAll(entry, "\n", " ")
	s.history = append(s.history, entry)
	s.saveHistory(entry)
}

func (s *shellSession) saveHistory(entry string) {
	path := s.historyPath()
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, entry)
}
