// Package determinism checks the determinism contract (AGENTS.md rules 1-6)
// end to end: every way of rebuilding engine state from the WAL must produce
// the same query-visible state as the engine that wrote it.
//
// The tests compare fingerprints: a canonical text rendering of everything a
// client can observe (schema, table contents in natural and sorted order,
// row history, entity versions, the commit timeline, time-travel reads at
// every commit LSN and timestamp resolution).
package determinism_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
)

// node is one engine over a data directory.
// Like the server, it wires the audit store, which serves FOR HISTORY.
type node struct {
	t         *testing.T
	dir       string
	opts      nodeOptions
	store     *wal.SegmentedLogStore
	audit     *audit.Store
	engine    *executor.Engine
	walPath   string
	auditPath string
	snapDir   string
}

type nodeOptions struct {
	segmentSize int64
	ledger      wal.LedgerMode
	engine      []executor.EngineOption
}

func openNode(t *testing.T, dir string, opts nodeOptions) *node {
	t.Helper()
	n := &node{t: t, dir: dir, opts: opts, walPath: filepath.Join(dir, "wal", "wal"),
		auditPath: filepath.Join(dir, "audit", "audit"), snapDir: filepath.Join(dir, "snap")}
	for _, d := range []string{filepath.Dir(n.walPath), filepath.Dir(n.auditPath), n.snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	n.open()
	return n
}

func (n *node) open() {
	n.t.Helper()
	walOpts := []wal.SegmentOption{wal.WithLedgerMode(n.opts.ledger)}
	if n.opts.segmentSize > 0 {
		walOpts = append(walOpts, wal.WithSegmentSize(n.opts.segmentSize))
	}
	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{}, walOpts...)
	if err != nil {
		n.t.Fatalf("open wal: %v", err)
	}
	auditStore, err := audit.New(n.auditPath)
	if err != nil {
		_ = store.Close()
		n.t.Fatalf("open audit store: %v", err)
	}
	opts := append([]executor.EngineOption{executor.WithAuditStore(auditStore)}, n.opts.engine...)
	engine, err := executor.New(context.Background(), store, n.snapDir, opts...)
	if err != nil {
		_ = store.Close()
		_ = auditStore.Close()
		n.t.Fatalf("open engine: %v", err)
	}
	n.store, n.audit, n.engine = store, auditStore, engine
}

// close shuts the node down cleanly: the engine writes its final checkpoint.
func (n *node) close() {
	n.t.Helper()
	if n.engine == nil {
		return
	}
	n.engine.WaitPendingSnapshots()
	n.closeStores()
}

func (n *node) closeStores() {
	n.t.Helper()
	if err := n.store.Close(); err != nil {
		n.t.Fatal(err)
	}
	if err := n.audit.Close(); err != nil {
		n.t.Fatal(err)
	}
	n.engine, n.store, n.audit = nil, nil, nil
}

// crash drops the node without a shutdown checkpoint.
func (n *node) crash() {
	n.t.Helper()
	n.closeStores()
}

func (n *node) restart() {
	n.close()
	n.open()
}

// records returns the node's whole WAL.
func (n *node) records() []ports.WALRecord {
	n.t.Helper()
	records, err := n.store.ReadFrom(context.Background(), 1, 0)
	if err != nil {
		n.t.Fatal(err)
	}
	return records
}

// copyDir copies a closed node's data directory.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// tx is one transaction of a workload.
type tx struct {
	domains    []string
	statements []string
	// rollback ends the transaction with ROLLBACK instead of COMMIT.
	rollback bool
}

func (n *node) run(txs ...tx) {
	n.t.Helper()
	ctx := context.Background()
	for _, t := range txs {
		session := n.engine.NewSession()
		begin := "BEGIN DOMAIN " + t.domains[0]
		if len(t.domains) > 1 {
			begin = "BEGIN CROSS DOMAIN " + strings.Join(t.domains, ", ")
		}
		end := "COMMIT"
		if t.rollback {
			end = "ROLLBACK"
		}
		for _, sql := range append(append([]string{begin}, t.statements...), end) {
			if _, err := n.engine.Execute(ctx, session, sql); err != nil {
				n.t.Fatalf("execute %q: %v", sql, err)
			}
		}
	}
}

// fingerprint is the canonical rendering of a node's observable state, by
// section.
type fingerprint struct {
	order    []string
	sections map[string]string
}

func (f *fingerprint) add(name, value string) {
	if f.sections == nil {
		f.sections = map[string]string{}
	}
	if _, dup := f.sections[name]; !dup {
		f.order = append(f.order, name)
	}
	f.sections[name] = value
}

// probes lists what a workload asks of the fingerprint beyond tables: the
// entities whose version history is compared.
type probes struct {
	entities []entityProbe
}

type entityProbe struct {
	domain, entity, rootPK string
}

func (n *node) fingerprint(p probes) fingerprint {
	n.t.Helper()
	ctx := context.Background()
	e := n.engine
	var f fingerprint
	head := e.HeadLSN()
	f.add("head", fmt.Sprint(head))

	schema := e.SchemaSnapshot(nil)
	f.add("schema", jsonString(n.t, schema))

	type tableRef struct{ domain, table string }
	var tables []tableRef
	for _, d := range schema.Domains {
		for _, t := range d.Tables {
			tables = append(tables, tableRef{d.Name, t.Name})
		}
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].domain+"."+tables[i].table < tables[j].domain+"."+tables[j].table
	})

	for _, t := range tables {
		ref := t.domain + "." + t.table
		rows, err := e.Query(ctx, fmt.Sprintf("SELECT * FROM %s", ref), []string{t.domain})
		f.addResult("rows "+ref, rows, err)
		hist, err := e.RowHistory(ctx, fmt.Sprintf("SELECT * FROM %s FOR HISTORY", ref), []string{t.domain})
		f.addResult("history "+ref, hist, err)
	}

	timeline, err := e.TimelineCommits(ctx, 0, head, "", 0)
	if err != nil {
		n.t.Fatalf("timeline: %v", err)
	}
	f.add("timeline", jsonString(n.t, timeline))

	for _, probe := range p.entities {
		history, err := e.EntityVersionHistory(ctx, probe.domain, probe.entity, probe.rootPK)
		f.add(fmt.Sprintf("entity %s.%s[%s]", probe.domain, probe.entity, probe.rootPK), fmt.Sprintf("%s err=%v", jsonString(n.t, history), err))
	}

	// Time travel at every commit, and timestamp resolution.
	for _, c := range timeline {
		var b strings.Builder
		for _, t := range tables {
			res, err := e.TimeTravelQueryAsOfLSN(ctx, fmt.Sprintf("SELECT * FROM %s.%s", t.domain, t.table), []string{t.domain}, c.LSN)
			fmt.Fprintf(&b, "%s.%s: %s\n", t.domain, t.table, renderResult(res, err))
		}
		f.add(fmt.Sprintf("as-of-lsn %d", c.LSN), b.String())
		lsn, err := e.LSNForTimestamp(ctx, c.Timestamp)
		f.add(fmt.Sprintf("ts %d", c.Timestamp), fmt.Sprintf("%d err=%v", lsn, err))
	}
	return f
}

func renderRecord(r ports.WALRecord) string {
	return fmt.Sprintf("lsn=%d term=%d ts=%d tx=%s type=%s payload=%x", r.LSN, r.Term, r.Timestamp, r.TxID, r.Type, r.Payload)
}

func jsonString(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// renderResult renders rows sorted, so that it compares content only.
func renderResult(res executor.Result, err error) string {
	content, _ := renderResultParts(res, err)
	return content
}

// renderResultParts renders rows sorted (content) and in their returned
// order, so a difference in natural order is reported apart from a
// difference in content.
func renderResultParts(res executor.Result, err error) (content, order string) {
	if err != nil {
		return "error: " + err.Error(), ""
	}
	rows := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		rows = append(rows, renderRow(row))
	}
	sorted := append([]string(nil), rows...)
	sort.Strings(sorted)
	return strings.Join(sorted, "; "), strings.Join(rows, "; ")
}

// addResult adds a result's content and, as a separate section, its
// natural row order.
func (f *fingerprint) addResult(name string, res executor.Result, err error) {
	content, order := renderResultParts(res, err)
	f.add(name, content)
	f.add(name+" (natural order)", order)
}

func renderRow(row map[string]ast.Literal) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := row[k]
		parts = append(parts, fmt.Sprintf("%s=%v:%q/%d/%t/%g", k, v.Kind, v.StringValue, v.NumberValue, v.BoolValue, v.FloatValue))
	}
	return strings.Join(parts, ",")
}

// historyIsUnreliable marks FOR HISTORY sections in comparisons where row
// history is known not to be reproducible yet: across crashes (the audit
// store is written asynchronously), on followers (they never write their
// audit store) and between independent runs (rows of one commit come back in
// no fixed order).
// TODO: drop once FOR HISTORY is reproducible from the WAL.
func historyIsUnreliable(section string) bool { return strings.HasPrefix(section, "history ") }

// without returns f minus the sections skip selects.
func (f fingerprint) without(skip func(string) bool) fingerprint {
	var out fingerprint
	for _, name := range f.order {
		if !skip(name) {
			out.add(name, f.sections[name])
		}
	}
	return out
}

// requireSame fails listing every differing section, with an excerpt of the
// first difference in each.
func requireSame(t *testing.T, label string, want, got fingerprint) {
	t.Helper()
	var diffs []string
	for _, name := range want.order {
		w, g := want.sections[name], got.sections[name]
		if w != g {
			diffs = append(diffs, fmt.Sprintf("section %q differs\n  want: %s\n  got:  %s", name, excerpt(w, g), excerpt(g, w)))
		}
	}
	for _, name := range got.order {
		if _, ok := want.sections[name]; !ok {
			diffs = append(diffs, fmt.Sprintf("unexpected section %q", name))
		}
	}
	if len(diffs) > 0 {
		t.Fatalf("%s: %d of %d sections differ:\n%s", label, len(diffs), len(want.order), strings.Join(diffs, "\n"))
	}
}

// excerpt returns the text of a around the first byte where a and b differ.
func excerpt(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	start := max(0, i-120)
	end := min(len(a), i+200)
	return fmt.Sprintf("…%s… (len %d)", a[start:end], len(a))
}
