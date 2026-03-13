package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"asql/internal/engine/parser/ast"
	"asql/internal/engine/ports"
	"asql/internal/platform/datadir"
	auditstore "asql/internal/storage/audit"
)

type auditCommandOptions struct {
	DataDir   string
	Domains   []string
	Table     string
	Operation string
	FromLSN   uint64
	Limit     int
	Output    string
	Format    string
}

type auditPolicySummary struct {
	RetentionMode string `json:"retention_mode"`
	RetainForever bool   `json:"retain_forever"`
	PruningActive bool   `json:"pruning_active"`
}

type auditReport struct {
	Status          string             `json:"status"`
	Policy          auditPolicySummary `json:"policy"`
	EntryCount      int                `json:"entry_count"`
	StoreSizeBytes  int64              `json:"store_size_bytes"`
	OldestCommitLSN uint64             `json:"oldest_commit_lsn,omitempty"`
	NewestCommitLSN uint64             `json:"newest_commit_lsn,omitempty"`
	Operations      map[string]int     `json:"operations,omitempty"`
	Domains         map[string]int     `json:"domains,omitempty"`
	Tables          map[string]int     `json:"tables,omitempty"`
}

type auditExportEntry struct {
	CommitLSN uint64         `json:"commit_lsn"`
	Domain    string         `json:"domain"`
	Table     string         `json:"table"`
	Operation string         `json:"operation"`
	OldRow    map[string]any `json:"old_row,omitempty"`
	NewRow    map[string]any `json:"new_row,omitempty"`
}

type auditExportResult struct {
	Status          string             `json:"status"`
	Policy          auditPolicySummary `json:"policy"`
	EntryCount      int                `json:"entry_count"`
	Format          string             `json:"format,omitempty"`
	Output          string             `json:"output,omitempty"`
	SHA256          string             `json:"sha256,omitempty"`
	OldestCommitLSN uint64             `json:"oldest_commit_lsn,omitempty"`
	NewestCommitLSN uint64             `json:"newest_commit_lsn,omitempty"`
}

func runLocalAuditCommand(ctx context.Context, out io.Writer, command string, opts auditCommandOptions) error {
	entries, sizeBytes, err := loadAuditEntries(ctx, opts)
	if err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(command)) {
	case "audit-report":
		return printJSONTo(out, buildAuditReport(entries, sizeBytes))
	case "audit-export":
		return exportAuditEntries(out, entries, opts)
	default:
		return fmt.Errorf("unsupported local audit command %q", command)
	}
}

func loadAuditEntries(ctx context.Context, opts auditCommandOptions) ([]ports.AuditEntry, int64, error) {
	if strings.TrimSpace(opts.DataDir) == "" {
		return nil, 0, errors.New("audit command requires -data-dir")
	}
	dd, err := datadir.New(strings.TrimSpace(opts.DataDir))
	if err != nil {
		return nil, 0, fmt.Errorf("initialize data dir: %w", err)
	}
	store, err := auditstore.New(dd.AuditBasePath())
	if err != nil {
		return nil, 0, fmt.Errorf("open audit store: %w", err)
	}
	defer store.Close()

	sizeBytes, err := store.TotalSize()
	if err != nil {
		return nil, 0, fmt.Errorf("audit store size: %w", err)
	}

	entries, err := store.ReadFromLSN(ctx, opts.FromLSN, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("read audit entries: %w", err)
	}
	return filterAuditEntries(entries, opts), sizeBytes, nil
}

func filterAuditEntries(entries []ports.AuditEntry, opts auditCommandOptions) []ports.AuditEntry {
	allowedDomains := make(map[string]struct{}, len(opts.Domains))
	for _, domain := range opts.Domains {
		domain = strings.TrimSpace(strings.ToLower(domain))
		if domain != "" {
			allowedDomains[domain] = struct{}{}
		}
	}
	table := strings.TrimSpace(strings.ToLower(opts.Table))
	operation := strings.TrimSpace(strings.ToUpper(opts.Operation))

	filtered := make([]ports.AuditEntry, 0, len(entries))
	for _, entry := range entries {
		if len(allowedDomains) > 0 {
			if _, ok := allowedDomains[strings.ToLower(entry.Domain)]; !ok {
				continue
			}
		}
		if table != "" && strings.ToLower(entry.Table) != table {
			continue
		}
		if operation != "" && strings.ToUpper(entry.Operation) != operation {
			continue
		}
		filtered = append(filtered, entry)
		if opts.Limit > 0 && len(filtered) >= opts.Limit {
			break
		}
	}
	return filtered
}

func buildAuditReport(entries []ports.AuditEntry, sizeBytes int64) auditReport {
	report := auditReport{
		Status:         "reported",
		Policy:         defaultAuditPolicySummary(),
		EntryCount:     len(entries),
		StoreSizeBytes: sizeBytes,
		Operations:     make(map[string]int),
		Domains:        make(map[string]int),
		Tables:         make(map[string]int),
	}
	if len(entries) == 0 {
		return report
	}
	report.OldestCommitLSN = entries[0].CommitLSN
	report.NewestCommitLSN = entries[0].CommitLSN
	for _, entry := range entries {
		if entry.CommitLSN < report.OldestCommitLSN {
			report.OldestCommitLSN = entry.CommitLSN
		}
		if entry.CommitLSN > report.NewestCommitLSN {
			report.NewestCommitLSN = entry.CommitLSN
		}
		report.Operations[strings.ToUpper(entry.Operation)]++
		report.Domains[strings.ToLower(entry.Domain)]++
		report.Tables[strings.ToLower(entry.Domain)+"."+strings.ToLower(entry.Table)]++
	}
	return report
}

func exportAuditEntries(out io.Writer, entries []ports.AuditEntry, opts auditCommandOptions) error {
	format := strings.ToLower(strings.TrimSpace(opts.Format))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "jsonl" {
		return fmt.Errorf("unsupported audit export format %q", opts.Format)
	}

	exportEntries := make([]auditExportEntry, 0, len(entries))
	var oldest uint64
	var newest uint64
	for i, entry := range entries {
		if i == 0 || entry.CommitLSN < oldest {
			oldest = entry.CommitLSN
		}
		if entry.CommitLSN > newest {
			newest = entry.CommitLSN
		}
		exportEntries = append(exportEntries, auditExportEntry{
			CommitLSN: entry.CommitLSN,
			Domain:    entry.Domain,
			Table:     entry.Table,
			Operation: entry.Operation,
			OldRow:    normalizeAuditRow(entry.OldRow),
			NewRow:    normalizeAuditRow(entry.NewRow),
		})
	}

	content, err := encodeAuditExport(exportEntries, format)
	if err != nil {
		return err
	}

	if strings.TrimSpace(opts.Output) == "" {
		_, err := out.Write(content)
		if err == nil && format == "jsonl" && len(content) > 0 && content[len(content)-1] != '\n' {
			_, err = fmt.Fprintln(out)
		}
		return err
	}

	if err := os.WriteFile(opts.Output, content, 0o644); err != nil {
		return fmt.Errorf("write audit export: %w", err)
	}
	digest := sha256.Sum256(content)
	result := auditExportResult{
		Status:          "exported",
		Policy:          defaultAuditPolicySummary(),
		EntryCount:      len(exportEntries),
		Format:          format,
		Output:          opts.Output,
		SHA256:          hex.EncodeToString(digest[:]),
		OldestCommitLSN: oldest,
		NewestCommitLSN: newest,
	}
	return printJSONTo(out, result)
}

func encodeAuditExport(entries []auditExportEntry, format string) ([]byte, error) {
	switch format {
	case "json":
		return json.MarshalIndent(entries, "", "  ")
	case "jsonl":
		lines := make([]string, 0, len(entries))
		for _, entry := range entries {
			payload, err := json.Marshal(entry)
			if err != nil {
				return nil, err
			}
			lines = append(lines, string(payload))
		}
		if len(lines) == 0 {
			return []byte{}, nil
		}
		return []byte(strings.Join(lines, "\n") + "\n"), nil
	default:
		return nil, fmt.Errorf("unsupported audit export format %q", format)
	}
}

func defaultAuditPolicySummary() auditPolicySummary {
	return auditPolicySummary{
		RetentionMode: "retain_forever",
		RetainForever: true,
		PruningActive: false,
	}
}

func normalizeAuditRow(row map[string]ast.Literal) map[string]any {
	if len(row) == 0 {
		return nil
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	converted := make(map[string]any, len(row))
	for _, key := range keys {
		converted[key] = normalizeAuditLiteral(row[key])
	}
	return converted
}

func normalizeAuditLiteral(value ast.Literal) any {
	switch value.Kind {
	case ast.LiteralString:
		return value.StringValue
	case ast.LiteralNumber:
		return value.NumberValue
	case ast.LiteralBoolean:
		return value.BoolValue
	case ast.LiteralFloat:
		return value.FloatValue
	case ast.LiteralTimestamp:
		return value.NumberValue
	case ast.LiteralJSON:
		var parsed any
		if err := json.Unmarshal([]byte(value.StringValue), &parsed); err == nil {
			return parsed
		}
		return value.StringValue
	case ast.LiteralNull:
		return nil
	default:
		return nil
	}
}
