package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// EntityChangeEvent describes one committed entity-version transition.
type EntityChangeEvent struct {
	Domain    string
	Entity    string
	RootPK    string
	Version   uint64
	CommitLSN uint64
	Timestamp uint64
	Tables    []string
}

// EntityChangesRequest scopes an entity change query.
type EntityChangesRequest struct {
	Domain  string
	Entity  string
	RootPK  string
	FromLSN uint64
	ToLSN   uint64
	Limit   int
}

// EntityChanges returns committed entity-version transitions ordered by commit
// LSN. The result is derived from the committed entity-version index, so it is
// deterministic and replay-safe.
func (engine *Engine) EntityChanges(ctx context.Context, request EntityChangesRequest) ([]EntityChangeEvent, error) {
	if engine == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Limit < 0 {
		return nil, fmt.Errorf("entity changes limit must be non-negative")
	}

	domainName := strings.ToLower(strings.TrimSpace(request.Domain))
	entityName := strings.ToLower(strings.TrimSpace(request.Entity))
	if domainName == "" {
		return nil, fmt.Errorf("entity changes requires a domain")
	}
	if entityName == "" {
		return nil, fmt.Errorf("entity changes requires an entity name")
	}

	state := engine.readState.Load()
	if state == nil {
		return nil, nil
	}
	domainState, ok := state.domains[domainName]
	if !ok || domainState == nil {
		return nil, fmt.Errorf("domain %q not found", domainName)
	}
	if domainState.entityVersions == nil {
		return nil, fmt.Errorf("entity %q not found in domain %q", entityName, domainName)
	}
	idx, ok := domainState.entityVersions[entityName]
	if !ok || idx == nil {
		return nil, fmt.Errorf("entity %q not found in domain %q", entityName, domainName)
	}

	flat := flattenEntityVersionIndex(idx)
	rootKeys := make([]string, 0)
	if strings.TrimSpace(request.RootPK) != "" {
		for _, candidate := range literalKeyCandidates(request.RootPK) {
			if _, exists := flat.versions[candidate]; exists {
				rootKeys = append(rootKeys, candidate)
				break
			}
		}
		if len(rootKeys) == 0 {
			return nil, nil
		}
	} else {
		rootKeys = make([]string, 0, len(flat.versions))
		for rootKey := range flat.versions {
			rootKeys = append(rootKeys, rootKey)
		}
		sort.Strings(rootKeys)
	}

	events := make([]EntityChangeEvent, 0)
	for _, rootKey := range rootKeys {
		versions := flat.versions[rootKey]
		for _, version := range versions {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if version.commitLSN == 0 || version.version == 0 {
				continue
			}
			if request.FromLSN > 0 && version.commitLSN < request.FromLSN {
				continue
			}
			if request.ToLSN > 0 && version.commitLSN > request.ToLSN {
				continue
			}
			tables := make([]string, len(version.tables))
			copy(tables, version.tables)
			events = append(events, EntityChangeEvent{
				Domain:    domainName,
				Entity:    entityName,
				RootPK:    formatLiteralKey(rootKey),
				Version:   version.version,
				CommitLSN: version.commitLSN,
				Tables:    tables,
			})
		}
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].CommitLSN != events[j].CommitLSN {
			return events[i].CommitLSN < events[j].CommitLSN
		}
		if events[i].RootPK != events[j].RootPK {
			return events[i].RootPK < events[j].RootPK
		}
		return events[i].Version < events[j].Version
	})

	if request.Limit > 0 && len(events) > request.Limit {
		events = events[:request.Limit]
	}
	if len(events) == 0 {
		return events, nil
	}
	timestampsByLSN, err := engine.resolveCommitTimestamps(ctx, events)
	if err != nil {
		return nil, err
	}
	for i := range events {
		events[i].Timestamp = timestampsByLSN[events[i].CommitLSN]
	}

	return events, nil
}

func (engine *Engine) resolveCommitTimestamps(ctx context.Context, events []EntityChangeEvent) (map[uint64]uint64, error) {
	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve entity change timestamps read wal: %w", err)
	}
	targets := make(map[uint64]struct{}, len(events))
	for _, event := range events {
		targets[event.CommitLSN] = struct{}{}
	}
	resolved := make(map[uint64]uint64, len(targets))
	for _, record := range records {
		if _, ok := targets[record.LSN]; !ok {
			continue
		}
		resolved[record.LSN] = record.Timestamp
		if len(resolved) == len(targets) {
			break
		}
	}
	if len(resolved) != len(targets) {
		missing := make([]uint64, 0, len(targets)-len(resolved))
		for target := range targets {
			if _, ok := resolved[target]; !ok {
				missing = append(missing, target)
			}
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		return nil, fmt.Errorf("resolve entity change timestamps missing WAL records for commit LSNs %v", missing)
	}
	return resolved, nil
}

// EntityChangesAsPrincipal returns entity changes only when the authenticated
// durable principal holds the explicit temporal-read privilege.
func (engine *Engine) EntityChangesAsPrincipal(ctx context.Context, request EntityChangesRequest, principal string) ([]EntityChangeEvent, error) {
	if err := engine.AuthorizeHistoricalRead(principal); err != nil {
		return nil, err
	}
	return engine.EntityChanges(ctx, request)
}

// HeadLSN returns the latest committed LSN currently visible in the engine's
// read state.
func (engine *Engine) HeadLSN() uint64 {
	if engine == nil {
		return 0
	}
	state := engine.readState.Load()
	if state == nil {
		return 0
	}
	return state.headLSN
}

func formatLiteralKey(key string) string {
	if len(key) < 2 || key[1] != ':' {
		return key
	}
	suffix := key[2:]
	switch key[0] {
	case 's', 'n', 'f', 't', 'j':
		return suffix
	case 'b':
		if suffix == "1" {
			return "true"
		}
		if suffix == "0" {
			return "false"
		}
		return suffix
	case 'z':
		return "null"
	default:
		return suffix
	}
}
