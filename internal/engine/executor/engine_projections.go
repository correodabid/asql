package executor

import (
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

// fanoutProjections mirrors DML mutations from a source domain table into all
// subscriber domains that hold a versioned-foreign-key projection of that table.
//
// It must be called after applyPlanToStateTracked succeeds, while still under
// writeMu. state is the mutable COW clone being built for this commit batch;
// subscriber domains are guaranteed to be shallow-cloned in state before this
// function runs (see addProjectionDomainsToAffected in processCommitBatch).
//
// Fan-out uses a full-sync strategy (v1): for every DML mutation on a source
// table that has subscribers, the projected table is rebuilt from the current
// source rows.  This is O(N) per mutation against a subscribed table but ensures
// correctness across INSERT / UPDATE / DELETE without tracking per-row deltas.
// Incremental fan-out can be added later as a performance optimisation.
func (engine *Engine) fanoutProjections(state *readableState, plan planner.Plan) {
	if len(engine.vfkSubscriptions) == 0 {
		return
	}

	subKey := plan.DomainName + "." + plan.TableName
	subs := engine.vfkSubscriptions[subKey]
	if len(subs) == 0 {
		return
	}

	srcDS := state.domains[plan.DomainName]
	if srcDS == nil {
		return
	}
	srcTable := srcDS.tables[plan.TableName]
	if srcTable == nil {
		return
	}

	for _, sub := range subs {
		subDS := state.domains[sub.subscriberDomain]
		if subDS == nil {
			continue
		}
		projTable := subDS.tables[sub.projTableName]
		if projTable == nil {
			continue
		}
		// Rebuild projected table from current source rows and update the
		// subscriber domain's tables map (which is already COW-mutable).
		subDS.tables[sub.projTableName] = rebuildProjectionFromSource(srcTable)
	}
}

// rebuildProjectionFromSource creates a fresh projected tableState that mirrors
// src's schema (isProjection=true, no VFKs) and contains a clone of all rows
// from src with flat indexes rebuilt from scratch.
func rebuildProjectionFromSource(src *tableState) *tableState {
	ts := cloneTableStateProjection(src)

	for rowID, row := range src.rows {
		cloned := make([]ast.Literal, len(row))
		copy(cloned, row)
		ts.rows = append(ts.rows, cloned)

		// Update flat indexes for each configured index on the projected table.
		for _, idx := range ts.indexes {
			entry, exists := buildIndexEntryForRow(idx, cloned, ts.columnIndex, rowID)
			if !exists {
				continue
			}
			addIndexEntry(idx, entry)
		}
	}

	// Finalise baseSize for all indexes (used by adaptive overlay depth).
	for _, idx := range ts.indexes {
		if idx.kind == "hash" {
			idx.baseSize = len(idx.buckets)
		} else {
			idx.baseSize = len(idx.entries)
		}
	}

	return ts
}

// addProjectionDomainsToAffected extends the affected-domain set to include
// every subscriber domain that will receive a projection fan-out from the
// given batch of commit jobs. Called before cloneForMutation so that subscriber
// domain states are shallow-cloned and ready for write by fanoutProjections.
func (engine *Engine) addProjectionDomainsToAffected(allAffected map[string]struct{}, jobs []*commitJob) {
	if len(engine.vfkSubscriptions) == 0 {
		return
	}
	for _, job := range jobs {
		for _, mutation := range job.ordered {
			subKey := mutation.plan.DomainName + "." + mutation.plan.TableName
			for _, sub := range engine.vfkSubscriptions[subKey] {
				allAffected[sub.subscriberDomain] = struct{}{}
			}
		}
	}
}

// extendAffectedWithProjections returns an extended copy of affected that also
// includes subscriber domains reachable by any of the given plans. Used by
// saveTableRefs to ensure fan-out domains are captured for rollback.
func (engine *Engine) extendAffectedWithProjections(affected map[string]struct{}, mutations []preparedMutation) map[string]struct{} {
	if len(engine.vfkSubscriptions) == 0 {
		return affected
	}
	extra := make(map[string]struct{}, len(affected)+4)
	for k := range affected {
		extra[k] = struct{}{}
	}
	for _, m := range mutations {
		subKey := m.plan.DomainName + "." + m.plan.TableName
		for _, sub := range engine.vfkSubscriptions[subKey] {
			extra[sub.subscriberDomain] = struct{}{}
		}
	}
	return extra
}
