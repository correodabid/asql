package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"asql/internal/engine/parser"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

func (engine *Engine) beginDomain(session *Session, sql string) (Result, error) {
	if session.activeTx != nil {
		return Result{}, errTxActive
	}

	domain := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(sql, "BEGIN DOMAIN "), ";"))
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		return Result{}, errTxDomainMissing
	}

	// Fast path: domain already exists — skip writeMu entirely.
	state := engine.readState.Load()
	if _, exists := state.domains[domain]; !exists {
		// Slow path: first time seeing this domain — need writeMu for catalog + state mutation.
		engine.writeMu.Lock()
		engine.catalog.EnsureDomain(domain)
		state = engine.readState.Load() // re-read under lock
		if _, exists := state.domains[domain]; !exists {
			newState := state.cloneForMutation(map[string]struct{}{})
			newState.domains[domain] = &domainState{tables: make(map[string]*tableState)}
			newState.headLSN = engine.headLSN
			newState.logicalTS = engine.logicalTS
			engine.readState.Store(newState)
			state = newState
		}
		engine.writeMu.Unlock()
	}

	txNum := atomic.AddUint64(&engine.txCount, 1)
	txID := fmt.Sprintf("tx-%d", txNum)
	startLogicalTS := state.logicalTS

	session.activeTx = &transaction{id: txID, domains: []string{domain}, startLogicalTS: startLogicalTS, statements: make([]string, 0), plans: make([]planner.Plan, 0), savepoints: make([]savepointMarker, 0)}
	engine.perf.recordBegin()
	return Result{Status: "BEGIN", TxID: txID}, nil
}

func (engine *Engine) beginCrossDomain(session *Session, sql string) (Result, error) {
	if session.activeTx != nil {
		return Result{}, errTxActive
	}

	statement, err := parser.Parse(sql)
	if err != nil {
		return Result{}, fmt.Errorf("parse cross domain begin: %w", err)
	}

	begin, ok := statement.(ast.BeginCrossDomainStatement)
	if !ok {
		return Result{}, errors.New("invalid begin cross domain statement")
	}

	domainsList := make([]string, len(begin.Domains))
	copy(domainsList, begin.Domains)
	sort.Strings(domainsList)

	// Fast path: check if all domains already exist without writeMu.
	state := engine.readState.Load()
	allExist := true
	for _, domain := range domainsList {
		if _, exists := state.domains[domain]; !exists {
			allExist = false
			break
		}
	}

	if !allExist {
		// Slow path: need writeMu for catalog + state mutation.
		engine.writeMu.Lock()
		for _, domain := range domainsList {
			engine.catalog.EnsureDomain(domain)
		}

		state = engine.readState.Load() // re-read under lock
		needsSwap := false
		for _, domain := range domainsList {
			if _, exists := state.domains[domain]; !exists {
				needsSwap = true
				break
			}
		}
		if needsSwap {
			newState := state.cloneForMutation(map[string]struct{}{})
			for _, domain := range domainsList {
				if _, exists := newState.domains[domain]; !exists {
					newState.domains[domain] = &domainState{tables: make(map[string]*tableState)}
				}
			}
			newState.headLSN = engine.headLSN
			newState.logicalTS = engine.logicalTS
			engine.readState.Store(newState)
			state = newState
		}
		engine.writeMu.Unlock()
	}

	txNum := atomic.AddUint64(&engine.txCount, 1)
	txID := fmt.Sprintf("tx-%d", txNum)
	startLogicalTS := state.logicalTS

	session.activeTx = &transaction{id: txID, domains: domainsList, startLogicalTS: startLogicalTS, statements: make([]string, 0), plans: make([]planner.Plan, 0), savepoints: make([]savepointMarker, 0)}
	engine.perf.recordBegin()
	engine.perf.recordCrossDomainBegin(len(domainsList))
	return Result{Status: "BEGIN", TxID: txID}, nil
}

func parseSavepointName(sql string) (string, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	if !strings.HasPrefix(strings.ToUpper(trimmed), "SAVEPOINT ") {
		return "", errSavepointName
	}
	name := strings.ToLower(strings.TrimSpace(trimmed[len("SAVEPOINT "):]))
	if name == "" {
		return "", errSavepointName
	}
	if strings.ContainsAny(name, " \t\n\r") {
		return "", errSavepointName
	}
	return name, nil
}

func parseRollbackToSavepointName(sql string) (string, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	upper := strings.ToUpper(trimmed)

	const rollbackToSavepointPrefix = "ROLLBACK TO SAVEPOINT "
	const rollbackToPrefix = "ROLLBACK TO "

	name := ""
	switch {
	case strings.HasPrefix(upper, rollbackToSavepointPrefix):
		name = strings.TrimSpace(trimmed[len(rollbackToSavepointPrefix):])
	case strings.HasPrefix(upper, rollbackToPrefix):
		name = strings.TrimSpace(trimmed[len(rollbackToPrefix):])
	default:
		return "", errSavepointName
	}

	name = strings.ToLower(name)
	if name == "" {
		return "", errSavepointName
	}
	if strings.ContainsAny(name, " \t\n\r") {
		return "", errSavepointName
	}

	return name, nil
}

func (engine *Engine) savepoint(tx *transaction, name string) (Result, error) {
	if tx == nil {
		return Result{}, errTxRequired
	}

	tx.savepoints = append(tx.savepoints, savepointMarker{name: name, statementCount: len(tx.statements)})
	return Result{Status: "SAVEPOINT", TxID: tx.id}, nil
}

func (engine *Engine) rollbackToSavepoint(tx *transaction, name string) (Result, error) {
	if tx == nil {
		return Result{}, errTxRequired
	}

	markerIndex := -1
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].name == name {
			markerIndex = i
			break
		}
	}

	if markerIndex == -1 {
		return Result{}, fmt.Errorf("%w: %s", errSavepointMissing, name)
	}

	statementCount := tx.savepoints[markerIndex].statementCount
	if statementCount < len(tx.statements) {
		tx.statements = tx.statements[:statementCount]
		tx.plans = tx.plans[:statementCount]
	}

	tx.savepoints = tx.savepoints[:markerIndex+1]
	return Result{Status: "ROLLBACK TO", TxID: tx.id}, nil
}

func (engine *Engine) commit(ctx context.Context, session *Session) (Result, error) {
	if session.activeTx == nil {
		return Result{}, errTxRequired
	}

	commitStart := time.Now()

	tx := session.activeTx
	prepared, err := engine.prepareMutations(tx)
	if err != nil {
		return Result{}, err
	}

	// Preserve statement order inside a transaction so later statements can
	// resolve references against rows and entity versions made visible by
	// earlier statements in the same transaction.
	ordered := prepared

	// Collect affected domains for COW clone.
	affectedDomains := make(map[string]struct{}, len(tx.domains))
	for _, m := range ordered {
		affectedDomains[m.domain] = struct{}{}
	}

	// Pre-encode WAL payloads outside the write lock — binary encoding
	// is deterministic and doesn't need writeMu state.
	preEncodedPayloads := make([][]byte, len(ordered))
	for i, mutation := range ordered {
		preEncodedPayloads[i] = encodeMutationPayloadV2(mutation.domain, mutation.plan, mutation.sql)
	}

	// Check whether all mutations are DML (INSERT/UPDATE/DELETE).
	allDML := true
	for _, m := range ordered {
		switch m.plan.Operation {
		case planner.OperationInsert, planner.OperationUpdate, planner.OperationDelete:
			// pure DML — no catalog side-effects
		default:
			allDML = false
		}
	}

	// Submit to the commit queue and wait for result.
	job := &commitJob{
		ctx:         ctx,
		session:     session,
		tx:          tx,
		ordered:     ordered,
		preEncoded:  preEncodedPayloads,
		affected:    affectedDomains,
		allDML:      allDML,
		commitStart: commitStart,
		queuedAt:    time.Now(),
		resultCh:    make(chan commitJobResult, 1),
	}

	engine.perf.recordBegin()
	result := engine.commitQ.submit(job)
	if result.err != nil {
		return Result{}, result.err
	}

	session.activeTx = nil
	return Result{Status: "COMMIT", CommitLSN: result.commitLSN}, nil
}

func (engine *Engine) validateWriteConflicts(state *readableState, tx *transaction, mutations []preparedMutation) error {
	if tx == nil || len(mutations) == 0 {
		return nil
	}

	// Only UPDATE and DELETE require table-level conflict detection.
	// INSERTs add new rows and never conflict with other mutations at the
	// data level — PK uniqueness is enforced separately during apply.
	// DDL (CREATE TABLE, ALTER, CREATE INDEX) is always single-threaded
	// (schema setup phase), so it doesn't need conflict detection here.
	writeTargets := make(map[string]struct{})
	for _, mutation := range mutations {
		switch mutation.plan.Operation {
		case planner.OperationUpdate, planner.OperationDelete:
			key := mutation.plan.DomainName + "." + mutation.plan.TableName
			if mutation.plan.TableName != "" {
				writeTargets[key] = struct{}{}
			}
		}
	}

	for target := range writeTargets {
		parts := strings.SplitN(target, ".", 2)
		if len(parts) != 2 {
			continue
		}

		domainState, ok := state.domains[parts[0]]
		if !ok {
			continue
		}

		table, ok := domainState.tables[parts[1]]
		if !ok {
			continue
		}

		if table.lastMutationTS > tx.startLogicalTS {
			return fmt.Errorf("%w: table %s changed at ts=%d after tx snapshot ts=%d", errWriteConflict, target, table.lastMutationTS, tx.startLogicalTS)
		}
	}

	return nil
}

func (engine *Engine) rollback(session *Session) Result {
	tx := session.activeTx
	session.activeTx = nil
	if tx != nil && len(tx.statements) > 0 {
		engine.perf.recordRollback()
	} else {
		engine.perf.recordEndTx()
	}
	return Result{Status: "ROLLBACK"}
}

func (engine *Engine) prepareMutations(tx *transaction) ([]preparedMutation, error) {
	prepared := make([]preparedMutation, 0, len(tx.statements))

	// Use cached plans when available (populated during Execute).
	if len(tx.plans) == len(tx.statements) {
		for i, sql := range tx.statements {
			prepared = append(prepared, preparedMutation{domain: tx.plans[i].DomainName, sql: sql, plan: tx.plans[i]})
		}
		return prepared, nil
	}

	// Fallback: re-parse (e.g. replay path where plans aren't cached).
	for _, sql := range tx.statements {
		statement, err := parser.Parse(sql)
		if err != nil {
			return nil, fmt.Errorf("parse sql %q: %w", sql, err)
		}

		plan, err := planner.BuildForDomains(statement, tx.domains)
		if err != nil {
			return nil, fmt.Errorf("build plan: %w", err)
		}

		prepared = append(prepared, preparedMutation{domain: plan.DomainName, sql: sql, plan: plan})
	}

	return prepared, nil
}

func orderMutationsByDomain(mutations []preparedMutation, domains []string) []preparedMutation {
	if len(mutations) == 0 {
		return nil
	}

	orderedDomains := make([]string, len(domains))
	copy(orderedDomains, domains)
	sort.Strings(orderedDomains)

	buckets := make(map[string][]preparedMutation, len(orderedDomains))
	for _, mutation := range mutations {
		buckets[mutation.domain] = append(buckets[mutation.domain], mutation)
	}

	ordered := make([]preparedMutation, 0, len(mutations))
	for _, domain := range orderedDomains {
		ordered = append(ordered, buckets[domain]...)
	}

	return ordered
}
