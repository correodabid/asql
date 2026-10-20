package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
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

	txID := engine.newTxID()
	startLogicalTS := state.logicalTS

	session.activeTx = &transaction{id: txID, domains: []string{domain}, startLogicalTS: startLogicalTS, startLSN: state.headLSN, statements: make([]string, 0), plans: make([]planner.Plan, 0), savepoints: make([]savepointMarker, 0)}
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

	txID := engine.newTxID()
	startLogicalTS := state.logicalTS

	session.activeTx = &transaction{id: txID, domains: domainsList, startLogicalTS: startLogicalTS, startLSN: state.headLSN, statements: make([]string, 0), plans: make([]planner.Plan, 0), savepoints: make([]savepointMarker, 0)}
	engine.perf.recordBegin()
	engine.perf.recordCrossDomainBegin(len(domainsList))
	return Result{Status: "BEGIN", TxID: txID}, nil
}

// txIDPrefix starts every TxID: "tx-" and a number.
const txIDPrefix = "tx-"

// newTxID hands out the next TxID. The counter resumes after the highest
// TxID in the WAL (see noteTxID), so an ID names one transaction across
// restarts and leaders. WALs written before that may still repeat IDs, which
// is why replay matches a record to its commit by position (txCommitLSNs).
func (engine *Engine) newTxID() string {
	return txIDPrefix + strconv.FormatUint(atomic.AddUint64(&engine.txCount, 1), 10)
}

// txNum returns the number in a TxID, or false for an ID not made by
// newTxID.
func txNum(id string) (uint64, bool) {
	if !strings.HasPrefix(id, txIDPrefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(id[len(txIDPrefix):], 10, 64)
	return n, err == nil
}

// reserveTxID keeps newTxID from handing out id or anything before it.
func (engine *Engine) reserveTxID(id string) {
	if n, ok := txNum(id); ok {
		engine.reserveTxNum(n)
	}
}

func (engine *Engine) reserveTxNum(n uint64) {
	for {
		current := atomic.LoadUint64(&engine.txCount)
		if current >= n || atomic.CompareAndSwapUint64(&engine.txCount, current, n) {
			return
		}
	}
}

// noteTxID records that a transaction reached the WAL. Callers hold writeMu.
func (engine *Engine) noteTxID(id string) {
	n, ok := txNum(id)
	if !ok {
		return
	}
	engine.lastTxNum = max(engine.lastTxNum, n)
	engine.reserveTxNum(n)
}

// setLastTxNum restores lastTxNum from a snapshot. Callers hold writeMu.
func (engine *Engine) setLastTxNum(n uint64) {
	engine.lastTxNum = n
	engine.reserveTxNum(n)
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
		return Result{}, engine.failCommit(session, err)
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

	result := engine.commitQ.submit(job)
	if result.err != nil {
		return Result{}, engine.failCommit(session, result.err)
	}

	session.activeTx = nil
	return Result{Status: "COMMIT", CommitLSN: result.commitLSN}, nil
}

// failCommit ends the transaction of a COMMIT that failed, as PostgreSQL
// does: the COMMIT cannot be retried, and a transaction left open would fail
// the session's next BEGIN with 25001. That holds even when the outcome is
// unknown (Raft quorum unreachable): the session must find out by reading.
func (engine *Engine) failCommit(session *Session, err error) error {
	engine.rollback(session)
	return err
}

func (engine *Engine) validateWriteConflicts(state *readableState, tx *transaction, mutations []preparedMutation) error {
	if tx == nil || len(mutations) == 0 {
		return nil
	}

	// Only UPDATE and DELETE are checked: they change rows the transaction
	// selected by its WHERE, and that selection must still hold at commit.
	// INSERTs add new rows; PK and UNIQUE conflicts are enforced on apply.
	// DDL runs in its own schema-setup transactions and is not checked.
	for _, mutation := range mutations {
		plan := mutation.plan
		if plan.Operation != planner.OperationUpdate && plan.Operation != planner.OperationDelete {
			continue
		}
		domainState, ok := state.domains[plan.DomainName]
		if !ok {
			continue
		}
		table, ok := domainState.tables[plan.TableName]
		if !ok || table.lastMutationTS <= tx.startLogicalTS {
			continue
		}
		if err := engine.rowWriteConflict(state, tx, plan, table); err != nil {
			return err
		}
	}
	return nil
}

// rowWriteConflict reports whether a row the transaction's UPDATE or DELETE
// selects was changed since the transaction began: a change committed after
// it whose row before or after the change matches the WHERE. The first is a
// row the statement would have changed and someone else changed first; the
// second, a row that now matches and did not. So two transactions that
// update different rows of a table both commit, and the later of two that
// update the same row gets 40001, even when its WHERE no longer matches it
// (claiming a job WHERE state = 'pending').
//
// The changeLog is bounded. When entries newer than the transaction's start
// were trimmed, any change to the table since then is a conflict, as it was
// before rows were tracked.
func (engine *Engine) rowWriteConflict(state *readableState, tx *transaction, plan planner.Plan, table *tableState) error {
	target := plan.DomainName + "." + plan.TableName
	if table.changeLogTrimmedLSN > tx.startLSN {
		return fmt.Errorf("%w: table %s changed at ts=%d after tx snapshot ts=%d", errWriteConflict, target, table.lastMutationTS, tx.startLogicalTS)
	}
	for i := len(table.changeLog) - 1; i >= 0; i-- {
		entry := table.changeLog[i]
		if entry.commitLSN <= tx.startLSN {
			break
		}
		for _, row := range []map[string]ast.Literal{entry.oldRow, entry.newRow} {
			if row == nil {
				continue
			}
			if plan.Filter == nil || matchPredicate(row, plan.Filter, state, engine) {
				return fmt.Errorf("%w: a row of %s matching the %s was changed at lsn=%d after tx snapshot lsn=%d",
					errWriteConflict, target, strings.ToUpper(string(plan.Operation)), entry.commitLSN, tx.startLSN)
			}
		}
	}
	return nil
}

func (engine *Engine) rollback(session *Session) Result {
	tx := session.activeTx
	session.activeTx = nil
	if tx != nil && txHasMutations(tx) {
		engine.perf.recordRollback()
	} else {
		engine.perf.recordEndTx()
	}
	return Result{Status: "ROLLBACK"}
}

// txHasMutations reports whether the transaction contains any write statements.
// Read-only transactions (only SELECTs) are not counted as rollbacks.
func txHasMutations(tx *transaction) bool {
	for _, p := range tx.plans {
		if p.Operation != planner.OperationSelect && p.Operation != planner.OperationSetOp {
			return true
		}
	}
	return false
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
