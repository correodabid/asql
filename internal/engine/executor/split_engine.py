#!/usr/bin/env python3
"""Split engine.go into logical files. Run from the executor directory."""

import re
import sys
from collections import defaultdict

# Which file each function/type goes to. Key = function/type name pattern (start of line)
# Value = target filename (without .go extension)
ASSIGNMENTS = {
    # engine_types.go — All type definitions
    'type Result struct': 'engine_types',
    'type Session struct': 'engine_types',
    'func (session *Session) InTransaction': 'engine_types',
    'func (session *Session) ActiveDomains': 'engine_types',
    'type transaction struct': 'engine_types',
    'type savepointMarker struct': 'engine_types',
    'type engineState struct': 'engine_types',
    'type readableState struct': 'engine_types',
    'func (s *readableState) cloneForMutation': 'engine_types',
    'func (d *domainState) shallowClone': 'engine_types',
    'func (s *readableState) toEngineState': 'engine_types',
    'func cloneDomains(': 'engine_types',
    'func cloneEntities(': 'engine_types',
    'func cloneEntityVersions(': 'engine_types',
    'type domainState struct': 'engine_types',
    'type tableState struct': 'engine_types',
    'type foreignKeyConstraint struct': 'engine_types',
    'type versionedForeignKeyConstraint struct': 'engine_types',
    'type checkConstraint struct': 'engine_types',
    'type entityDefinition struct': 'engine_types',
    'type fkHop struct': 'engine_types',
    'type entityVersionIndex struct': 'engine_types',
    'type entityVersion struct': 'engine_types',
    'type indexState struct': 'engine_types',
    'func (idx *indexState) lookupBucket': 'engine_types',
    'func (idx *indexState) hasBucket': 'engine_types',
    'func (idx *indexState) foreachBucket': 'engine_types',
    'func (idx *indexState) overlayDepth': 'engine_types',
    'func flattenIndex(': 'engine_types',
    'func (idx *indexState) allEntries': 'engine_types',
    'func mergeSortedEntries(': 'engine_types',
    'type indexEntry struct': 'engine_types',
    'type scanStrategy string': 'engine_types',
    'type mutationPayload struct': 'engine_types',
    'type preparedMutation struct': 'engine_types',
    'type MigrationValidationReport struct': 'engine_types',
    'type SchemaSnapshot struct': 'engine_types',
    'type SchemaDomain struct': 'engine_types',
    'type SchemaTable struct': 'engine_types',
    'type SchemaIndex struct': 'engine_types',
    'type SchemaVersionedFK struct': 'engine_types',
    'type SchemaEntity struct': 'engine_types',
    'type SchemaColumn struct': 'engine_types',
    'type EntityVersionHistoryEntry struct': 'engine_types',
    'type aggregateSelectSpec struct': 'engine_types',
    'type scanCostEstimate struct': 'engine_types',
    'type ternaryResult int': 'engine_types',
    'type windowPartition struct': 'engine_types',

    # engine_tx.go — Transaction management
    'func (engine *Engine) beginDomain': 'engine_tx',
    'func (engine *Engine) beginCrossDomain': 'engine_tx',
    'func parseSavepointName(': 'engine_tx',
    'func parseRollbackToSavepointName(': 'engine_tx',
    'func (engine *Engine) savepoint': 'engine_tx',
    'func (engine *Engine) rollbackToSavepoint': 'engine_tx',
    'func (engine *Engine) commit(': 'engine_tx',
    'func (engine *Engine) validateWriteConflicts': 'engine_tx',
    'func (engine *Engine) rollback(': 'engine_tx',
    'func (engine *Engine) prepareMutations': 'engine_tx',
    'func orderMutationsByDomain(': 'engine_tx',

    # engine_replay.go — WAL replay & recovery
    'func (engine *Engine) Replay(': 'engine_replay',
    'func (engine *Engine) replayFromSnapshots': 'engine_replay',
    'func (engine *Engine) ReplayToLSN': 'engine_replay',
    'func (engine *Engine) LSNForTimestamp': 'engine_replay',
    'func (engine *Engine) buildStateFromRecords': 'engine_replay',
    'func (engine *Engine) rebuildFromRecords(': 'engine_replay',
    'func (engine *Engine) rebuildFromRecordsAfterSnapshot': 'engine_replay',
    'func (engine *Engine) rebuildFromRecordsPartial': 'engine_replay',
    'func buildTxCommitLSNIndex(': 'engine_replay',

    # engine_history.go — Time-travel & history
    'func (engine *Engine) TimeTravelQueryAsOfLSN': 'engine_history',
    'func (engine *Engine) TimeTravelQueryAsOfTimestamp': 'engine_history',
    'func (engine *Engine) RowHistory': 'engine_history',
    'func (engine *Engine) EntityVersionHistory': 'engine_history',
    'func deduplicateSortedLSNs(': 'engine_history',
    'func detectRowChanges(': 'engine_history',
    'func rowContentKey(': 'engine_history',
    'func (engine *Engine) recordScanStrategy': 'engine_history',
    'func (engine *Engine) scanStrategySnapshot': 'engine_history',
    'func (engine *Engine) mergeScanStrategyCounts': 'engine_history',
    'func (engine *Engine) ScanStrategyCounts': 'engine_history',

    # engine_mutations.go — DML state application
    'func (engine *Engine) applyMutationToState(': 'engine_mutations',
    'func (engine *Engine) applyMutationToStateWithEntityTracking': 'engine_mutations',
    'func (engine *Engine) applyPlanToState(': 'engine_mutations',
    'func (engine *Engine) applyPlanToStateTracked': 'engine_mutations',
    'func (engine *Engine) validateMutationsCanApply': 'engine_mutations',
    'func (engine *Engine) ValidateMigrationPlan': 'engine_mutations',
    'func buildShadowEngine(': 'engine_mutations',
    'func isMigrationMutationOperation(': 'engine_mutations',
    'func cloneEngineState(': 'engine_mutations',
    'func shareTableForInsert(': 'engine_mutations',
    'func overlayIndexForInsert(': 'engine_mutations',
    'func cloneTableState(': 'engine_mutations',
    'func cloneIndexState(': 'engine_mutations',
    'func clonePredicate(': 'engine_mutations',

    # engine_entity.go — Entity version tracking
    'func resolveEntityRootPK(': 'engine_entity',
    'func collectEntityMutations(': 'engine_entity',
    'func recordEntityVersions(': 'engine_entity',
    'func findEntityForTableInDomain(': 'engine_entity',
    'func resolveEntityVersionCommitLSN(': 'engine_entity',
    'func latestEntityVersion(': 'engine_entity',
    'func resolveEntityFKPath(': 'engine_entity',
    'func (engine *Engine) recordMutationLSN(': 'engine_entity',
    'func (engine *Engine) recordMutationLSNFromSQL': 'engine_entity',
    'func (engine *Engine) rebuildMutationIndex': 'engine_entity',
    'func rebuildTableIndexes(': 'engine_entity',

    # engine_constraints.go — Validation & defaults
    'func validateInsertRow(': 'engine_constraints',
    'func (engine *Engine) expandDomainsForVFKJoins': 'engine_constraints',
    'func validateVersionedForeignKeys(': 'engine_constraints',
    'func (engine *Engine) tableContainsValueAtLSN': 'engine_constraints',
    'func resolveDefaults(': 'engine_constraints',
    'func resolveVFKVersions(': 'engine_constraints',
    'func nextAutoIncrement(': 'engine_constraints',
    'func generateUUIDv7(': 'engine_constraints',
    'func flattenRow(': 'engine_constraints',
    'func buildReturningRow(': 'engine_constraints',
    'func rebuildInsertSQL(': 'engine_constraints',
    'func validateConstraints(': 'engine_constraints',
    'func checkConstraintSatisfied(': 'engine_constraints',
    'func validateForeignKeyDefinitions(': 'engine_constraints',

    # engine_query.go — SELECT, aggregation, window, subqueries
    'func (engine *Engine) selectRows': 'engine_query',
    'func valueOrNull(': 'engine_query',
    'func requiresAggregation(': 'engine_query',
    'func aggregateRows(': 'engine_query',
    'func buildGroupKey(': 'engine_query',
    'func parseAggregateSelectColumn(': 'engine_query',
    'func computeAggregate(': 'engine_query',
    'func applyWindowFunctions(': 'engine_query',
    'func windowPartitionRows(': 'engine_query',
    'func windowSortPartition(': 'engine_query',
    'func windowOrderByEqual(': 'engine_query',
    'func executeSubquery(': 'engine_query',
    'func evaluateExistsSubquery(': 'engine_query',
    'func evaluateInSubquery(': 'engine_query',
    'func evaluateScalarSubquery(': 'engine_query',
    'func cloneRow(': 'engine_query',

    # engine_joins.go — Join pipeline + CTE
    'func (engine *Engine) executeJoinPipeline': 'engine_joins',
    'func nullFilledRow(': 'engine_joins',
    'func buildAliasMap(': 'engine_joins',
    'func displayPrefix(': 'engine_joins',
    'func prefixRow(': 'engine_joins',
    'func mergePipelineRows(': 'engine_joins',
    'func resolveJoinColumnRef(': 'engine_joins',
    'func extractPipelineJoinValue(': 'engine_joins',
    'func findVFKForJoin(': 'engine_joins',
    'func applyImports(': 'engine_joins',
    'func materializeCTEs(': 'engine_joins',
    'func domainsFromState(': 'engine_joins',
    'func injectVirtualTable(': 'engine_joins',

    # engine_scan.go — Scan strategies & index scanning
    'func chooseSingleTableScanStrategy(': 'engine_scan',
    'func estimateFullScanCost(': 'engine_scan',
    'func estimateHashLookupCost(': 'engine_scan',
    'func estimateBTreeOrderCost(': 'engine_scan',
    'func estimateCompositeBTreeOrderCost(': 'engine_scan',
    'func estimateBTreePrefixCost(': 'engine_scan',
    'func estimateBTreeLookupCost(': 'engine_scan',
    'func estimateRowsByIndexPredicate(': 'engine_scan',
    'func pickBestScanStrategy(': 'engine_scan',
    'func rowsForPredicate(': 'engine_scan',
    'func orderedRowsFromBTreeIndex(': 'engine_scan',
    'func orderedRowsFromCompositeBTreeIndex(': 'engine_scan',
    'func compositeBTreeIndexForOrder(': 'engine_scan',
    'func orderedRowsFromBTreePrefix(': 'engine_scan',
    'func sortRowsWithinFirstOrderGroups(': 'engine_scan',
    'func btreeBoundDecision(': 'engine_scan',
    'func joinCandidateRows(': 'engine_scan',
    'func chooseJoinScanStrategy(': 'engine_scan',
    'func hasJoinIndex(': 'engine_scan',
    'func indexForColumn(': 'engine_scan',
    'func joinColumnForTable(': 'engine_scan',

    # engine_index.go — Index entry operations
    'func rowIDsForPredicate(': 'engine_index',
    'func rowIDsForEquality(': 'engine_index',
    'func insertIndexEntry(': 'engine_index',
    'func compareIndexEntries(': 'engine_index',
    'func buildIndexEntryForRow(': 'engine_index',
    'func indexColumnSetKey(': 'engine_index',

    # engine_predicates.go — Predicate evaluation & literals
    'func compareLiteralByOperator(': 'engine_predicates',
    'func isSimplePredicate(': 'engine_predicates',
    'func isSimpleColumnReference(': 'engine_predicates',
    'func evaluatePredicate3VL(': 'engine_predicates',
    'func resolvePredicateOperand(': 'engine_predicates',
    'func evaluateNumericExpression(': 'engine_predicates',
    'func tokenizeArithmeticExpression(': 'engine_predicates',
    'func isArithmeticOperator(': 'engine_predicates',
    'func arithmeticPrecedence(': 'engine_predicates',
    'func resolveArithmeticToken(': 'engine_predicates',
    'func applyArithmeticOperator(': 'engine_predicates',
    'func literalKey(': 'engine_predicates',
    'func extractJoinValue(': 'engine_predicates',
    'func literalEqual(': 'engine_predicates',
    'func mergeQualifiedRows(': 'engine_predicates',
    'func tableColumnNames(': 'engine_predicates',
    'func rowsEqual(': 'engine_predicates',
    'func compareLiterals(': 'engine_predicates',
    'func literalRank(': 'engine_predicates',
    'func matchPredicate(': 'engine_predicates',

    # engine_schema.go — Schema snapshot
    'func (engine *Engine) SchemaSnapshot': 'engine_schema',
    'func (table *tableState) columnIsUnique': 'engine_schema',
    'func schemaIndexes(': 'engine_schema',
    'func schemaVersionedFKs(': 'engine_schema',
    'func schemaEntities(': 'engine_schema',
    'func tableHasColumn(': 'engine_schema',
    'func tableContainsValue(': 'engine_schema',
    'func getOrCreateDomain(': 'engine_schema',
    'func allDomainKeys(': 'engine_schema',
    'func decodeCanonical(': 'engine_schema',
}


def parse_blocks(lines):
    """Parse engine.go into blocks. Each block is a contiguous section
    (function, type, const, var, or comment block) identified by its start line.

    Handles multi-line function signatures where '{' is not on the first line.
    """
    blocks = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]
        stripped = line.rstrip()

        # Check if this line starts a type or func definition
        is_definition = (stripped.startswith('func ') or
                        stripped.startswith('type ') or
                        stripped.startswith('const ') or
                        stripped.startswith('var '))

        if is_definition:
            # Look backwards for preceding comments
            block_start = i
            while block_start > 0 and lines[block_start - 1].rstrip().startswith('//'):
                block_start -= 1

            # Find the end of this block
            if stripped.startswith('type ') and stripped.endswith('{'):
                # Multi-line type - find closing brace
                brace_depth = 1
                j = i + 1
                while j < n and brace_depth > 0:
                    for ch in lines[j]:
                        if ch == '{':
                            brace_depth += 1
                        elif ch == '}':
                            brace_depth -= 1
                    j += 1
                block_end = j

            elif stripped.startswith('func '):
                # Function definition - may have multi-line signature
                # First, find the opening brace (could be on a later line)
                j = i
                found_brace = False
                while j < n:
                    line_j = lines[j].rstrip()
                    if '{' in line_j:
                        found_brace = True
                        break
                    j += 1

                if not found_brace:
                    # No brace found at all (shouldn't happen in valid Go)
                    block_end = i + 1
                else:
                    # Now j points to the line with the opening '{'.
                    # Count braces from line i through the end of function.
                    brace_depth = 0
                    k = i
                    while k <= j:
                        brace_depth += lines[k].count('{') - lines[k].count('}')
                        k += 1
                    # Continue finding the closing brace
                    while k < n and brace_depth > 0:
                        brace_depth += lines[k].count('{') - lines[k].count('}')
                        k += 1
                    block_end = k

            elif stripped.startswith('type ') and not stripped.endswith('{'):
                # Single-line type definition
                block_end = i + 1
            elif stripped.startswith('const ') and stripped.endswith('('):
                # Const block
                j = i + 1
                while j < n and not lines[j].rstrip().startswith(')'):
                    j += 1
                block_end = j + 1
            elif stripped.startswith('var ') and stripped.endswith('('):
                # Var block
                j = i + 1
                while j < n and not lines[j].rstrip().startswith(')'):
                    j += 1
                block_end = j + 1
            else:
                block_end = i + 1

            # Include trailing blank lines (one blank line after)
            while block_end < n and lines[block_end].strip() == '':
                block_end += 1

            blocks.append({
                'start': block_start,
                'end': block_end,
                'definition_line': lines[i].rstrip(),
                'lines': lines[block_start:block_end],
            })
            i = block_end
        else:
            i += 1

    return blocks


def assign_block(block):
    """Determine which file a block belongs to."""
    defn = block['definition_line']
    for pattern, target in ASSIGNMENTS.items():
        if defn.startswith(pattern):
            return target
    return 'engine'  # Default: stays in engine.go


def main():
    with open('engine.go', 'r') as f:
        lines = f.readlines()

    # Find the import block boundaries
    import_start = None
    import_end = None
    for i, line in enumerate(lines):
        stripped = line.rstrip()
        if stripped == 'import (':
            import_start = i
        elif import_start is not None and stripped == ')':
            import_end = i + 1
            break

    # Parse blocks starting after imports
    blocks = parse_blocks(lines)

    # Assign blocks to files
    file_blocks = defaultdict(list)
    for block in blocks:
        target = assign_block(block)
        file_blocks[target].append(block)

    # Report
    for target, blocks_list in sorted(file_blocks.items()):
        total_lines = sum(len(b['lines']) for b in blocks_list)
        print(f"{target}.go: {len(blocks_list)} blocks, ~{total_lines} lines")

    # Write each file
    for target, blocks_list in sorted(file_blocks.items()):
        if target == 'engine':
            continue  # Handle engine.go specially

        # Collect all lines for this file
        all_block_lines = []
        for b in blocks_list:
            all_block_lines.extend(b['lines'])

        block_text = ''.join(all_block_lines)
        needed_imports = set()

        # Standard library imports - check for package usage
        import_checks = {
            'context': 'context.',
            'fmt': 'fmt.',
            'math': 'math.',
            'sort': 'sort.',
            'strings': 'strings.',
            'strconv': 'strconv.',
            'time': 'time.',
            'sync': 'sync.',
        }
        for imp, pattern in import_checks.items():
            if pattern in block_text:
                needed_imports.add(f'"{imp}"')

        # Special: sync/atomic
        if 'atomic.' in block_text:
            needed_imports.add('"sync/atomic"')
        # sync.Mutex etc still require sync
        if 'sync.Mutex' in block_text or 'sync.RWMutex' in block_text or 'sync.WaitGroup' in block_text or 'sync.Once' in block_text:
            needed_imports.add('"sync"')
        # If we only use atomic, we don't need sync unless explicitly used
        if 'sync.' not in block_text:
            needed_imports.discard('"sync"')

        # Special: encoding
        if 'binary.' in block_text:
            needed_imports.add('"encoding/binary"')
        if 'json.' in block_text:
            needed_imports.add('"encoding/json"')

        # Special: log/slog
        if 'slog.' in block_text:
            needed_imports.add('"log/slog"')

        # Special: crypto/rand (aliased as crand)
        if 'crand.' in block_text:
            needed_imports.add('crand "crypto/rand"')
        # math/rand
        if 'rand.New(' in block_text or 'rand.Source' in block_text or 'rand.Intn' in block_text or 'rand.Int63' in block_text:
            needed_imports.add('"math/rand"')

        # Project imports — scan the original import block
        if import_start is not None and import_end is not None:
            for imp_line in lines[import_start+1:import_end-1]:
                stripped = imp_line.strip()
                if not stripped or stripped.startswith('//'):
                    continue
                # Handle aliased imports: `alias "path/to/pkg"`
                parts_ws = stripped.split()
                if len(parts_ws) == 2:
                    # aliased import
                    alias = parts_ws[0]
                    if alias + '.' in block_text:
                        needed_imports.add(stripped)
                else:
                    # unaliased import — extract package name (last path element)
                    pkg_path = stripped.strip('"')
                    pkg_name = pkg_path.split('/')[-1]
                    if pkg_name + '.' in block_text:
                        needed_imports.add(stripped)

        # Build file content
        file_content = 'package executor\n\n'
        if needed_imports:
            # Sort: std lib first, then project imports
            std_imports = sorted([i for i in needed_imports if not i.strip('"').startswith('asql/')])
            project_imports = sorted([i for i in needed_imports if i.strip('"').startswith('asql/')])
            file_content += 'import (\n'
            for imp in std_imports:
                file_content += f'\t{imp}\n'
            if std_imports and project_imports:
                file_content += '\n'
            for imp in project_imports:
                file_content += f'\t{imp}\n'
            file_content += ')\n\n'

        for b in blocks_list:
            file_content += ''.join(b['lines'])

        filename = f'{target}.go'
        with open(filename, 'w') as f:
            f.write(file_content)
        print(f"Wrote {filename}")

    # Now write the remaining engine.go
    # Collect all line ranges that were extracted
    extracted_ranges = set()
    for target, blocks_list in file_blocks.items():
        if target == 'engine':
            continue
        for b in blocks_list:
            for i in range(b['start'], b['end']):
                extracted_ranges.add(i)

    # Write engine.go with only the non-extracted lines
    remaining_lines = []
    for i, line in enumerate(lines):
        if i not in extracted_ranges:
            remaining_lines.append(line)

    # Clean up multiple consecutive blank lines
    cleaned = []
    prev_blank = False
    for line in remaining_lines:
        is_blank = line.strip() == ''
        if is_blank and prev_blank:
            continue
        cleaned.append(line)
        prev_blank = is_blank

    with open('engine.go', 'w') as f:
        f.writelines(cleaned)
    print(f"Updated engine.go ({len(cleaned)} lines)")


if __name__ == '__main__':
    main()
