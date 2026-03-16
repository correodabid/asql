import { useState } from 'react'
import { IconChevronDown } from './Icons'
import type { AccessPlan } from '../types/workspace'

type PlanShape = {
  operation: string
  domain_name?: string
  table_name?: string
  columns?: string[]
  filter?: Record<string, unknown>
  join?: Record<string, unknown>
  group_by?: string[]
  having?: Record<string, unknown>
  order_by?: { column: string; direction: string }[]
  limit?: number
  offset?: number
  index_name?: string
  index_columns?: string[]
  index_method?: string
  schema?: { name: string; type: string }[]
  values?: unknown[]
  alter_column?: Record<string, unknown>
}

type Props = {
  planShape: Record<string, unknown>
  accessPlan?: AccessPlan
  operation: string
  domain: string
  table: string
}

export function ExplainTree({ planShape, accessPlan, operation, domain, table }: Props) {
  return (
    <div className="explain-tree">
      <div className="explain-header">
        <span className="explain-title">Query Plan</span>
        <span className="result-meta">{operation}</span>
        {domain && <span className="result-meta">{domain}</span>}
        {table && <span className="result-meta">{table}</span>}
      </div>
      {accessPlan && <AccessPlanSection plan={accessPlan} />}
      <div className="explain-nodes">
        <PlanNode plan={planShape as PlanShape} depth={0} />
      </div>
      {accessPlan && <IndexSuggestions plan={accessPlan} operation={operation} />}
    </div>
  )
}

function AccessPlanSection({ plan }: { plan: AccessPlan }) {
  const [showCandidates, setShowCandidates] = useState(false)
  const [showPruned, setShowPruned] = useState(false)
  const selectivity = plan.table_rows > 0 && plan.estimated_rows !== undefined
    ? ((plan.estimated_rows / plan.table_rows) * 100)
    : null
  const selectivityPct = selectivity !== null ? selectivity.toFixed(1) : null

  // Determine if this is a "hot" path (full scan on large table)
  const isHotPath = plan.strategy.includes('full-scan') && plan.table_rows > 100

  return (
    <div className={`explain-access-plan ${isHotPath ? 'explain-hot-path' : ''}`}>
      <PlannerVerdict plan={plan} />
      <div className="explain-access-row">
        <span className="explain-detail-label">Strategy</span>
        <span className={`explain-op-badge ${strategyClass(plan.strategy)}`}>
          {plan.strategy.replace(/-/g, ' ').toUpperCase()}
        </span>
        {isHotPath && <span className="explain-hot-badge">HOT PATH</span>}
      </div>
      <div className="explain-access-row">
        <span className="explain-detail-label">Rows</span>
        <span className="explain-detail-value mono">
          {plan.estimated_rows !== undefined
            ? `${plan.estimated_rows} / ${plan.table_rows}${selectivityPct ? ` (${selectivityPct}%)` : ''}`
            : String(plan.table_rows)}
        </span>
      </div>

      {/* Selectivity Bar */}
      {selectivity !== null && (
        <div className="explain-access-row">
          <span className="explain-detail-label">Selectivity</span>
          <div className="explain-cost-bar-wrap">
            <div className="explain-cost-bar-track">
              <div
                className={`explain-cost-bar-fill ${selectivity > 75 ? 'high' : selectivity > 30 ? 'medium' : 'low'}`}
                style={{ width: `${Math.min(selectivity, 100)}%` }}
              />
            </div>
            <span className="explain-cost-bar-label">{selectivityPct}%</span>
          </div>
        </div>
      )}

      {plan.index_used && (
        <div className="explain-access-row">
          <span className="explain-detail-label">Index</span>
          <span className="explain-detail-value mono">
            {plan.index_used} ({plan.index_type}) on {plan.index_column}
          </span>
        </div>
      )}
      {plan.indexed_predicates && plan.indexed_predicates.length > 0 && (
        <div className="explain-access-row explain-access-row-wrap">
          <span className="explain-detail-label">Indexed</span>
          <div className="explain-token-list">
            {plan.indexed_predicates.map((predicate, i) => (
              <span key={`${predicate}-${i}`} className="explain-token mono">{predicate}</span>
            ))}
          </div>
        </div>
      )}
      {plan.residual_predicate && (
        <div className="explain-access-row explain-access-row-wrap">
          <span className="explain-detail-label">Residual</span>
          <span className="explain-detail-value mono">{plan.residual_predicate}</span>
        </div>
      )}
      {plan.candidates && plan.candidates.length > 0 && (
        <>
          <div className="explain-access-row">
            <span
              className="explain-detail-label explain-toggle-label"
              onClick={() => setShowCandidates(!showCandidates)}
            >
              Candidates {showCandidates ? '\u25BC' : '\u25B6'}
            </span>
          </div>
          {showCandidates && (
            <div className="explain-candidates">
              {renderCandidatesWithBars(plan.candidates)}
            </div>
          )}
        </>
      )}
      {plan.pruned_candidates && plan.pruned_candidates.length > 0 && (
        <>
          <div className="explain-access-row">
            <span
              className="explain-detail-label explain-toggle-label"
              onClick={() => setShowPruned(!showPruned)}
            >
              Pruned {showPruned ? '\u25BC' : '\u25B6'}
            </span>
          </div>
          {showPruned && (
            <div className="explain-candidates explain-pruned-list">
              {renderPrunedCandidates(plan.pruned_candidates)}
            </div>
          )}
        </>
      )}
      {plan.joins && plan.joins.length > 0 && (
        <>
          <div className="explain-access-row">
            <span className="explain-detail-label">Joins</span>
          </div>
          {plan.joins.map((j, i) => (
            <div key={i} className="explain-join-row">
              <span className={`explain-op-badge ${strategyClass(j.strategy)}`}>
                {j.strategy.replace(/-/g, ' ').toUpperCase()}
              </span>
              <span className="mono">{j.join_type.toUpperCase()} {j.table}</span>
              <span className="mono explain-cost">{j.table_rows} rows</span>
              {j.index_used && <span className="mono explain-cost">via {j.index_used}</span>}
            </div>
          ))}
        </>
      )}
    </div>
  )
}

function renderCandidatesWithBars(candidates: NonNullable<AccessPlan['candidates']>) {
  const maxCost = Math.max(...candidates.map(c => c.cost ?? 0), 1)

  return candidates.map((c, i) => (
    <div key={i} className={`explain-candidate-card${c.chosen ? ' chosen' : ''}`}>
      <div className={`explain-candidate-row${c.chosen ? ' chosen' : ''}`}>
        <span className="mono" style={{ minWidth: 90 }}>{c.strategy}</span>
        <div className="explain-cost-bar-wrap" style={{ flex: 1 }}>
          <div className="explain-cost-bar-track">
            <div
              className={`explain-cost-bar-fill ${c.chosen ? 'chosen' : 'candidate'}`}
              style={{ width: `${((c.cost ?? 0) / maxCost) * 100}%` }}
            />
          </div>
          <span className="explain-cost-bar-label">{c.cost ?? 0}</span>
        </div>
        {c.chosen && <span className="explain-chosen-badge">chosen</span>}
      </div>
      {(c.detail || c.rejected_reason) && (
        <div className="explain-candidate-meta">
          {c.detail && <div className="explain-candidate-detail mono">{c.detail}</div>}
          {c.rejected_reason && <div className="explain-candidate-reason">{c.rejected_reason}</div>}
        </div>
      )}
    </div>
  ))
}

function PlannerVerdict({ plan }: { plan: AccessPlan }) {
  const verdict = buildPlannerVerdict(plan)

  return (
    <div className={`explain-verdict ${verdict.level}`}>
      <div className="explain-verdict-header">
        <span className="explain-verdict-kicker">Planner verdict</span>
        <span className="explain-verdict-title">{verdict.title}</span>
      </div>
      <div className="explain-verdict-body">{verdict.body}</div>
    </div>
  )
}

function renderPrunedCandidates(candidates: NonNullable<AccessPlan['pruned_candidates']>) {
  return candidates.map((candidate, i) => (
    <div key={i} className="explain-pruned-card">
      <div className="explain-pruned-header">
        <span className={`explain-op-badge ${strategyClass(candidate.strategy)}`}>
          {candidate.strategy.replace(/-/g, ' ').toUpperCase()}
        </span>
      </div>
      {candidate.detail && <div className="explain-candidate-detail mono">{candidate.detail}</div>}
      <div className="explain-candidate-reason">{candidate.reason}</div>
    </div>
  ))
}

function IndexSuggestions({ plan, operation }: { plan: AccessPlan; operation: string }) {
  const suggestions = buildSuggestions(plan, operation)
  if (suggestions.length === 0) return null

  return (
    <div className="explain-suggestions">
      <div className="explain-suggestions-title">Suggestions</div>
      {suggestions.map((s, i) => (
        <div key={i} className={`explain-suggestion-item ${s.level}`}>
          <span className="explain-suggestion-icon">{suggestionIcon(s.level)}</span>
          <div className="explain-suggestion-copy">
            <div className="explain-suggestion-label">{s.title}</div>
            <div>{s.body}</div>
          </div>
        </div>
      ))}
    </div>
  )
}

type ExplainSuggestion = {
  title: string
  body: string
  level: 'info' | 'warn' | 'good'
}

type PlannerVerdict = {
  title: string
  body: string
  level: 'info' | 'warn' | 'good'
}

function buildPlannerVerdict(plan: AccessPlan): PlannerVerdict {
  const selectivity = plan.table_rows > 0 && plan.estimated_rows !== undefined
    ? (plan.estimated_rows / plan.table_rows) * 100
    : null

  if (plan.pruned_candidates && plan.pruned_candidates.length > 0 && plan.strategy.includes('full-scan')) {
    return {
      title: 'Broad fallback to full scan',
      body: 'Indexed alternatives were considered but pruned because they would still touch too much of the table, so the planner intentionally kept a full scan.',
      level: 'warn',
    }
  }

  if (plan.indexed_predicates && plan.indexed_predicates.length > 0 && plan.residual_predicate) {
    return {
      title: 'Partial index pushdown with residual filter',
      body: `The planner can narrow the search using ${plan.indexed_predicates.join(', ')}, but it still has to apply ${plan.residual_predicate} after fetching candidate rows.`,
      level: 'info',
    }
  }

  if (plan.index_used && selectivity !== null && selectivity <= 15) {
    return {
      title: 'Selective indexed path',
      body: `The chosen index path is tight: only ${selectivity.toFixed(1)}% of rows are expected to be visited.`,
      level: 'good',
    }
  }

  if (plan.index_used && selectivity !== null && selectivity > 80) {
    return {
      title: 'Indexed path is still broad',
      body: `The planner used ${plan.index_used}, but it still expects to visit ${selectivity.toFixed(0)}% of rows, so this path may remain expensive under growth.`,
      level: 'warn',
    }
  }

  if (plan.strategy.includes('full-scan')) {
    return {
      title: 'No narrower access path found',
      body: 'The planner did not find a cheaper indexed strategy for the current predicate shape.',
      level: 'info',
    }
  }

  if (plan.strategy.includes('join')) {
    return {
      title: 'Join-driven access path',
      body: 'The runtime plan is shaped primarily by join execution rather than a single-table lookup path.',
      level: 'info',
    }
  }

  return {
    title: 'Planner chose the cheapest current path',
    body: 'The access plan reflects the best deterministic strategy available under the current cost model and predicate shape.',
    level: 'info',
  }
}

function buildSuggestions(plan: AccessPlan, operation: string): ExplainSuggestion[] {
  const suggestions: ExplainSuggestion[] = []

  if (plan.strategy.includes('full-scan') && plan.table_rows > 100 && operation === 'select') {
    if (plan.pruned_candidates && plan.pruned_candidates.length > 0) {
      suggestions.push({
        title: 'Full scan was chosen intentionally',
        body: 'The planner discarded broader indexed alternatives because they would still touch most rows. This usually means the current predicate is not selective enough for an index win.',
        level: 'info',
      })
    } else if (!plan.indexed_predicates || plan.indexed_predicates.length === 0) {
      suggestions.push({
        title: 'No selective access path found',
        body: 'Consider adding an index on the filtered column(s) if this query is latency-sensitive and expected to stay selective.',
        level: 'warn',
      })
    }
  }

  if (plan.indexed_predicates && plan.indexed_predicates.length > 0 && plan.residual_predicate) {
    suggestions.push({
      title: 'Partial index pushdown is active',
      body: `The planner used indexes for ${plan.indexed_predicates.join(', ')} and left ${plan.residual_predicate} as residual filtering. A more targeted composite/indexable predicate shape could remove the extra filter work.`,
      level: 'info',
    })
  }

  if (plan.estimated_rows !== undefined && plan.table_rows > 0) {
    const selectivity = (plan.estimated_rows / plan.table_rows) * 100
    if (selectivity > 80 && plan.index_used) {
      suggestions.push({
        title: 'Chosen index is broad',
        body: `Index "${plan.index_used}" still matches ${selectivity.toFixed(0)}% of rows. A more selective predicate or composite index may reduce row visits.`,
        level: 'warn',
      })
    } else if (selectivity <= 15 && plan.index_used) {
      suggestions.push({
        title: 'Selective indexed path',
        body: `The chosen index path looks strong: only ${selectivity.toFixed(1)}% of rows are expected to be visited.`,
        level: 'good',
      })
    }
  }

  const rejected = plan.candidates?.filter((candidate) => !candidate.chosen && candidate.rejected_reason)
  if (rejected && rejected.length > 0) {
    const topRejected = rejected.slice(0, 2).map((candidate) => `${candidate.strategy}: ${candidate.rejected_reason}`).join(' · ')
    suggestions.push({
      title: 'Alternative plans were evaluated',
      body: topRejected,
      level: 'info',
    })
  }

  if (plan.pruned_candidates && plan.pruned_candidates.length > 0) {
    const firstPruned = plan.pruned_candidates[0]
    suggestions.push({
      title: 'A heuristic crossover was applied',
      body: `${firstPruned.strategy} was pruned before the final cost comparison: ${firstPruned.reason}.`,
      level: 'info',
    })
  }

  if (plan.joins) {
    for (const join of plan.joins) {
      if (!join.index_used && join.table_rows > 50) {
        suggestions.push({
          title: `Join on ${join.table} has no index help`,
          body: `The join reads ${join.table_rows} rows without an index-backed path. Consider indexing the join key if this shape is common.`,
          level: 'warn',
        })
      }
    }
  }

  return dedupeSuggestions(suggestions)
}

function dedupeSuggestions(suggestions: ExplainSuggestion[]): ExplainSuggestion[] {
  const seen = new Set<string>()
  return suggestions.filter((suggestion) => {
    const key = `${suggestion.level}:${suggestion.title}:${suggestion.body}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function suggestionIcon(level: ExplainSuggestion['level']): string {
  switch (level) {
    case 'good':
      return '✓'
    case 'warn':
      return '!'
    default:
      return 'i'
  }
}

function strategyClass(strategy: string): string {
  if (strategy.includes('hash') || strategy.includes('btree')) return 'index'
  if (strategy.includes('full-scan')) return 'scan'
  if (strategy.includes('join')) return 'join'
  return ''
}

function PlanNode({ plan, depth }: { plan: PlanShape; depth: number }) {
  const [expanded, setExpanded] = useState(true)
  const hasDetails = !!(
    plan.filter ||
    plan.join ||
    plan.group_by?.length ||
    plan.having ||
    plan.order_by?.length ||
    plan.limit !== undefined ||
    plan.offset !== undefined ||
    plan.schema?.length ||
    plan.values?.length ||
    plan.index_name ||
    plan.alter_column
  )

  const opLabel = formatOperation(plan.operation)
  const opClass = operationClass(plan.operation)

  return (
    <div className="explain-node" style={{ marginLeft: depth * 20 }}>
      <div className="explain-node-header" onClick={() => hasDetails && setExpanded(!expanded)}>
        {depth > 0 && <span className="explain-connector" />}
        <span className={`explain-op-badge ${opClass}`}>{opLabel}</span>
        {plan.table_name && <span className="explain-target mono">{plan.table_name}</span>}
        {plan.columns && plan.columns.length > 0 && (
          <span className="explain-columns mono">({plan.columns.join(', ')})</span>
        )}
        {hasDetails && (
          <span className={`explain-toggle ${expanded ? 'open' : ''}`}>
            <IconChevronDown />
          </span>
        )}
      </div>

      {expanded && hasDetails && (
        <div className="explain-node-detail">
          {plan.filter && (
            <DetailRow label="Filter" value={formatPredicate(plan.filter)} />
          )}
          {plan.join && (
            <DetailRow label="Join" value={formatJoin(plan.join)} />
          )}
          {plan.group_by && plan.group_by.length > 0 && (
            <DetailRow label="Group By" value={plan.group_by.join(', ')} />
          )}
          {plan.having && (
            <DetailRow label="Having" value={formatPredicate(plan.having)} />
          )}
          {plan.order_by && plan.order_by.length > 0 && (
            <DetailRow label="Order By" value={plan.order_by.map((o) => `${o.column} ${o.direction || 'ASC'}`).join(', ')} />
          )}
          {plan.limit !== undefined && (
            <DetailRow label="Limit" value={String(plan.limit)} />
          )}
          {plan.offset !== undefined && (
            <DetailRow label="Offset" value={String(plan.offset)} />
          )}
          {plan.index_name && (
            <DetailRow label="Index" value={`${plan.index_name} (${plan.index_method || 'btree'}) on [${(plan.index_columns || []).join(', ')}]`} />
          )}
          {plan.schema && plan.schema.length > 0 && (
            <DetailRow label="Schema" value={plan.schema.map((c) => `${c.name} ${c.type}`).join(', ')} />
          )}
          {plan.alter_column && (
            <DetailRow label="Alter Column" value={JSON.stringify(plan.alter_column)} />
          )}
          {plan.values && plan.values.length > 0 && (
            <DetailRow label="Values" value={plan.values.map((v) => formatValue(v)).join(', ')} />
          )}
        </div>
      )}

      {/* Render join subtree if present */}
      {plan.join && expanded && (
        <div className="explain-node" style={{ marginLeft: 20 }}>
          <div className="explain-node-header">
            <span className="explain-connector" />
            <span className="explain-op-badge scan">SCAN</span>
            <span className="explain-target mono">{(plan.join as Record<string, unknown>).table as string || 'joined table'}</span>
          </div>
        </div>
      )}
    </div>
  )
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="explain-detail-row">
      <span className="explain-detail-label">{label}</span>
      <span className="explain-detail-value mono">{value}</span>
    </div>
  )
}

function formatOperation(op: string): string {
  return op.replace(/_/g, ' ').toUpperCase()
}

function operationClass(op: string): string {
  if (op === 'select') return 'read'
  if (op === 'insert' || op === 'update' || op === 'delete') return 'write'
  if (op.startsWith('create') || op.startsWith('alter')) return 'ddl'
  return ''
}

function formatPredicate(pred: Record<string, unknown>): string {
  if (pred.column && pred.operator && 'value' in pred) {
    return `${pred.column} ${pred.operator} ${formatValue(pred.value)}`
  }
  if (pred.left && pred.operator && pred.right) {
    return `${formatPredicate(pred.left as Record<string, unknown>)} ${pred.operator} ${formatPredicate(pred.right as Record<string, unknown>)}`
  }
  return JSON.stringify(pred)
}

function formatJoin(join: Record<string, unknown>): string {
  const type = (join.type as string) || 'JOIN'
  const table = (join.table as string) || '?'
  const on = join.on ? ` ON ${formatPredicate(join.on as Record<string, unknown>)}` : ''
  return `${type.toUpperCase()} ${table}${on}`
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}
