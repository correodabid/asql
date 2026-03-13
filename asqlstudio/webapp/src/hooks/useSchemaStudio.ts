import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../lib/api'
import {
  clone,
  DEFAULT_MODEL,
  type ApplySafeDiffResponse,
  type DDLResponse,
  type DiffResponse,
  type MultiDomainModel,
  type SchemaColumn,
  type SchemaModel,
  type SchemaTable,
} from '../schema'
import type { StatementState } from '../components/DDLPanel'

type BaselineResponse = {
  status: string
  baseline: SchemaModel
}

type AllBaselinesResponse = {
  status: string
  baselines: SchemaModel[]
}

export const ALL_DOMAINS_KEY = '__all__'

export function useSchemaStudio() {
  const [model, setModel] = useState<SchemaModel>(clone(DEFAULT_MODEL))
  const [baseline, setBaseline] = useState<SchemaModel>(clone(DEFAULT_MODEL))
  const [selectedTable, setSelectedTable] = useState(0)
  const [selectedColumn, setSelectedColumn] = useState(0)
  const [selectedIndex, setSelectedIndex] = useState(0)
  const [designerStatus, setDesignerStatus] = useState('Ready')
  const [ddl, setDdl] = useState('-- Build your model and click "Generate DDL"')
  const [ddlStatements, setDdlStatements] = useState<string[]>([])
  const [diffSummary, setDiffSummary] = useState('No diff preview yet.')
  const [diffSafe, setDiffSafe] = useState<boolean | null>(null)
  const [diffOperations, setDiffOperations] = useState<DiffResponse['operations']>([])
  const [diffWarnings, setDiffWarnings] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  // Designer sub-view: 'canvas' | 'ddl' | 'diff'
  const [designerView, setDesignerView] = useState<'canvas' | 'ddl' | 'diff'>('canvas')
  const [allDomainsModel, setAllDomainsModel] = useState<MultiDomainModel | null>(null)
  const [statementStates, setStatementStates] = useState<StatementState[]>([])

  const isAllDomains = model.domain === ALL_DOMAINS_KEY

  const activeTable = useMemo(() => model.tables[selectedTable] || null, [model, selectedTable])
  const activeColumn = useMemo(() => {
    if (!activeTable) return null
    return activeTable.columns[selectedColumn] || null
  }, [activeTable, selectedColumn])

  const health = useMemo(() => (designerStatus.toLowerCase().includes('error') ? 'Error' : 'Ready'), [designerStatus])

  const updateTable = (updater: (table: SchemaTable) => SchemaTable) => {
    setModel((current) => {
      if (!current.tables[selectedTable]) return current
      const next = clone(current)
      next.tables[selectedTable] = updater(next.tables[selectedTable])
      return next
    })
  }

  const updateColumn = (updater: (column: SchemaColumn) => SchemaColumn) => {
    updateTable((table) => {
      if (!table.columns[selectedColumn]) return table
      const next = clone(table)
      next.columns[selectedColumn] = updater(next.columns[selectedColumn])
      return next
    })
  }

  const normalizeSelection = (next: SchemaModel) => {
    if (next.tables.length === 0) {
      next.tables.push({
        name: 'table_1',
        columns: [{ name: 'id', type: 'INT', nullable: false, primary_key: true, unique: false, default_value: '' }],
      })
    }
    if (selectedTable >= next.tables.length) setSelectedTable(next.tables.length - 1)
    const table = next.tables[Math.min(selectedTable, next.tables.length - 1)]
    if (table.columns.length === 0) {
      table.columns.push({ name: 'id', type: 'INT', nullable: false, primary_key: true, unique: false, default_value: '' })
    }
    if (selectedColumn >= table.columns.length) setSelectedColumn(table.columns.length - 1)
  }

  const designerPayload = (): SchemaModel => clone(model)

  const renderDiff = (response: DiffResponse) => {
    const opCount = response.operations.length
    setDiffSummary(`Diff for domain ${response.domain}: ${opCount} operation(s), safe=${response.safe ? 'yes' : 'no'}`)
    setDiffSafe(response.safe)
    setDiffOperations(response.operations)
    setDiffWarnings(response.warnings || [])
  }

  const onGenerateDDL = async () => {
    setLoading(true)
    try {
      const response = await api<DDLResponse>('/api/schema/ddl', 'POST', designerPayload())
      setDdl(response.ddl || '-- empty')
      const stmts = response.statements || []
      setDdlStatements(stmts)
      setStatementStates(stmts.map(sql => ({ sql, status: 'pending' as const })))
      setDesignerStatus(`DDL generated (${stmts.length} statements)`)
      setDesignerView('ddl')
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const onLoadBaseline = async (overrideDomain?: string) => {
    const targetDomain = overrideDomain || model.domain || 'default'
    setLoading(true)
    try {
      const response = await api<BaselineResponse>('/api/schema/load-baseline', 'POST', {
        domain: targetDomain,
      })
      const nextBaseline = clone(response.baseline)
      setBaseline(nextBaseline)
      if (nextBaseline.tables.length > 0) {
        setModel(nextBaseline)
        setSelectedTable(0)
        setSelectedColumn(0)
      } else {
        setModel({ ...clone(DEFAULT_MODEL), domain: targetDomain })
      }
      setDiffSummary(`Baseline loaded from ASQL for domain ${nextBaseline.domain}`)
      setDiffSafe(true)
      setDiffOperations([])
      setDiffWarnings([])
      setDesignerStatus(`Baseline loaded (${nextBaseline.tables.length} table(s))`)
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const changeDomain = async (newDomain: string) => {
    if (newDomain === ALL_DOMAINS_KEY) {
      setLoading(true)
      try {
        const response = await api<AllBaselinesResponse>('/api/schema/load-all-baselines', 'GET')
        setAllDomainsModel({ domains: response.baselines || [] })
        setModel({ domain: ALL_DOMAINS_KEY, tables: [] })
        setDesignerStatus(`All domains loaded (${response.baselines?.length || 0} domain(s))`)
      } catch (error) {
        setDesignerStatus((error as Error).message)
      } finally {
        setLoading(false)
      }
      return
    }
    setAllDomainsModel(null)
    await onLoadBaseline(newDomain)
  }

  // Auto-load baseline from engine on first mount
  const didAutoLoad = useRef(false)
  useEffect(() => {
    if (didAutoLoad.current) return
    didAutoLoad.current = true
    api<{ domains: string[] }>('/api/domains', 'GET')
      .then((resp) => {
        const available = resp.domains || []
        if (available.length > 0 && available[0] !== 'default') {
          onLoadBaseline(available[0])
        } else {
          onLoadBaseline()
        }
      })
      .catch(() => onLoadBaseline())
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const onSetBaseline = () => {
    setBaseline(designerPayload())
    setDesignerStatus('Baseline set from current model')
  }

  // Silent variant: updates diff state without switching views (used by auto-diff in the canvas footer)
  const onSilentDiff = async () => {
    try {
      const response = await api<DiffResponse>('/api/schema/diff', 'POST', {
        base: baseline,
        target: designerPayload(),
      })
      renderDiff(response)
      setDesignerStatus(`Diff auto-updated (${response.operations.length} operation(s))`)
    } catch {
      // silent — don't overwrite status on background diff failures
    }
  }

  const onPreviewDiff = async () => {
    setLoading(true)
    try {
      const response = await api<DiffResponse>('/api/schema/diff', 'POST', {
        base: baseline,
        target: designerPayload(),
      })
      renderDiff(response)
      setDesignerStatus(`Diff preview ready (${response.operations.length} operation(s))`)
      setDesignerView('diff')
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const onApplySafeDiff = async () => {
    setLoading(true)
    try {
      const response = await api<ApplySafeDiffResponse>('/api/schema/apply-safe-diff', 'POST', {
        base: baseline,
        target: designerPayload(),
      })
      const statements = response.executed_statements || []
      setDdl(statements.length > 0 ? `${statements.join(';\n')};` : '-- no safe statements to apply')
      setDdlStatements(statements)
      setDiffSummary(`Applied safe diff in ${response.domain}: applied=${response.applied_count}, unsafe=${response.unsafe_count}, diff_safe=${response.diff_safe ? 'yes' : 'no'}`)
      setDiffSafe(response.diff_safe)
      setDiffWarnings(response.warnings || [])
      setDesignerStatus(`Safe diff applied: ${response.applied_count} statement(s)`)
      setBaseline(designerPayload())
      setDesignerView('diff')
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const onRefreshAutoDiff = async () => {
    setLoading(true)
    try {
      const target = designerPayload()
      const baselineResponse = await api<BaselineResponse>('/api/schema/load-baseline', 'POST', {
        domain: model.domain || 'default',
      })
      const nextBaseline = clone(baselineResponse.baseline)
      setBaseline(nextBaseline)
      const diffResponse = await api<DiffResponse>('/api/schema/diff', 'POST', {
        base: nextBaseline,
        target,
      })
      renderDiff(diffResponse)
      setDesignerStatus(`Auto-diff ready (${diffResponse.operations.length} operation(s))`)
      setDesignerView('diff')
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const onRefreshAutoDiffApplySafe = async () => {
    setLoading(true)
    try {
      const target = designerPayload()
      const baselineResponse = await api<BaselineResponse>('/api/schema/load-baseline', 'POST', {
        domain: model.domain || 'default',
      })
      const nextBaseline = clone(baselineResponse.baseline)
      setBaseline(nextBaseline)
      const diffResponse = await api<DiffResponse>('/api/schema/diff', 'POST', {
        base: nextBaseline,
        target,
      })
      renderDiff(diffResponse)

      if ((diffResponse.statements || []).length === 0) {
        setDesignerStatus('Auto-diff ready: no safe statements to apply')
        setDesignerView('diff')
        return
      }

      const shouldApply = window.confirm(`Apply ${diffResponse.statements.length} safe statement(s) to domain ${diffResponse.domain}?`)
      if (!shouldApply) {
        setDesignerStatus(`Auto-diff ready (${diffResponse.operations.length} operation(s)); apply cancelled`)
        setDesignerView('diff')
        return
      }

      const applyResponse = await api<ApplySafeDiffResponse>('/api/schema/apply-safe-diff', 'POST', {
        base: nextBaseline,
        target,
      })
      const statements = applyResponse.executed_statements || []
      setDdl(statements.length > 0 ? `${statements.join(';\n')};` : '-- no safe statements to apply')
      setDdlStatements(statements)
      setDiffSummary(`Applied safe diff in ${applyResponse.domain}: applied=${applyResponse.applied_count}, unsafe=${applyResponse.unsafe_count}, diff_safe=${applyResponse.diff_safe ? 'yes' : 'no'}`)
      setDiffSafe(applyResponse.diff_safe)
      setDiffWarnings(applyResponse.warnings || [])
      setDesignerStatus(`Auto-flow applied: ${applyResponse.applied_count} safe statement(s)`)
      setBaseline(designerPayload())
      setDesignerView('diff')
    } catch (error) {
      setDesignerStatus((error as Error).message)
    } finally {
      setLoading(false)
    }
  }

  // Per-statement execution
  const onExecuteStatement = useCallback(async (index: number) => {
    if (index < 0 || index >= ddlStatements.length) return
    const sql = ddlStatements[index]
    const domain = model.domain || 'default'

    setStatementStates(prev => {
      const next = [...prev]
      next[index] = { ...next[index], status: 'running' }
      return next
    })

    try {
      await api<{ status: string }>('/api/schema/apply', 'POST', {
        domain,
        statements: [sql],
      })
      setStatementStates(prev => {
        const next = [...prev]
        next[index] = { ...next[index], status: 'success' }
        return next
      })
    } catch (error) {
      setStatementStates(prev => {
        const next = [...prev]
        next[index] = { ...next[index], status: 'error', error: (error as Error).message }
        return next
      })
    }
  }, [ddlStatements, model.domain])

  const onExecuteAll = useCallback(async () => {
    const domain = model.domain || 'default'
    const pendingIndices = statementStates
      .map((s, i) => ({ s, i }))
      .filter(({ s }) => s.status === 'pending')
      .map(({ i }) => i)

    if (pendingIndices.length === 0) return

    const pendingStatements = pendingIndices.map(i => ddlStatements[i])

    // Mark all pending as running
    setStatementStates(prev => {
      const next = [...prev]
      for (const idx of pendingIndices) {
        next[idx] = { ...next[idx], status: 'running' }
      }
      return next
    })

    try {
      await api<{ status: string }>('/api/schema/apply', 'POST', {
        domain,
        statements: pendingStatements,
      })
      // All succeeded
      setStatementStates(prev => {
        const next = [...prev]
        for (const idx of pendingIndices) {
          next[idx] = { ...next[idx], status: 'success' }
        }
        return next
      })
      setDesignerStatus(`Executed ${pendingIndices.length} statement(s) successfully`)
    } catch (error) {
      // Mark remaining as error (we don't know which one failed in batch)
      setStatementStates(prev => {
        const next = [...prev]
        for (const idx of pendingIndices) {
          if (next[idx].status === 'running') {
            next[idx] = { ...next[idx], status: 'error', error: (error as Error).message }
          }
        }
        return next
      })
      setDesignerStatus(`Execution failed: ${(error as Error).message}`)
    }
  }, [ddlStatements, model.domain, statementStates, setDesignerStatus])

  // Apply selected diff operations
  const onApplySelectedDiff = useCallback(async (indices: number[]) => {
    const domain = model.domain || 'default'
    const statements = indices
      .map(i => diffOperations[i]?.statement)
      .filter((s): s is string => !!s && s.trim() !== '')

    if (statements.length === 0) return

    setLoading(true)
    try {
      await api<{ status: string }>('/api/schema/apply', 'POST', {
        domain,
        statements,
      })
      setDesignerStatus(`Applied ${statements.length} selected operation(s)`)
    } catch (error) {
      setDesignerStatus(`Apply failed: ${(error as Error).message}`)
    } finally {
      setLoading(false)
    }
  }, [model.domain, diffOperations, setDesignerStatus, setLoading])

  return {
    model,
    setModel,
    selectedTable,
    setSelectedTable,
    selectedColumn,
    setSelectedColumn,
    selectedIndex,
    setSelectedIndex,
    activeTable,
    activeColumn,
    designerStatus,
    designerView,
    setDesignerView,
    ddl,
    ddlStatements,
    diffSummary,
    diffSafe,
    diffOperations,
    diffWarnings,
    health,
    loading,
    allDomainsModel,
    isAllDomains,
    updateTable,
    updateColumn,
    normalizeSelection,
    onGenerateDDL,
    onLoadBaseline,
    onSetBaseline,
    changeDomain,
    onSilentDiff,
    onPreviewDiff,
    onApplySafeDiff,
    onRefreshAutoDiff,
    onRefreshAutoDiffApplySafe,
    statementStates,
    onExecuteStatement,
    onExecuteAll,
    onApplySelectedDiff,
  }
}
