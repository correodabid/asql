import { useCallback, useRef, useState } from 'react'
import { api } from '../lib/api'
import { isExplainQuery, isForHistoryQuery, isReadQuery } from '../lib/sql'
import type {
  ExplainPlan,
  HistoryEntry,
  QueryResult,
  TxState,
  WorkspaceTab,
} from '../types/workspace'

const HISTORY_KEY = 'asql_query_history'
const FAVORITES_KEY = 'asql_query_favorites'
const MAX_HISTORY = 50

function notifyHistoryCount(count: number) {
  window.dispatchEvent(new CustomEvent('asql:query-history-updated', { detail: { count } }))
}

function loadFromStorage<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

function saveToStorage<T>(key: string, value: T): void {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // quota exceeded or unavailable
  }
}

let tabCounter = 1

function createTab(label?: string): WorkspaceTab {
  const id = `tab-${Date.now()}-${tabCounter++}`
  return {
    id,
    label: label || `Query ${tabCounter - 1}`,
    sql: '',
    explainEnabled: false,
    result: null,
    results: [],
    error: null,
    loading: false,
    tableName: null,
    selectedRow: null,
    explainPlan: null,
  }
}

export function useWorkspace(domain: string) {
  const [tabs, setTabs] = useState<WorkspaceTab[]>(() => [createTab('Query 1')])
  const [activeTabId, setActiveTabId] = useState(() => tabs[0].id)
  const [history, setHistory] = useState<HistoryEntry[]>(() =>
    loadFromStorage(HISTORY_KEY, []),
  )
  const [favorites, setFavorites] = useState<HistoryEntry[]>(() =>
    loadFromStorage(FAVORITES_KEY, []),
  )
  const [txState, setTxState] = useState<TxState | null>(null)
  const [timeTravelMode, setTimeTravelMode] = useState(false)
  const [timeTravelLSN, setTimeTravelLSN] = useState(0)
  const [maxLSN, setMaxLSN] = useState(0)
  const [detailPanelOpen, setDetailPanelOpen] = useState(false)
  const startRef = useRef(0)

  const activeTab = tabs.find((t) => t.id === activeTabId) || tabs[0]

  // ─── Tab management ────────────────────────────────────

  const updateTab = useCallback(
    (id: string, patch: Partial<WorkspaceTab>) => {
      setTabs((prev) => prev.map((t) => (t.id === id ? { ...t, ...patch } : t)))
    },
    [],
  )

  const addTab = useCallback(() => {
    const t = createTab()
    setTabs((prev) => [...prev, t])
    setActiveTabId(t.id)
  }, [])

  const closeTab = useCallback(
    (id: string) => {
      setTabs((prev) => {
        if (prev.length <= 1) return prev
        const next = prev.filter((t) => t.id !== id)
        if (activeTabId === id) {
          setActiveTabId(next[next.length - 1].id)
        }
        return next
      })
    },
    [activeTabId],
  )

  const setTabSql = useCallback(
    (id: string, sql: string) => {
      updateTab(id, { sql })
    },
    [updateTab],
  )

  const setTabExplainEnabled = useCallback(
    (id: string, explainEnabled: boolean) => {
      updateTab(id, { explainEnabled })
    },
    [updateTab],
  )

  const setSelectedRow = useCallback(
    (tabId: string, rowIndex: number | null) => {
      updateTab(tabId, { selectedRow: rowIndex })
    },
    [updateTab],
  )

  // ─── History ────────────────────────────────────────────

  const pushHistory = useCallback((entry: HistoryEntry) => {
    setHistory((prev) => {
      const next = [entry, ...prev].slice(0, MAX_HISTORY)
      saveToStorage(HISTORY_KEY, next)
      notifyHistoryCount(next.length)
      return next
    })
  }, [])

  const toggleFavorite = useCallback((entry: HistoryEntry) => {
    setFavorites((prev) => {
      const exists = prev.some((f) => f.sql === entry.sql)
      const next = exists ? prev.filter((f) => f.sql !== entry.sql) : [entry, ...prev]
      saveToStorage(FAVORITES_KEY, next)
      return next
    })
  }, [])

  const clearHistory = useCallback(() => {
    setHistory([])
    saveToStorage(HISTORY_KEY, [])
    notifyHistoryCount(0)
  }, [])

  // ─── Max LSN ───────────────────────────────────────────

  const refreshMaxLSN = useCallback(async () => {
    try {
      const resp = await api<{ lsn: number }>('/api/replication/last-lsn', 'GET')
      setMaxLSN(resp.lsn || 0)
    } catch {
      // ignore
    }
  }, [])

  const validateExplainStatement = useCallback(
    (sql: string): string | null => {
      if (timeTravelMode && timeTravelLSN > 0) {
        return 'EXPLAIN mode is unavailable during time travel.'
      }

      const normalized = sql.replace(/^[\s;]+/, '').toUpperCase()
      if (normalized.startsWith('IMPORT')) {
        return 'EXPLAIN mode does not support IMPORT statements.'
      }
      if (isForHistoryQuery(sql)) {
        return 'EXPLAIN mode does not support FOR HISTORY queries.'
      }
      if (!isReadQuery(sql)) {
        return 'EXPLAIN mode only supports read queries.'
      }
      return null
    },
    [timeTravelLSN, timeTravelMode],
  )

  // ─── Query execution ──────────────────────────────────

  const executeSingleStatement = useCallback(
    async (
      sql: string,
      start: number,
      options?: { explainMode?: boolean },
    ): Promise<{ result: QueryResult; explain: ExplainPlan | null }> => {
      const explainMode = options?.explainMode === true

      // Time-travel mode
      if (timeTravelMode && timeTravelLSN > 0) {
        const response = await api<{
          status: string
          rows?: Record<string, unknown>[]
          columns?: string[]
        }>('/api/time-travel', 'POST', {
          sql,
          domains: [domain],
          lsn: timeTravelLSN,
        })
        const dur = performance.now() - start
        const rows = response.rows || []
        const cols = rows.length > 0 ? Object.keys(rows[0]) : response.columns || []
        return {
          result: { columns: cols, rows, rowCount: rows.length, duration: dur, status: response.status || 'OK', asOfLSN: timeTravelLSN },
          explain: null,
        }
      }

      // READ queries
      if (isReadQuery(sql)) {
        if (explainMode) {
          const explainError = validateExplainStatement(sql)
          if (explainError) {
            throw new Error(explainError)
          }
        }

        // FOR HISTORY
        if (isForHistoryQuery(sql)) {
          const response = await api<{ status: string; rows?: Record<string, unknown>[] }>(
            '/api/row-history', 'POST', { sql, domains: [domain] },
          )
          const dur = performance.now() - start
          const rows = response.rows || []
          const cols = rows.length > 0 ? Object.keys(rows[0]) : []
          return {
            result: { columns: cols, rows, rowCount: rows.length, duration: dur, status: response.status || 'OK' },
            explain: null,
          }
        }

        // EXPLAIN
        if (explainMode || isExplainQuery(sql)) {
          const explainSQL = explainMode && !isExplainQuery(sql)
            ? `EXPLAIN ${sql.trim().replace(/;\s*$/, '')}`
            : sql
          const response = await api<{ status: string; rows?: Record<string, unknown>[] }>(
            '/api/explain', 'POST', { sql: explainSQL, domains: [domain] },
          )
          const dur = performance.now() - start
          const rows = response.rows || []
          const firstRow = rows[0]
          let plan: ExplainPlan | null = null
          if (firstRow && firstRow.plan_shape) {
            const shape = typeof firstRow.plan_shape === 'string' ? JSON.parse(firstRow.plan_shape as string) : firstRow.plan_shape
            const access = firstRow.access_plan
              ? typeof firstRow.access_plan === 'string' ? JSON.parse(firstRow.access_plan as string) : firstRow.access_plan
              : undefined
            plan = {
              operation: (firstRow.operation as string) || '',
              domain: (firstRow.domain as string) || '',
              table: (firstRow.table as string) || '',
              planShape: shape,
              accessPlan: access,
            }
          }
          const cols = rows.length > 0 ? Object.keys(rows[0]) : []
          return {
            result: { columns: cols, rows, rowCount: rows.length, duration: dur, status: response.status || 'EXPLAIN' },
            explain: plan,
          }
        }

        // Regular SELECT
        const response = await api<{
          status: string; rows?: Record<string, unknown>[]; route?: string; consistency?: string; as_of_lsn?: number
        }>('/api/read-query', 'POST', { sql, domains: [domain], consistency: 'strong' })
        const dur = performance.now() - start
        const rows = response.rows || []
        const cols = rows.length > 0 ? Object.keys(rows[0]) : []
        return {
          result: {
            columns: cols, rows, rowCount: rows.length, duration: dur,
            status: response.status || 'OK', route: response.route,
            consistency: response.consistency, asOfLSN: response.as_of_lsn,
          },
          explain: null,
        }
      }

      if (explainMode) {
        throw new Error('EXPLAIN mode only supports read queries.')
      }

      // WRITE queries
      if (txState) {
        const response = await api<{ status: string; rows_affected?: number }>(
          '/api/execute', 'POST', { tx_id: txState.txId, sql },
        )
        const dur = performance.now() - start
        return {
          result: { columns: [], rows: [], rowCount: 0, duration: dur, status: response.status || 'QUEUED' },
          explain: null,
        }
      }

      // Auto-transaction for writes
      const beginResp = await api<{ tx_id: string }>('/api/begin', 'POST', { mode: 'domain', domains: [domain] })
      const autoTxId = beginResp.tx_id
      try {
        await api<{ status: string }>('/api/execute', 'POST', { tx_id: autoTxId, sql })
        await api('/api/commit', 'POST', { tx_id: autoTxId })
        const dur = performance.now() - start
        return {
          result: { columns: [], rows: [], rowCount: 0, duration: dur, status: 'OK' },
          explain: null,
        }
      } catch (execError) {
        try { await api('/api/rollback', 'POST', { tx_id: autoTxId }) } catch { /* best-effort */ }
        throw execError
      }
    },
    [domain, txState, timeTravelMode, timeTravelLSN, validateExplainStatement],
  )

  const executeTab = useCallback(
    async (id: string) => {
      const tab = tabs.find((t) => t.id === id)
      if (!tab) return
      const trimmed = tab.sql.trim()
      if (!trimmed) return

      updateTab(id, { loading: true, error: null })
      startRef.current = performance.now()

      try {
        // Split into statements, keeping IMPORT directives joined with their SELECT
        const rawStmts = trimmed.split(/;\s*/).filter((s) => s.trim())
        const stmts: string[] = []
        let importBuffer = ''
        for (const s of rawStmts) {
          if (s.trim().toUpperCase().startsWith('IMPORT ')) {
            importBuffer += s.trim() + '; '
          } else {
            stmts.push(importBuffer + s.trim())
            importBuffer = ''
          }
        }
        if (importBuffer) stmts.push(importBuffer.trim())

        if (tab.explainEnabled) {
          const explainError = stmts
            .map((stmt) => validateExplainStatement(stmt))
            .find((msg): msg is string => Boolean(msg))
          if (explainError) {
            throw new Error(explainError)
          }
        }

        if (stmts.length <= 1) {
          // Single statement — existing behavior
          const { result, explain } = await executeSingleStatement(trimmed, startRef.current, {
            explainMode: tab.explainEnabled,
          })
          updateTab(id, { loading: false, result, results: [result], explainPlan: explain })
          pushHistory({ sql: trimmed, ts: Date.now(), ok: true, duration: result.duration, rowCount: result.rowCount })
          return
        }

        // Multiple statements — execute sequentially, accumulate results
        const allResults: QueryResult[] = []
        let lastExplain: ExplainPlan | null = null

        for (const stmt of stmts) {
          const sql = stmt.trim()
          if (!sql) continue
          const stmtStart = performance.now()
          const { result, explain } = await executeSingleStatement(sql + ';', stmtStart, {
            explainMode: tab.explainEnabled,
          })
          allResults.push(result)
          if (explain) lastExplain = explain
        }

        const totalDur = performance.now() - startRef.current
        const lastResult = allResults[allResults.length - 1] || null
        updateTab(id, {
          loading: false,
          result: lastResult,
          results: allResults,
          explainPlan: lastExplain,
        })
        pushHistory({ sql: trimmed, ts: Date.now(), ok: true, duration: totalDur, rowCount: lastResult?.rowCount ?? 0 })
      } catch (err) {
        const dur = performance.now() - startRef.current
        const msg = (err as Error).message || 'Unknown error'
        updateTab(id, { loading: false, error: msg, result: null, results: [], explainPlan: null })
        pushHistory({ sql: trimmed, ts: Date.now(), ok: false, duration: dur, rowCount: 0 })
      }
    },
    [tabs, executeSingleStatement, pushHistory, updateTab, validateExplainStatement],
  )

  // ─── Table browsing shortcut ──────────────────────────

  const navigateToQuery = useCallback(
    (sql: string, tableName: string) => {
      const tabId = activeTab.id
      updateTab(tabId, { sql, tableName, selectedRow: null })
      setTimeout(() => {
        const run = async () => {
          updateTab(tabId, { loading: true, error: null })
          startRef.current = performance.now()
          try {
            const response = await api<{
              status: string
              rows?: Record<string, unknown>[]
              route?: string
              consistency?: string
              as_of_lsn?: number
            }>('/api/read-query', 'POST', {
              sql,
              domains: [domain],
              consistency: 'strong',
            })
            const dur = performance.now() - startRef.current
            const rows = response.rows || []
            const cols = rows.length > 0 ? Object.keys(rows[0]) : []
            const result = {
                columns: cols,
                rows,
                rowCount: rows.length,
                duration: dur,
                status: response.status || 'OK',
                route: response.route,
                consistency: response.consistency,
                asOfLSN: response.as_of_lsn,
              }
            updateTab(tabId, {
              loading: false,
              result,
              results: [result],
              explainPlan: null,
            })
            pushHistory({
              sql,
              ts: Date.now(),
              ok: true,
              duration: dur,
              rowCount: rows.length,
            })
          } catch (err) {
            updateTab(tabId, {
              loading: false,
              error: (err as Error).message,
              result: null,
            })
          }
        }
        run()
      }, 0)
    },
    [activeTab.id, domain, pushHistory, updateTab],
  )

  const selectTableIntoTab = useCallback(
    (tableName: string) => {
      navigateToQuery(`SELECT * FROM ${tableName} LIMIT 100;`, tableName)
    },
    [navigateToQuery],
  )

  // ─── Transactions ─────────────────────────────────────

  const beginTransaction = useCallback(async () => {
    if (txState) return
    try {
      const resp = await api<{ tx_id: string }>('/api/begin', 'POST', {
        mode: 'domain',
        domains: [domain],
      })
      setTxState({ txId: resp.tx_id, domains: [domain], mode: 'domain' })
    } catch {
      // ignore
    }
  }, [domain, txState])

  const commitTransaction = useCallback(async () => {
    if (!txState) return
    try {
      await api('/api/commit', 'POST', { tx_id: txState.txId })
      setTxState(null)
    } catch {
      // ignore
    }
  }, [txState])

  const rollbackTransaction = useCallback(async () => {
    if (!txState) return
    try {
      await api('/api/rollback', 'POST', { tx_id: txState.txId })
      setTxState(null)
    } catch {
      // ignore
    }
  }, [txState])

  // ─── Time-travel scrubbing ────────────────────────────

  const scrubToLSN = useCallback(
    (lsn: number) => {
      setTimeTravelLSN(lsn)
    },
    [],
  )

  return {
    // Tabs
    tabs,
    activeTabId,
    activeTab,
    setActiveTabId,
    addTab,
    closeTab,
    setTabSql,
    setTabExplainEnabled,
    executeTab,
    updateTab,
    selectTableIntoTab,
    navigateToQuery,
    setSelectedRow,

    // History
    history,
    favorites,
    toggleFavorite,
    clearHistory,
    pushHistory,

    // Transactions
    txState,
    beginTransaction,
    commitTransaction,
    rollbackTransaction,

    // Time-travel
    timeTravelMode,
    setTimeTravelMode,
    timeTravelLSN,
    setTimeTravelLSN,
    maxLSN,
    refreshMaxLSN,
    scrubToLSN,

    // Detail panel
    detailPanelOpen,
    setDetailPanelOpen,
  }
}
