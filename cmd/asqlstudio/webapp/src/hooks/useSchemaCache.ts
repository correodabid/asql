import { useCallback, useRef, useState } from 'react'
import { api } from '../lib/api'
import type { EntityDefinition, SchemaModel, SchemaTable } from '../schema'
import type { ForeignKeyLink, ReverseFK, TableInfo } from '../types/workspace'

type BaselineResponse = {
  status: string
  baseline: SchemaModel
}

export function useSchemaCache(domain: string) {
  const [tables, setTables] = useState<TableInfo[]>([])
  const [baseline, setBaseline] = useState<SchemaModel | null>(null)
  const [loading, setLoading] = useState(false)
  const [tableCounts, setTableCounts] = useState<Record<string, number>>({})
  const baselineRef = useRef<Promise<SchemaModel | null> | null>(null)
  const countsRef = useRef<Promise<void> | null>(null)

  // Reset baseline cache when domain changes so it reloads for the new domain
  const prevDomainRef = useRef(domain)
  if (prevDomainRef.current !== domain) {
    prevDomainRef.current = domain
    baselineRef.current = null
    countsRef.current = null
    setBaseline(null)
    setTableCounts({})
  }

  const loadTables = useCallback(async () => {
    setLoading(true)
    try {
      const resp = await api<{ tables: TableInfo[] }>(
        `/api/schema/tables?domain=${encodeURIComponent(domain)}`,
        'GET',
      )
      setTables(resp.tables || [])
    } catch {
      // ignore
    } finally {
      setLoading(false)
    }
  }, [domain])

  const loadBaseline = useCallback(async () => {
    if (baselineRef.current) return baselineRef.current
    const promise = (async () => {
      try {
        const response = await api<BaselineResponse>('/api/schema/load-baseline', 'POST', {
          domain,
        })
        const data = response.baseline
        if (data) {
          setBaseline(data)
        }
        return data
      } catch {
        baselineRef.current = null
        return null
      }
    })()
    baselineRef.current = promise
    return promise
  }, [domain])

  const getTable = useCallback(
    (name: string): SchemaTable | undefined => {
      return baseline?.tables.find((t) => t.name === name)
    },
    [baseline],
  )

  const getPKColumns = useCallback(
    (tableName: string): string[] => {
      // First try from tables list (fast)
      const info = tables.find((t) => t.name === tableName)
      if (info) return info.pk_columns

      // Fallback to baseline schema
      const schemaTable = getTable(tableName)
      if (!schemaTable) return []
      return schemaTable.columns.filter((c) => c.primary_key).map((c) => c.name)
    },
    [tables, getTable],
  )

  const getForeignKeys = useCallback(
    (tableName: string): ForeignKeyLink[] => {
      const schemaTable = getTable(tableName)
      if (!schemaTable) return []
      return schemaTable.columns
        .filter((c) => c.references)
        .map((c) => ({
          column: c.name,
          refTable: c.references!.table,
          refColumn: c.references!.column,
        }))
    },
    [getTable],
  )

  const getReferencedBy = useCallback(
    (tableName: string): ReverseFK[] => {
      if (!baseline) return []
      const refs: ReverseFK[] = []
      for (const table of baseline.tables) {
        for (const col of table.columns) {
          if (col.references && col.references.table === tableName) {
            refs.push({
              table: table.name,
              column: col.name,
              refColumn: col.references.column,
            })
          }
        }
      }
      return refs
    },
    [baseline],
  )

  const getEntityForTable = useCallback(
    (tableName: string): EntityDefinition | undefined => {
      if (!baseline?.entities) return undefined
      return baseline.entities.find((e) => e.tables.includes(tableName))
    },
    [baseline],
  )

  const loadTableCounts = useCallback(async (tableNames: string[]) => {
    if (countsRef.current || tableNames.length === 0) return
    const promise = (async () => {
      const counts: Record<string, number> = {}
      // Query counts in parallel batches
      const promises = tableNames.map(async (name) => {
        try {
          const resp = await api<{
            rows?: Record<string, unknown>[]
          }>('/api/read-query', 'POST', {
            sql: `SELECT COUNT(*) AS cnt FROM ${name};`,
            domains: [domain],
            consistency: 'strong',
          })
          const rows = resp.rows || []
          if (rows.length > 0 && rows[0].cnt !== undefined) {
            counts[name] = Number(rows[0].cnt)
          }
        } catch {
          // Table might not be queryable — skip
        }
      })
      await Promise.all(promises)
      setTableCounts(counts)
    })()
    countsRef.current = promise
    return promise
  }, [domain])

  return {
    tables,
    baseline,
    loading,
    tableCounts,
    loadTables,
    loadBaseline,
    loadTableCounts,
    getTable,
    getPKColumns,
    getForeignKeys,
    getReferencedBy,
    getEntityForTable,
  }
}
