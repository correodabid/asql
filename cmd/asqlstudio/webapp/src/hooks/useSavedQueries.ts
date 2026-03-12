import { useCallback, useEffect, useState } from 'react'

export type SavedQuery = {
  id: string
  name: string
  sql: string
  createdAt: number
  updatedAt: number
}

const STORAGE_KEY = 'asql-saved-queries'

function loadFromStorage(): SavedQuery[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? JSON.parse(raw) : []
  } catch {
    return []
  }
}

function saveToStorage(queries: SavedQuery[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(queries))
}

export function useSavedQueries() {
  const [queries, setQueries] = useState<SavedQuery[]>(() => loadFromStorage())

  // Sync to localStorage on change
  useEffect(() => {
    saveToStorage(queries)
  }, [queries])

  const addQuery = useCallback((name: string, sql: string) => {
    const now = Date.now()
    const newQuery: SavedQuery = {
      id: `sq-${now}-${Math.random().toString(36).slice(2, 6)}`,
      name,
      sql,
      createdAt: now,
      updatedAt: now,
    }
    setQueries((prev) => [newQuery, ...prev])
    return newQuery
  }, [])

  const updateQuery = useCallback((id: string, updates: { name?: string; sql?: string }) => {
    setQueries((prev) =>
      prev.map((q) =>
        q.id === id
          ? { ...q, ...updates, updatedAt: Date.now() }
          : q,
      ),
    )
  }, [])

  const deleteQuery = useCallback((id: string) => {
    setQueries((prev) => prev.filter((q) => q.id !== id))
  }, [])

  const duplicateQuery = useCallback((id: string) => {
    setQueries((prev) => {
      const source = prev.find((q) => q.id === id)
      if (!source) return prev
      const now = Date.now()
      const copy: SavedQuery = {
        id: `sq-${now}-${Math.random().toString(36).slice(2, 6)}`,
        name: `${source.name} (copy)`,
        sql: source.sql,
        createdAt: now,
        updatedAt: now,
      }
      return [copy, ...prev]
    })
  }, [])

  return { queries, addQuery, updateQuery, deleteQuery, duplicateQuery }
}
