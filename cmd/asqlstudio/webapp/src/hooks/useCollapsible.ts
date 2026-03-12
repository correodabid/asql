import { useCallback, useState } from 'react'

const STORAGE_PREFIX = 'dash-collapse-'

export function useCollapsible(key: string, defaultOpen = true) {
  const [open, setOpen] = useState(() => {
    try {
      const stored = localStorage.getItem(STORAGE_PREFIX + key)
      return stored !== null ? stored === '1' : defaultOpen
    } catch {
      return defaultOpen
    }
  })

  const toggle = useCallback(() => {
    setOpen(prev => {
      const next = !prev
      try { localStorage.setItem(STORAGE_PREFIX + key, next ? '1' : '0') } catch { /* noop */ }
      return next
    })
  }, [key])

  return { open, toggle }
}
