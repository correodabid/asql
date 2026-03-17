import { useCallback, useEffect, useState } from 'react'
import { api } from '../lib/api'

export function useDomains(reloadKey = 0) {
  const [domains, setDomains] = useState<string[]>([])
  const [loading, setLoading] = useState(false)

  const refresh = useCallback(async () => {
    setLoading(true)
    try {
      const resp = await api<{ domains: string[] }>('/api/domains', 'GET')
      setDomains(resp.domains || [])
    } catch {
      // leave previous
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh()
  }, [refresh, reloadKey])

  return { domains, loading, refresh }
}
