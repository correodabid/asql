import { useCallback, useEffect, useRef, useState } from 'react'

export type HeartbeatStatus = 'connected' | 'disconnected' | 'checking'

export function useHeartbeat(intervalMs = 10000) {
  const [status, setStatus] = useState<HeartbeatStatus>('checking')
  const [latency, setLatency] = useState<number | null>(null)
  const [lastCheck, setLastCheck] = useState(0)
  const [failCount, setFailCount] = useState(0)
  const timerRef = useRef<ReturnType<typeof setInterval>>(undefined)

  const check = useCallback(async () => {
    const start = performance.now()
    try {
      const res = await fetch('/api/health', { signal: AbortSignal.timeout(5000) })
      const elapsed = performance.now() - start
      if (res.ok) {
        setStatus('connected')
        setLatency(Math.round(elapsed))
        setFailCount(0)
      } else {
        setStatus('disconnected')
        setLatency(null)
        setFailCount((c) => c + 1)
      }
    } catch {
      setStatus('disconnected')
      setLatency(null)
      setFailCount((c) => c + 1)
    }
    setLastCheck(Date.now())
  }, [])

  useEffect(() => {
    check()
    timerRef.current = setInterval(check, intervalMs)
    return () => clearInterval(timerRef.current)
  }, [check, intervalMs])

  return { status, latency, lastCheck, failCount, check }
}
