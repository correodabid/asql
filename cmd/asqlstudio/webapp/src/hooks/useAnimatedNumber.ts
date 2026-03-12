import { useEffect, useRef, useState } from 'react'

export function useAnimatedNumber(target: number, duration = 400): number {
  const [current, setCurrent] = useState(target)
  const rafRef = useRef<number>(0)
  const startRef = useRef({ value: target, time: 0 })

  useEffect(() => {
    const from = current
    if (from === target) return

    startRef.current = { value: from, time: performance.now() }

    const animate = (now: number) => {
      const elapsed = now - startRef.current.time
      const progress = Math.min(elapsed / duration, 1)
      // ease-out cubic
      const eased = 1 - Math.pow(1 - progress, 3)
      const value = startRef.current.value + (target - startRef.current.value) * eased

      setCurrent(progress >= 1 ? target : value)
      if (progress < 1) rafRef.current = requestAnimationFrame(animate)
    }

    rafRef.current = requestAnimationFrame(animate)
    return () => cancelAnimationFrame(rafRef.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, duration])

  return current
}
