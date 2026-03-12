import { useEffect, useRef, useState } from 'react'

type Dot = { id: number; x: number; size: number; opacity: number }

type ActivityStreamProps = {
  rate: number
  color?: string
  width?: number
  height?: number
}

let _dotId = 0

export function ActivityStream({ rate, color = 'var(--accent)', width = 200, height = 24 }: ActivityStreamProps) {
  const [dots, setDots] = useState<Dot[]>([])
  const rafRef = useRef<number>(0)
  const lastSpawnRef = useRef(0)

  useEffect(() => {
    if (rate <= 0) return

    const spawnInterval = Math.max(80, 1000 / Math.min(rate, 12))
    let running = true

    const tick = (now: number) => {
      if (!running) return

      // Spawn new dot if enough time has passed
      if (now - lastSpawnRef.current >= spawnInterval) {
        lastSpawnRef.current = now
        const newDot: Dot = {
          id: ++_dotId,
          x: -4,
          size: 3 + Math.random() * 3,
          opacity: 0.6 + Math.random() * 0.4,
        }
        setDots(prev => [...prev, newDot])
      }

      // Advance all dots
      setDots(prev => {
        const speed = 1.2 + rate * 0.1
        return prev
          .map(d => ({ ...d, x: d.x + speed, opacity: d.x > width * 0.7 ? d.opacity * 0.95 : d.opacity }))
          .filter(d => d.x < width + 10)
      })

      rafRef.current = requestAnimationFrame(tick)
    }

    rafRef.current = requestAnimationFrame(tick)
    return () => {
      running = false
      cancelAnimationFrame(rafRef.current)
    }
  }, [rate, width])

  return (
    <svg className="activity-stream" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
      {dots.map(d => (
        <circle
          key={d.id}
          cx={d.x}
          cy={height / 2 + (Math.sin(d.id * 0.7) * height * 0.3)}
          r={d.size / 2}
          fill={color}
          opacity={d.opacity}
        />
      ))}
    </svg>
  )
}
