import { useCallback, useState } from 'react'

type JsonNodeProps = {
  data: unknown
  depth?: number
  keyName?: string
}

function JsonNode({ data, depth = 0, keyName }: JsonNodeProps) {
  const [collapsed, setCollapsed] = useState(depth > 1)

  const renderValue = (value: unknown): React.ReactNode => {
    if (value === null) return <span className="jv-null">null</span>
    if (typeof value === 'boolean') return <span className="jv-bool">{String(value)}</span>
    if (typeof value === 'number') return <span className="jv-num">{value}</span>
    if (typeof value === 'string') return <span className="jv-str">"{value}"</span>
    return null
  }

  const prefix = keyName !== undefined ? <span className="jv-key">"{keyName}": </span> : null

  if (data === null || typeof data !== 'object') {
    return (
      <div className="jv-line" style={{ paddingLeft: depth * 16 }}>
        {prefix}{renderValue(data)}
      </div>
    )
  }

  const isArray = Array.isArray(data)
  const entries = isArray ? data.map((v, i) => [String(i), v] as const) : Object.entries(data)
  const bracket = isArray ? ['[', ']'] : ['{', '}']

  if (entries.length === 0) {
    return (
      <div className="jv-line" style={{ paddingLeft: depth * 16 }}>
        {prefix}{bracket[0]}{bracket[1]}
      </div>
    )
  }

  const toggle = useCallback(() => setCollapsed((c) => !c), [])

  return (
    <div className="jv-node">
      <div
        className="jv-line jv-collapsible"
        style={{ paddingLeft: depth * 16 }}
        onClick={toggle}
      >
        <span className="jv-toggle">{collapsed ? '\u25b6' : '\u25bc'}</span>
        {prefix}{bracket[0]}
        {collapsed && <span className="jv-ellipsis"> ... {entries.length} items {bracket[1]}</span>}
      </div>
      {!collapsed && (
        <>
          {entries.map(([k, v]) => (
            <JsonNode
              key={k}
              data={v}
              depth={depth + 1}
              keyName={isArray ? undefined : k}
            />
          ))}
          <div className="jv-line" style={{ paddingLeft: depth * 16 }}>
            {bracket[1]}
          </div>
        </>
      )}
    </div>
  )
}

type Props = {
  data: unknown
}

export function JsonTreeView({ data }: Props) {
  return (
    <div className="json-tree">
      <JsonNode data={data} />
    </div>
  )
}
