import { useMemo, useRef, useState, useEffect } from 'react'
import type { HistoryEntry, TableInfo } from '../types/workspace'
import type { TabId } from './Tabs'
import { IconSearch } from './Icons'

type Command = {
  id: string
  category: 'Tables' | 'Queries' | 'Actions' | 'Navigation'
  label: string
  description: string
  action: () => void
}

type Props = {
  tables: TableInfo[]
  history: HistoryEntry[]
  favorites: HistoryEntry[]
  onSelectTable: (name: string) => void
  onSetSql: (sql: string) => void
  onAddTab: () => void
  onToggleTimeTravel: () => void
  onToggleDetailPanel: () => void
  onNavigate: (tab: TabId) => void
  onClose: () => void
}

export function CommandPalette({
  tables,
  history,
  favorites,
  onSelectTable,
  onSetSql,
  onAddTab,
  onToggleTimeTravel,
  onToggleDetailPanel,
  onNavigate,
  onClose,
}: Props) {
  const [query, setQuery] = useState('')
  const [selectedIndex, setSelectedIndex] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  const commands = useMemo<Command[]>(() => {
    const cmds: Command[] = []

    // Tables
    for (const t of tables) {
      cmds.push({
        id: `table-${t.name}`,
        category: 'Tables',
        label: `Browse ${t.name}`,
        description: `SELECT * FROM ${t.name}`,
        action: () => onSelectTable(t.name),
      })
    }

    // Favorites
    for (const f of favorites) {
      cmds.push({
        id: `fav-${f.sql}`,
        category: 'Queries',
        label: f.sql.slice(0, 60),
        description: 'Favorite',
        action: () => onSetSql(f.sql),
      })
    }

    // Recent queries (limit 20)
    for (const h of history.slice(0, 20)) {
      if (favorites.some((f) => f.sql === h.sql)) continue
      cmds.push({
        id: `hist-${h.ts}`,
        category: 'Queries',
        label: h.sql.slice(0, 60),
        description: `${h.duration.toFixed(0)}ms${h.ok ? '' : ' (failed)'}`,
        action: () => onSetSql(h.sql),
      })
    }

    // Actions
    cmds.push({
      id: 'action-new-tab',
      category: 'Actions',
      label: 'New Tab',
      description: 'Open a new query tab',
      action: onAddTab,
    })
    cmds.push({
      id: 'action-time-travel',
      category: 'Actions',
      label: 'Toggle Time-Travel',
      description: 'Enable/disable time-travel mode',
      action: onToggleTimeTravel,
    })
    cmds.push({
      id: 'action-detail',
      category: 'Actions',
      label: 'Toggle Detail Panel',
      description: 'Show/hide the row detail panel',
      action: onToggleDetailPanel,
    })

    // Navigation
    cmds.push({
      id: 'nav-workspace',
      category: 'Navigation',
      label: 'Go to Workspace',
      description: 'Switch to the Workspace tab',
      action: () => onNavigate('workspace'),
    })
    cmds.push({
      id: 'nav-designer',
      category: 'Navigation',
      label: 'Go to Designer',
      description: 'Switch to the Designer tab',
      action: () => onNavigate('designer'),
    })
    cmds.push({
      id: 'nav-dashboard',
      category: 'Navigation',
      label: 'Go to Dashboard',
      description: 'Switch to the Dashboard tab',
      action: () => onNavigate('dashboard'),
    })
    cmds.push({
      id: 'nav-cluster',
      category: 'Navigation',
      label: 'Go to Cluster',
      description: 'Switch to the Cluster tab',
      action: () => onNavigate('cluster'),
    })

    return cmds
  }, [
    tables,
    history,
    favorites,
    onSelectTable,
    onSetSql,
    onAddTab,
    onToggleTimeTravel,
    onToggleDetailPanel,
    onNavigate,
  ])

  const filtered = useMemo(() => {
    if (!query.trim()) return commands
    const q = query.toLowerCase()
    return commands.filter(
      (c) =>
        c.label.toLowerCase().includes(q) ||
        c.description.toLowerCase().includes(q) ||
        c.category.toLowerCase().includes(q),
    )
  }, [commands, query])

  useEffect(() => {
    setSelectedIndex(0)
  }, [query])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setSelectedIndex((i) => Math.min(i + 1, filtered.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setSelectedIndex((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (filtered[selectedIndex]) {
        filtered[selectedIndex].action()
        onClose()
      }
    } else if (e.key === 'Escape') {
      onClose()
    }
  }

  // Scroll selected item into view
  useEffect(() => {
    const list = listRef.current
    if (!list) return
    const item = list.children[selectedIndex] as HTMLElement | undefined
    item?.scrollIntoView({ block: 'nearest' })
  }, [selectedIndex])

  // Group by category
  const grouped = useMemo(() => {
    const groups: Record<string, Command[]> = {}
    for (const cmd of filtered) {
      if (!groups[cmd.category]) groups[cmd.category] = []
      groups[cmd.category].push(cmd)
    }
    return groups
  }, [filtered])

  let flatIndex = -1

  return (
    <div className="cmd-overlay" onClick={onClose}>
      <div className="cmd-palette" onClick={(e) => e.stopPropagation()}>
        <div className="cmd-input-wrap">
          <IconSearch />
          <input
            ref={inputRef}
            className="cmd-input"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Search tables, queries, actions..."
          />
        </div>
        <div className="cmd-list" ref={listRef}>
          {filtered.length === 0 && (
            <div className="cmd-empty">No results</div>
          )}
          {Object.entries(grouped).map(([category, cmds]) => (
            <div key={category}>
              <div className="cmd-category">{category}</div>
              {cmds.map((cmd) => {
                flatIndex++
                const idx = flatIndex
                return (
                  <button
                    key={cmd.id}
                    className={`cmd-item ${idx === selectedIndex ? 'selected' : ''}`}
                    onClick={() => {
                      cmd.action()
                      onClose()
                    }}
                    onMouseEnter={() => setSelectedIndex(idx)}
                  >
                    <span className="cmd-item-label">{cmd.label}</span>
                    <span className="cmd-item-desc">{cmd.description}</span>
                  </button>
                )
              })}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
