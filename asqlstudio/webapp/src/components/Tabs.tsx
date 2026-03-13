import type { ReactNode } from 'react'
import { IconChevronDown } from './Icons'

export type TabId = 'home' | 'workspace' | 'designer' | 'sql-details' | 'change-review' | 'dashboard' | 'cluster' | 'time-explorer' | 'fixtures' | 'recovery' | 'entities'

type TabDef = {
  id: TabId
  label: string
  icon: ReactNode
  badge?: string | number
}

type StandaloneItem = {
  kind: 'standalone'
  id: TabId
  label: string
  icon: ReactNode
  badge?: string | number
}

type TabGroup = {
  kind: 'group'
  id: string
  label: string
  icon: ReactNode
  items: TabDef[]
}

export type GroupDef = StandaloneItem | TabGroup

type Props = {
  groups: GroupDef[]
  active: TabId
  onChange: (id: TabId) => void
}

function resolveActiveGroup(groups: GroupDef[], active: TabId): GroupDef | null {
  for (const g of groups) {
    if (g.kind === 'standalone' && g.id === active) return g
    if (g.kind === 'group' && g.items.some((i) => i.id === active)) return g
  }
  return null
}

export function TabBar({ groups, active, onChange }: Props) {
  const activeGroup = resolveActiveGroup(groups, active)

  return (
    <div className="tab-bar-wrapper">
      <div className="tab-bar">
        {groups.map((g) => {
          if (g.kind === 'standalone') {
            return (
              <button
                key={g.id}
                className={`tab-item ${g.id === active ? 'active' : ''}`}
                onClick={() => onChange(g.id)}
              >
                <span className="tab-icon">{g.icon}</span>
                <span>{g.label}</span>
                {g.badge !== undefined && <span className="tab-badge">{g.badge}</span>}
              </button>
            )
          }

          const isActiveGroup = activeGroup?.kind === 'group' && activeGroup.id === g.id
          const activeItem = isActiveGroup ? g.items.find((i) => i.id === active) : null
          const totalBadge = g.items.reduce<number>(
            (sum, i) => sum + (typeof i.badge === 'number' ? i.badge : 0),
            0,
          )

          return (
            <button
              key={g.id}
              className={`tab-item tab-group-btn ${isActiveGroup ? 'active' : ''}`}
              onClick={() => { if (!isActiveGroup) onChange(g.items[0].id) }}
            >
              <span className="tab-icon">{activeItem?.icon ?? g.icon}</span>
              <span className="tab-group-label-block">
                {isActiveGroup && activeItem ? (
                  <>
                    <span className="tab-group-name">{g.label}</span>
                    <span className="tab-group-sep">·</span>
                    <span>{activeItem.label}</span>
                  </>
                ) : (
                  g.label
                )}
              </span>
              {totalBadge > 0 && <span className="tab-badge">{totalBadge}</span>}
              <span className={`tab-group-chevron${isActiveGroup ? ' open' : ''}`}>
                <IconChevronDown />
              </span>
            </button>
          )
        })}
      </div>

      {activeGroup?.kind === 'group' && (
        <div className="tab-bar-secondary">
          {activeGroup.items.map((item) => (
            <button
              key={item.id}
              className={`tab-item-secondary ${item.id === active ? 'active' : ''}`}
              onClick={() => onChange(item.id)}
            >
              <span className="tab-icon">{item.icon}</span>
              <span>{item.label}</span>
              {item.badge !== undefined && <span className="tab-badge">{item.badge}</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
