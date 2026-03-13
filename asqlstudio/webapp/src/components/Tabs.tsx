import type { ReactNode } from 'react'

export type TabId = 'home' | 'workspace' | 'designer' | 'dashboard' | 'cluster' | 'time-explorer' | 'fixtures' | 'recovery'

type TabDef = {
  id: TabId
  label: string
  icon: ReactNode
  badge?: string | number
}

type Props = {
  tabs: TabDef[]
  active: TabId
  onChange: (id: TabId) => void
}

export function TabBar({ tabs, active, onChange }: Props) {
  return (
    <div className="tab-bar">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          className={`tab-item ${tab.id === active ? 'active' : ''}`}
          onClick={() => onChange(tab.id)}
        >
          <span className="tab-icon">{tab.icon}</span>
          <span>{tab.label}</span>
          {tab.badge !== undefined && <span className="tab-badge">{tab.badge}</span>}
        </button>
      ))}
    </div>
  )
}
