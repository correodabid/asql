import type { WorkspaceTab } from '../types/workspace'
import { IconPlus } from './Icons'

type Props = {
  tabs: WorkspaceTab[]
  activeTabId: string
  onSelect: (id: string) => void
  onClose: (id: string) => void
  onAdd: () => void
}

export function WorkspaceTabBar({ tabs, activeTabId, onSelect, onClose, onAdd }: Props) {
  return (
    <div className="ws-tab-bar">
      <div className="ws-tabs-scroll">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`ws-tab ${tab.id === activeTabId ? 'active' : ''} ${tab.explainEnabled ? 'explain-enabled' : ''}`}
            onClick={() => onSelect(tab.id)}
          >
            <span className="ws-tab-label">{tab.label}</span>
            {tab.explainEnabled && <span className="ws-tab-badge">EXPLAIN</span>}
            {tabs.length > 1 && (
              <span
                className="ws-tab-close"
                onClick={(e) => {
                  e.stopPropagation()
                  onClose(tab.id)
                }}
              >
                x
              </span>
            )}
          </button>
        ))}
      </div>
      <button className="ws-tab-add" onClick={onAdd} title="New tab">
        <IconPlus />
      </button>
    </div>
  )
}
