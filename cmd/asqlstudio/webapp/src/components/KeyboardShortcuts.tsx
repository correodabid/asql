import { useCallback, useEffect } from 'react'

type Shortcut = {
  category: string
  shortcuts: { keys: string; description: string }[]
}

const SHORTCUTS: Shortcut[] = [
  {
    category: 'Global',
    shortcuts: [
      { keys: 'Cmd+K', description: 'Open command palette' },
      { keys: '?', description: 'Show keyboard shortcuts' },
      { keys: 'Cmd+1…7', description: 'Switch tabs' },
    ],
  },
  {
    category: 'Editor',
    shortcuts: [
      { keys: 'Cmd+Enter', description: 'Execute query' },
      { keys: 'Ctrl+Space', description: 'Show autocomplete' },
      { keys: 'Tab', description: 'Insert 2 spaces' },
      { keys: 'Cmd+/', description: 'Toggle line comment' },
      { keys: 'Escape', description: 'Dismiss autocomplete' },
    ],
  },
  {
    category: 'Results',
    shortcuts: [
      { keys: 'Click cell', description: 'Copy value to clipboard' },
      { keys: 'Double-click', description: 'Edit cell inline' },
      { keys: 'Right-click', description: 'Context menu (UPDATE/DELETE)' },
    ],
  },
  {
    category: 'Navigation',
    shortcuts: [
      { keys: 'Click table', description: 'SELECT * FROM table' },
      { keys: 'Click FK', description: 'Navigate to referenced row' },
      { keys: 'Cmd+N', description: 'New query tab' },
      { keys: 'Cmd+W', description: 'Close current tab' },
    ],
  },
]

type Props = {
  onClose: () => void
}

export function KeyboardShortcuts({ onClose }: Props) {
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape') onClose()
  }, [onClose])

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [handleKeyDown])

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="shortcuts-modal" onClick={(e) => e.stopPropagation()}>
        <div className="shortcuts-header">
          <span className="shortcuts-title">Keyboard Shortcuts</span>
          <button className="icon-btn" onClick={onClose}>x</button>
        </div>
        <div className="shortcuts-body">
          {SHORTCUTS.map((section) => (
            <div key={section.category} className="shortcuts-section">
              <div className="shortcuts-category">{section.category}</div>
              {section.shortcuts.map((s) => (
                <div key={s.keys} className="shortcuts-row">
                  <kbd className="shortcuts-keys">{s.keys}</kbd>
                  <span className="shortcuts-desc">{s.description}</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
