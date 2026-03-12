import { useState } from 'react'
import type { SavedQuery } from '../hooks/useSavedQueries'
import { IconPlay, IconPlus, IconTrash } from './Icons'

type Props = {
  queries: SavedQuery[]
  currentSQL: string
  onLoad: (sql: string) => void
  onSave: (name: string, sql: string) => void
  onUpdate: (id: string, updates: { name?: string; sql?: string }) => void
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
}

export function SavedQueries({
  queries,
  currentSQL,
  onLoad,
  onSave,
  onUpdate,
  onDelete,
  onDuplicate,
}: Props) {
  const [saveName, setSaveName] = useState('')
  const [editing, setEditing] = useState<string | null>(null)
  const [editName, setEditName] = useState('')

  const handleSave = () => {
    const name = saveName.trim()
    if (!name || !currentSQL.trim()) return
    onSave(name, currentSQL)
    setSaveName('')
  }

  const handleStartRename = (q: SavedQuery) => {
    setEditing(q.id)
    setEditName(q.name)
  }

  const handleFinishRename = (id: string) => {
    const name = editName.trim()
    if (name) onUpdate(id, { name })
    setEditing(null)
  }

  return (
    <div className="saved-queries">
      {/* Save current */}
      <div className="saved-queries-form">
        <input
          className="editor-input mono"
          placeholder="Query name..."
          value={saveName}
          onChange={(e) => setSaveName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleSave()}
        />
        <button
          className="toolbar-btn primary"
          onClick={handleSave}
          disabled={!saveName.trim() || !currentSQL.trim()}
          title="Save current query"
        >
          <IconPlus /> Save
        </button>
      </div>

      {/* List */}
      {queries.length === 0 ? (
        <div className="panel-empty" style={{ padding: '24px 16px' }}>
          <span className="text-muted" style={{ fontSize: 12 }}>No saved queries yet</span>
        </div>
      ) : (
        <div className="saved-queries-list">
          {queries.map((q) => (
            <div key={q.id} className="saved-query-item">
              <div className="saved-query-header">
                {editing === q.id ? (
                  <input
                    className="editor-input mono"
                    value={editName}
                    onChange={(e) => setEditName(e.target.value)}
                    onBlur={() => handleFinishRename(q.id)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleFinishRename(q.id)
                      if (e.key === 'Escape') setEditing(null)
                    }}
                    autoFocus
                    style={{ padding: '2px 6px', fontSize: 11 }}
                  />
                ) : (
                  <span
                    className="saved-query-name"
                    onDoubleClick={() => handleStartRename(q)}
                    title="Double-click to rename"
                  >
                    {q.name}
                  </span>
                )}
                <div className="saved-query-actions">
                  <button className="icon-btn" onClick={() => onLoad(q.sql)} title="Load into editor">
                    <IconPlay />
                  </button>
                  <button className="icon-btn" onClick={() => onDuplicate(q.id)} title="Duplicate">
                    <IconPlus />
                  </button>
                  <button className="icon-btn danger" onClick={() => onDelete(q.id)} title="Delete">
                    <IconTrash />
                  </button>
                </div>
              </div>
              <div className="saved-query-sql">{q.sql}</div>
              <div className="saved-query-meta">
                {new Date(q.updatedAt).toLocaleDateString()} {new Date(q.updatedAt).toLocaleTimeString()}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
