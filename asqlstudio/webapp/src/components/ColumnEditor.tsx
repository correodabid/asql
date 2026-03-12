import type { Dispatch, SetStateAction } from 'react'
import { clone, type SchemaColumn, type SchemaModel, type SchemaTable } from '../schema'
import { IconKey, IconLink, IconNullable, IconPlus, IconTrash, IconUnique } from './Icons'

type Props = {
  model: SchemaModel
  setModel: Dispatch<SetStateAction<SchemaModel>>
  selectedTable: number
  selectedColumn: number
  setSelectedColumn: Dispatch<SetStateAction<number>>
  activeTable: SchemaTable | null
  activeColumn: SchemaColumn | null
  updateTable: (updater: (table: SchemaTable) => SchemaTable) => void
  updateColumn: (updater: (column: SchemaColumn) => SchemaColumn) => void
}

const COLUMN_TYPES = ['INT', 'TEXT', 'JSON', 'BOOL', 'FLOAT', 'TIMESTAMP'] as const

function inferDefaultMode(value: string): string {
  if (!value) return 'none'
  const upper = value.toUpperCase()
  if (upper === 'NULL') return 'null'
  if (upper === 'AUTOINCREMENT') return 'autoincrement'
  if (upper === 'UUID_V7') return 'uuid_v7'
  return 'value'
}

export function ColumnEditor({
  model,
  setModel,
  selectedTable: _selectedTable,
  selectedColumn,
  setSelectedColumn,
  activeTable,
  activeColumn,
  updateTable,
  updateColumn,
}: Props) {
  if (!activeTable) {
    return (
      <div className="column-editor">
        <div className="panel-empty">
          <span className="text-muted">Select a table to edit columns</span>
        </div>
      </div>
    )
  }

  const addColumn = () => {
    updateTable((table) => ({
      ...table,
      columns: [
        ...table.columns,
        { name: `col_${table.columns.length + 1}`, type: 'TEXT', nullable: true, primary_key: false, unique: false, default_value: '' },
      ],
    }))
    setSelectedColumn(activeTable.columns.length)
  }

  const removeColumn = () => {
    if (activeTable.columns.length <= 1) return
    updateTable((table) => {
      const next = clone(table)
      next.columns.splice(selectedColumn, 1)
      return next
    })
    setSelectedColumn(Math.max(0, selectedColumn - 1))
  }

  return (
    <div className="column-editor">
      {/* Table name editor */}
      <div className="editor-section">
        <label className="editor-label">Table Name</label>
        <input
          className="editor-input"
          value={activeTable.name}
          onChange={(e) => updateTable((t) => ({ ...t, name: e.target.value }))}
        />
      </div>

      {/* Domain editor */}
      <div className="editor-section">
        <label className="editor-label">Domain</label>
        <input
          className="editor-input"
          value={model.domain}
          onChange={(e) => setModel((c) => ({ ...c, domain: e.target.value }))}
        />
      </div>

      {/* Column list */}
      <div className="editor-section">
        <div className="editor-section-header">
          <label className="editor-label">Columns</label>
          <div className="editor-actions">
            <button className="icon-btn" onClick={addColumn} title="Add column"><IconPlus /></button>
            <button className="icon-btn danger" onClick={removeColumn} title="Remove column" disabled={activeTable.columns.length <= 1}><IconTrash /></button>
          </div>
        </div>

        <div className="column-list">
          {activeTable.columns.map((col, i) => (
            <button
              key={`${col.name}-${i}`}
              className={`column-list-item ${i === selectedColumn ? 'active' : ''}`}
              onClick={() => setSelectedColumn(i)}
            >
              <span className="col-indicator">
                {col.primary_key ? <IconKey /> : col.references ? <IconLink /> : <span className="col-dot" />}
              </span>
              <span className="col-list-name">{col.name || '—'}</span>
              <span className="col-list-type">{col.type}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Column detail editor */}
      {activeColumn && (
        <div className="editor-section column-detail">
          <label className="editor-label">Column Properties</label>

          <div className="editor-row">
            <div className="editor-field">
              <label className="editor-field-label">Name</label>
              <input
                className="editor-input mono"
                value={activeColumn.name}
                onChange={(e) => updateColumn((c) => ({ ...c, name: e.target.value }))}
              />
            </div>
            <div className="editor-field">
              <label className="editor-field-label">Type</label>
              <select
                className="editor-input"
                value={activeColumn.type}
                onChange={(e) => updateColumn((c) => ({ ...c, type: e.target.value }))}
              >
                {COLUMN_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
          </div>

          <div className="editor-field">
            <label className="editor-field-label">Default Value</label>
            <div className="default-mode-row">
              <select
                className="editor-input default-mode-select"
                value={inferDefaultMode(activeColumn.default_value || '')}
                onChange={(e) => {
                  const mode = e.target.value
                  if (mode === 'none') updateColumn((c) => ({ ...c, default_value: '' }))
                  else if (mode === 'null') updateColumn((c) => ({ ...c, default_value: 'NULL' }))
                  else if (mode === 'autoincrement') updateColumn((c) => ({ ...c, default_value: 'AUTOINCREMENT' }))
                  else if (mode === 'uuid_v7') updateColumn((c) => ({ ...c, default_value: 'UUID_V7' }))
                  else if (mode === 'value') updateColumn((c) => ({ ...c, default_value: c.default_value && !['NULL', 'AUTOINCREMENT', 'UUID_V7'].includes(c.default_value.toUpperCase()) ? c.default_value : '0' }))
                }}
              >
                <option value="none">None</option>
                <option value="value">Value</option>
                <option value="null">NULL</option>
                <option value="autoincrement">AUTOINCREMENT</option>
                <option value="uuid_v7">UUID_V7</option>
              </select>
              {inferDefaultMode(activeColumn.default_value || '') === 'value' && (
                <input
                  className="editor-input mono default-value-input"
                  value={activeColumn.default_value || ''}
                  placeholder="e.g. 0, 'hello', TRUE"
                  onChange={(e) => updateColumn((c) => ({ ...c, default_value: e.target.value }))}
                />
              )}
            </div>
          </div>

          <div className="constraint-grid">
            <label className={`constraint-toggle ${activeColumn.primary_key ? 'on' : ''}`}>
              <input
                type="checkbox"
                checked={activeColumn.primary_key}
                onChange={(e) => updateColumn((c) => ({ ...c, primary_key: e.target.checked, nullable: e.target.checked ? false : c.nullable }))}
              />
              <IconKey /> <span>Primary Key</span>
            </label>
            <label className={`constraint-toggle ${activeColumn.unique ? 'on' : ''}`}>
              <input type="checkbox" checked={activeColumn.unique} onChange={(e) => updateColumn((c) => ({ ...c, unique: e.target.checked }))} />
              <IconUnique /> <span>Unique</span>
            </label>
            <label className={`constraint-toggle ${activeColumn.nullable ? 'on' : ''}`}>
              <input type="checkbox" checked={activeColumn.nullable} onChange={(e) => updateColumn((c) => ({ ...c, nullable: e.target.checked }))} />
              <IconNullable /> <span>Nullable</span>
            </label>
          </div>

          {/* Foreign Key */}
          <div className="editor-fk">
            <label className="editor-field-label"><IconLink /> Foreign Key Reference</label>
            <div className="editor-row">
              <div className="editor-field">
                <input
                  className="editor-input mono"
                  placeholder="table"
                  value={activeColumn.references?.table || ''}
                  onChange={(e) => updateColumn((c) => ({ ...c, references: { table: e.target.value, column: c.references?.column || '' } }))}
                />
              </div>
              <span className="fk-dot">.</span>
              <div className="editor-field">
                <input
                  className="editor-input mono"
                  placeholder="column"
                  value={activeColumn.references?.column || ''}
                  onChange={(e) => updateColumn((c) => ({ ...c, references: { table: c.references?.table || '', column: e.target.value } }))}
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Versioned Foreign Keys (table-level) */}
      <div className="editor-section">
        <div className="editor-section-header">
          <label className="editor-label">Versioned Foreign Keys</label>
          <div className="editor-actions">
            <button
              className="icon-btn"
              onClick={() =>
                updateTable((t) => ({
                  ...t,
                  versioned_foreign_keys: [
                    ...(t.versioned_foreign_keys || []),
                    { column: '', lsn_column: '', references_domain: '', references_table: '', references_column: '' },
                  ],
                }))
              }
              title="Add versioned FK"
            >
              <IconPlus />
            </button>
          </div>
        </div>

        {(activeTable.versioned_foreign_keys || []).map((vfk, i) => (
          <div key={i} className="vfk-entry">
            <div className="editor-row">
              <div className="editor-field">
                <label className="editor-field-label">Column</label>
                <input
                  className="editor-input mono"
                  placeholder="fk_column"
                  value={vfk.column}
                  onChange={(e) =>
                    updateTable((t) => {
                      const next = clone(t)
                      const vfks = next.versioned_foreign_keys || []
                      vfks[i] = { ...vfks[i], column: e.target.value }
                      next.versioned_foreign_keys = vfks
                      return next
                    })
                  }
                />
              </div>
              <div className="editor-field">
                <label className="editor-field-label">LSN Column</label>
                <input
                  className="editor-input mono"
                  placeholder="lsn_column"
                  value={vfk.lsn_column}
                  onChange={(e) =>
                    updateTable((t) => {
                      const next = clone(t)
                      const vfks = next.versioned_foreign_keys || []
                      vfks[i] = { ...vfks[i], lsn_column: e.target.value }
                      next.versioned_foreign_keys = vfks
                      return next
                    })
                  }
                />
              </div>
            </div>
            <div className="editor-row">
              <div className="editor-field">
                <label className="editor-field-label">Domain</label>
                <input
                  className="editor-input mono"
                  placeholder="domain"
                  value={vfk.references_domain}
                  onChange={(e) =>
                    updateTable((t) => {
                      const next = clone(t)
                      const vfks = next.versioned_foreign_keys || []
                      vfks[i] = { ...vfks[i], references_domain: e.target.value }
                      next.versioned_foreign_keys = vfks
                      return next
                    })
                  }
                />
              </div>
              <span className="fk-dot">.</span>
              <div className="editor-field">
                <label className="editor-field-label">Table</label>
                <input
                  className="editor-input mono"
                  placeholder="table"
                  value={vfk.references_table}
                  onChange={(e) =>
                    updateTable((t) => {
                      const next = clone(t)
                      const vfks = next.versioned_foreign_keys || []
                      vfks[i] = { ...vfks[i], references_table: e.target.value }
                      next.versioned_foreign_keys = vfks
                      return next
                    })
                  }
                />
              </div>
              <span className="fk-dot">(</span>
              <div className="editor-field">
                <label className="editor-field-label">Column</label>
                <input
                  className="editor-input mono"
                  placeholder="column"
                  value={vfk.references_column}
                  onChange={(e) =>
                    updateTable((t) => {
                      const next = clone(t)
                      const vfks = next.versioned_foreign_keys || []
                      vfks[i] = { ...vfks[i], references_column: e.target.value }
                      next.versioned_foreign_keys = vfks
                      return next
                    })
                  }
                />
              </div>
              <span className="fk-dot">)</span>
              <button
                className="icon-btn danger"
                title="Remove versioned FK"
                onClick={() =>
                  updateTable((t) => {
                    const next = clone(t)
                    const vfks = next.versioned_foreign_keys || []
                    vfks.splice(i, 1)
                    next.versioned_foreign_keys = vfks.length > 0 ? vfks : undefined
                    return next
                  })
                }
              >
                <IconTrash />
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
