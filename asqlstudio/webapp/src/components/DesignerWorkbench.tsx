import { useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { StatementState } from './DDLPanel'
import { ColumnEditor } from './ColumnEditor'
import { ERDiagram } from './ERDiagram'
import { IconAlertTriangle, IconArrowRight, IconCheckCircle, IconChevronDown, IconChevronUp, IconCode, IconPlay, IconRefresh, IconSchema, IconShield, IconX } from './Icons'
import { IndexEditor } from './IndexEditor'
import { clone, type DiffOperation, type SchemaColumn, type SchemaModel, type SchemaTable } from '../schema'

const DESIGNER_GUIDANCE_KEY = 'asql-designer-guidance-dismissed'

function readGuidanceDismissed() {
  try {
    return localStorage.getItem(DESIGNER_GUIDANCE_KEY) === '1'
  } catch {
    return false
  }
}

function writeGuidanceDismissed(value: boolean) {
  try {
    localStorage.setItem(DESIGNER_GUIDANCE_KEY, value ? '1' : '0')
  } catch {
    // ignore localStorage failures
  }
}

type Props = {
  model: SchemaModel
  setModel: Dispatch<SetStateAction<SchemaModel>>
  selectedTable: number
  setSelectedTable: Dispatch<SetStateAction<number>>
  selectedColumn: number
  setSelectedColumn: Dispatch<SetStateAction<number>>
  selectedIndex: number
  setSelectedIndex: Dispatch<SetStateAction<number>>
  activeTable: SchemaTable | null
  activeColumn: SchemaColumn | null
  updateTable: (updater: (table: SchemaTable) => SchemaTable) => void
  updateColumn: (updater: (column: SchemaColumn) => SchemaColumn) => void
  designerTableCounts: Record<string, number>
  ddlStatements: string[]
  statementStates: StatementState[]
  diffSummary: string
  diffSafe: boolean | null
  diffOperations: DiffOperation[]
  diffWarnings: string[]
  onGenerateDDL: () => void
  onPreviewDiff: () => void
  onApplySafeDiff: () => void
  onExecuteAll: () => void
  onOpenDDL: () => void
  onOpenDiff: () => void
}

function summarizeOps(diffOperations: DiffOperation[]) {
  const safe = diffOperations.filter((op) => op.safe).length
  const unsafe = diffOperations.length - safe
  const adds = diffOperations.filter((op) => op.type.startsWith('add_')).length
  const drops = diffOperations.filter((op) => op.type.startsWith('drop_')).length
  return { safe, unsafe, adds, drops }
}

function sqlSnippet(sql: string) {
  return sql.replace(/\s+/g, ' ').trim()
}

export function DesignerWorkbench({
  model,
  setModel,
  selectedTable,
  setSelectedTable,
  selectedColumn,
  setSelectedColumn,
  selectedIndex,
  setSelectedIndex,
  activeTable,
  activeColumn,
  updateTable,
  updateColumn,
  designerTableCounts,
  ddlStatements,
  statementStates,
  diffSummary,
  diffSafe,
  diffOperations,
  diffWarnings,
  onGenerateDDL,
  onPreviewDiff,
  onApplySafeDiff,
  onExecuteAll,
  onOpenDDL,
  onOpenDiff,
}: Props) {
  const stats = summarizeOps(diffOperations)
  const executedStatements = statementStates.filter((state) => state.status === 'success').length
  const pendingStatements = statementStates.filter((state) => state.status === 'pending').length
  const previewOps = diffOperations.slice(0, 4)
  const previewDDL = ddlStatements.slice(0, 3)
  const [showGuidance, setShowGuidance] = useState(false)
  const [guidanceDismissed, setGuidanceDismissed] = useState(() => readGuidanceDismissed())

  const dismissGuidance = () => {
    setGuidanceDismissed(true)
    setShowGuidance(false)
    writeGuidanceDismissed(true)
  }

  return (
    <div className="designer-workbench">
      {!guidanceDismissed && (
        <div className="designer-guidance-banner">
          <div className="designer-guidance-copy">
            <div className="designer-guidance-title">Builder keeps modeling, change review, and SQL in one flow.</div>
            <div className="designer-guidance-description">Useful as orientation, but hidden by default so it does not compete with the actual work surface.</div>
          </div>
          <div className="designer-guidance-actions">
            <button className="ddl-action-btn" onClick={() => setShowGuidance((current) => !current)}>
              {showGuidance ? <IconChevronUp /> : <IconChevronDown />}
              {showGuidance ? 'Hide flow' : 'Show flow'}
            </button>
            <button className="designer-guidance-dismiss" onClick={dismissGuidance} title="Dismiss guidance">
              <IconX />
            </button>
          </div>
        </div>
      )}

      {!guidanceDismissed && showGuidance && (
        <div className="designer-flow-strip">
          <div className="designer-flow-card current">
            <div className="designer-flow-icon"><IconSchema /></div>
            <div className="designer-flow-copy">
              <div className="designer-flow-label">1. Model</div>
              <div className="designer-flow-title">Shape tables visually</div>
              <div className="designer-flow-description">Stay in one canvas while columns, indexes, and relationships evolve together.</div>
            </div>
          </div>

          <div className="designer-flow-arrow"><IconArrowRight /></div>

          <button className="designer-flow-card actionable" onClick={onPreviewDiff}>
            <div className="designer-flow-icon"><IconRefresh /></div>
            <div className="designer-flow-copy">
              <div className="designer-flow-label">2. Review</div>
              <div className="designer-flow-title">See the change plan</div>
              <div className="designer-flow-description">
                {diffOperations.length > 0
                  ? `${diffOperations.length} planned change(s) · ${stats.safe} safe / ${stats.unsafe} unsafe`
                  : 'Preview what will change without leaving the builder flow.'}
              </div>
            </div>
          </button>

          <div className="designer-flow-arrow"><IconArrowRight /></div>

          <button className="designer-flow-card actionable" onClick={onGenerateDDL}>
            <div className="designer-flow-icon"><IconCode /></div>
            <div className="designer-flow-copy">
              <div className="designer-flow-label">3. Apply</div>
              <div className="designer-flow-title">Generate SQL when ready</div>
              <div className="designer-flow-description">
                {ddlStatements.length > 0
                  ? `${ddlStatements.length} statement(s) generated · ${executedStatements} executed`
                  : 'Generate SQL only when the visual model already looks right.'}
              </div>
            </div>
          </button>
        </div>
      )}

      <div className="designer-layout">
        <div className="designer-canvas">
          <ERDiagram
            model={model}
            selectedTable={selectedTable}
            onSelectTable={(i) => {
              setSelectedTable(i)
              setSelectedColumn(0)
            }}
            tableCounts={designerTableCounts}
            onAddColumn={(tableName) => {
              const idx = model.tables.findIndex((t) => t.name === tableName)
              if (idx !== -1) {
                setSelectedTable(idx)
                setSelectedColumn(0)
              }
            }}
            onCreateFK={(fromTable, fromCol, toTable, toCol) => {
              const tableIdx = model.tables.findIndex((t) => t.name === fromTable)
              if (tableIdx < 0) return
              const colIdx = model.tables[tableIdx].columns.findIndex((c) => c.name === fromCol)
              if (colIdx < 0) return
              setModel((prev) => {
                const next = clone(prev)
                next.tables[tableIdx].columns[colIdx] = {
                  ...next.tables[tableIdx].columns[colIdx],
                  references: { table: toTable, column: toCol },
                }
                return next
              })
            }}
          />
        </div>

        <div className="designer-editor">
          <ColumnEditor
            model={model}
            setModel={setModel}
            selectedTable={selectedTable}
            selectedColumn={selectedColumn}
            setSelectedColumn={setSelectedColumn}
            activeTable={activeTable}
            activeColumn={activeColumn}
            updateTable={updateTable}
            updateColumn={updateColumn}
          />
          <div className="designer-editor-divider" />
          <IndexEditor
            activeTable={activeTable}
            updateTable={updateTable}
            selectedIndex={selectedIndex}
            setSelectedIndex={setSelectedIndex}
          />
        </div>
      </div>

      <div className="designer-review-grid">
        <section className="designer-review-card">
          <div className="designer-review-header">
            <div>
              <div className="designer-review-kicker">Change Review</div>
              <h3>One place to understand impact</h3>
            </div>
            <button className="ddl-action-btn" onClick={onOpenDiff}>Open full review</button>
          </div>

          <div className={`designer-review-summary ${diffSafe === true ? 'safe' : diffSafe === false ? 'unsafe' : ''}`}>
            {diffSafe === true ? <IconCheckCircle /> : diffSafe === false ? <IconAlertTriangle /> : <IconShield />}
            <span>{diffSummary || 'No diff preview yet. Refresh to compute the change plan.'}</span>
          </div>

          <div className="designer-review-stats">
            <div><strong>{diffOperations.length}</strong><span>changes</span></div>
            <div><strong>{stats.safe}</strong><span>safe</span></div>
            <div><strong>{stats.unsafe}</strong><span>unsafe</span></div>
            <div><strong>{stats.drops}</strong><span>removals</span></div>
          </div>

          <div className="designer-review-list">
            {previewOps.length === 0 && (
              <div className="designer-review-empty">No preview yet. Use Refresh to compute the change plan from the current model.</div>
            )}
            {previewOps.map((op, index) => (
              <div key={`${op.type}-${op.table}-${op.column ?? index}`} className={`designer-review-item ${op.safe ? 'safe' : 'unsafe'}`}>
                <div className="designer-review-item-main">
                  <span className="designer-review-item-type">{op.type.replace(/_/g, ' ')}</span>
                  <span className="designer-review-item-target">{op.table}{op.column ? `.${op.column}` : ''}</span>
                </div>
                <span className={`designer-review-badge ${op.safe ? 'safe' : 'unsafe'}`}>{op.safe ? 'safe' : 'review'}</span>
              </div>
            ))}
          </div>

          {diffWarnings.length > 0 && (
            <div className="designer-review-warning">{diffWarnings[0]}</div>
          )}

          <div className="designer-review-actions">
            <button className="ddl-action-btn" onClick={onPreviewDiff}><IconRefresh /> Refresh</button>
            <button className="ddl-action-btn safe" onClick={onApplySafeDiff} disabled={stats.safe === 0}><IconShield /> Apply Safe</button>
          </div>
        </section>

        <section className="designer-review-card">
          <div className="designer-review-header">
            <div>
              <div className="designer-review-kicker">SQL Plan</div>
              <h3>Generate only when needed</h3>
            </div>
            <button className="ddl-action-btn" onClick={onOpenDDL}>Open SQL details</button>
          </div>

          <div className="designer-review-summary neutral">
            <IconCode />
            <span>
              {ddlStatements.length > 0
                ? `${ddlStatements.length} statement(s) ready · ${executedStatements} executed · ${pendingStatements} pending`
                : 'No generated SQL yet. Create it from the current visual model when you want to inspect or execute it.'}
            </span>
          </div>

          <div className="designer-review-list sql">
            {previewDDL.length === 0 && (
              <div className="designer-review-empty">Generate SQL to inspect the actual execution plan behind the visual model.</div>
            )}
            {previewDDL.map((sql, index) => (
              <div key={`${index}-${sql}`} className="designer-review-item sql">
                <span className="designer-review-item-type">#{index + 1}</span>
                <code className="designer-review-sql">{sqlSnippet(sql)}</code>
              </div>
            ))}
          </div>

          <div className="designer-review-actions">
            <button className="ddl-action-btn primary" onClick={onGenerateDDL}><IconPlay /> Generate SQL</button>
            <button className="ddl-action-btn accent" onClick={onExecuteAll} disabled={pendingStatements === 0}><IconPlay /> Execute Pending</button>
          </div>
        </section>
      </div>
    </div>
  )
}
