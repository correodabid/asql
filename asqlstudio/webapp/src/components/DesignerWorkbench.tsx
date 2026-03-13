import { useEffect, useRef, useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { StatementState } from './DDLPanel'
import { ColumnEditor } from './ColumnEditor'
import { ERDiagram } from './ERDiagram'
import { IconAlertTriangle, IconArrowRight, IconCheckCircle, IconChevronDown, IconChevronUp, IconCode, IconPlay, IconRefresh, IconSchema, IconShield, IconX } from './Icons'
import { IndexEditor } from './IndexEditor'
import { ResizeHandle } from './ResizeHandle'
import { useResizable } from '../hooks/useResizable'
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
  diffSafe: boolean | null
  diffOperations: DiffOperation[]
  onGenerateDDL: () => void
  onSilentDiff: () => Promise<void>
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
  diffSafe,
  diffOperations,
  onGenerateDDL,
  onSilentDiff,
  onPreviewDiff,
  onApplySafeDiff,
  onExecuteAll,
  onOpenDDL,
  onOpenDiff,
}: Props) {
  const stats = summarizeOps(diffOperations)
  const executedStatements = statementStates.filter((state) => state.status === 'success').length
  const pendingStatements = statementStates.filter((state) => state.status === 'pending').length
  const editorResize = useResizable({ key: 'designer-editor', initial: 320, min: 240, max: 640, direction: 'horizontal' })
  const [showGuidance, setShowGuidance] = useState(false)
  const [guidanceDismissed, setGuidanceDismissed] = useState(() => readGuidanceDismissed())
  const [isDiffing, setIsDiffing] = useState(false)
  const isFirstRender = useRef(true)
  const silentDiffRef = useRef(onSilentDiff)
  useEffect(() => { silentDiffRef.current = onSilentDiff }, [onSilentDiff])

  // Auto-diff: debounce 700ms after any model change
  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }
    const timer = setTimeout(async () => {
      setIsDiffing(true)
      try { await silentDiffRef.current() } finally { setIsDiffing(false) }
    }, 700)
    return () => clearTimeout(timer)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model])

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

        <ResizeHandle direction="horizontal" onMouseDown={editorResize.startDragInverse} />

        <div className="designer-editor" style={{ width: editorResize.size }}>
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

      <div className="designer-footer-bar">
        <div className="designer-footer-section">
          <span className={`designer-footer-indicator${diffSafe === true ? ' safe' : diffSafe === false ? ' unsafe' : ''}`}>
            {isDiffing
              ? <span className="designer-footer-diffing" />
              : diffSafe === true ? <IconCheckCircle /> : diffSafe === false ? <IconAlertTriangle /> : <IconShield />}
            {isDiffing
              ? 'Checking…'
              : diffOperations.length > 0
                ? `${diffOperations.length} change${diffOperations.length !== 1 ? 's' : ''} · ${stats.safe} safe · ${stats.unsafe} unsafe`
                : 'No diff'}
          </span>
          <button className="ddl-action-btn" onClick={onApplySafeDiff} disabled={stats.safe === 0 || isDiffing}><IconShield /> Apply safe</button>
          <button className="designer-footer-link" onClick={onOpenDiff}>Full review →</button>
        </div>

        <div className="designer-footer-sep" />

        <div className="designer-footer-section">
          <span className="designer-footer-indicator">
            <IconCode />
            {ddlStatements.length > 0
              ? `${ddlStatements.length} stmt${ddlStatements.length !== 1 ? 's' : ''} · ${executedStatements} executed · ${pendingStatements} pending`
              : 'No SQL'}
          </span>
          <button className="ddl-action-btn" onClick={onGenerateDDL}><IconPlay /> Generate SQL</button>
          <button className="ddl-action-btn" onClick={onExecuteAll} disabled={pendingStatements === 0}><IconPlay /> Execute pending</button>
          <button className="designer-footer-link" onClick={onOpenDDL}>SQL details →</button>
        </div>
      </div>
    </div>
  )
}
