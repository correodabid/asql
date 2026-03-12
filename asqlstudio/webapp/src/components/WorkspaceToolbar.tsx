import type { TxState } from '../types/workspace'
import { IconCheck, IconCode, IconFormat, IconHistory, IconKey, IconLayers, IconPlay, IconRefresh, IconUpload } from './Icons'

type Props = {
  loading: boolean
  sql: string
  txState: TxState | null
  showHistory: boolean
  onRun: () => void
  onFormat: () => void
  onBegin: () => void
  onCommit: () => void
  onRollback: () => void
  onToggleHistory: () => void
  onImport: () => void
  onSaved: () => void
  onBuilder: () => void
}

export function WorkspaceToolbar({
  loading,
  sql,
  txState,
  showHistory,
  onRun,
  onFormat,
  onBegin,
  onCommit,
  onRollback,
  onToggleHistory,
  onImport,
  onSaved,
  onBuilder,
}: Props) {
  return (
    <div className="ws-toolbar">
      <div className="toolbar-group">
        <button
          className="toolbar-btn primary"
          onClick={onRun}
          disabled={loading || !sql.trim()}
          title="Execute (Cmd+Enter)"
        >
          <IconPlay /> Run
        </button>
        <button
          className="toolbar-btn"
          onClick={onFormat}
          disabled={!sql.trim()}
          title="Format SQL (Shift+Alt+F)"
        >
          <IconFormat /> Format
        </button>
        <button
          className="toolbar-btn"
          onClick={onImport}
          title="Import CSV/JSON"
        >
          <IconUpload /> Import
        </button>
        <button
          className="toolbar-btn"
          onClick={onSaved}
          title="Saved Queries"
        >
          <IconCode /> Saved
        </button>
        <button
          className="toolbar-btn"
          onClick={onBuilder}
          title="Visual Query Builder"
        >
          <IconLayers /> Builder
        </button>
      </div>

      <div className="toolbar-separator" />

      <div className="toolbar-group">
        {!txState ? (
          <button className="toolbar-btn accent" onClick={onBegin} disabled={loading}>
            <IconKey /> BEGIN
          </button>
        ) : (
          <>
            <span className="tx-badge">TX: {txState.txId.slice(0, 8)}</span>
            <button className="toolbar-btn safe" onClick={onCommit} disabled={loading}>
              <IconCheck /> COMMIT
            </button>
            <button
              className="toolbar-btn"
              onClick={onRollback}
              disabled={loading}
              style={{ borderColor: 'rgba(248,113,113,0.3)', color: 'var(--text-unsafe)' }}
            >
              <IconRefresh /> ROLLBACK
            </button>
          </>
        )}
      </div>

      <div className="toolbar-separator" />

      <div className="toolbar-group">
        <button
          className={`toolbar-btn ${showHistory ? 'active' : ''}`}
          onClick={onToggleHistory}
          title="Query History"
        >
          <IconHistory /> History
        </button>
      </div>
    </div>
  )
}
