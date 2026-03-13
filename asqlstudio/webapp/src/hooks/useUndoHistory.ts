import { useCallback, useReducer } from 'react'
import type { Reducer } from 'react'

// ─── Types ────────────────────────────────────────────────

type HistoryState<T> = {
  past: T[]
  present: T
  future: T[]
}

type HistoryAction<T> =
  | { type: 'push'; value: T | ((prev: T) => T) }
  | { type: 'undo' }
  | { type: 'redo' }
  | { type: 'reset'; value: T }

// ─── Reducer (generic, cast at call-site) ─────────────────

const MAX_HISTORY = 80

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function historyReducer<T>(state: HistoryState<T>, action: HistoryAction<T>): HistoryState<T> {
  switch (action.type) {
    case 'push': {
      const next =
        typeof action.value === 'function'
          ? (action.value as (prev: T) => T)(state.present)
          : action.value
      return {
        past: [...state.past, state.present].slice(-MAX_HISTORY),
        present: next,
        future: [],
      }
    }
    case 'undo': {
      if (state.past.length === 0) return state
      return {
        past: state.past.slice(0, -1),
        present: state.past[state.past.length - 1],
        future: [state.present, ...state.future],
      }
    }
    case 'redo': {
      if (state.future.length === 0) return state
      return {
        past: [...state.past, state.present],
        present: state.future[0],
        future: state.future.slice(1),
      }
    }
    case 'reset':
      return { past: [], present: action.value, future: [] }
  }
}

// ─── Hook ─────────────────────────────────────────────────

/**
 * Wraps a stateful value with an undo/redo history stack (max 80 entries).
 *
 * - `setValue`  – sets a new value and pushes the old one onto the undo stack
 *                 (supports both direct values and updater functions, just like useState)
 * - `undo`      – move backward one step
 * - `redo`      – move forward one step
 * - `reset`     – replace the value and clear the entire history (use for loads/baseline switches)
 */
export function useUndoHistory<T>(initial: T) {
  const [state, dispatch] = useReducer(
    historyReducer as Reducer<HistoryState<T>, HistoryAction<T>>,
    { past: [], present: initial, future: [] },
  )

  const setValue = useCallback((value: T | ((prev: T) => T)) => {
    dispatch({ type: 'push', value })
  }, [])

  const undo = useCallback(() => dispatch({ type: 'undo' }), [])
  const redo = useCallback(() => dispatch({ type: 'redo' }), [])
  const reset = useCallback((value: T) => dispatch({ type: 'reset', value }), [])

  return {
    value: state.present,
    setValue,
    undo,
    redo,
    canUndo: state.past.length > 0,
    canRedo: state.future.length > 0,
    reset,
  }
}
