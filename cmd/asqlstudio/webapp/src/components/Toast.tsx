import type { ToastItem } from '../hooks/useToast'

type Props = {
  toasts: ToastItem[]
  onDismiss: (id: number) => void
}

export function ToastContainer({ toasts, onDismiss }: Props) {
  if (toasts.length === 0) return null

  return (
    <div className="toast-container">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={`toast toast-${toast.type}`}
          onClick={() => onDismiss(toast.id)}
        >
          <span className="toast-icon">
            {toast.type === 'success' ? '\u2713' : toast.type === 'error' ? '!' : 'i'}
          </span>
          <span className="toast-message">{toast.message}</span>
        </div>
      ))}
    </div>
  )
}
