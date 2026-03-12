type Props = {
  direction: 'horizontal' | 'vertical'
  onMouseDown: (e: React.MouseEvent) => void
}

export function ResizeHandle({ direction, onMouseDown }: Props) {
  return (
    <div
      className={`resize-bar resize-bar-${direction}`}
      onMouseDown={onMouseDown}
    />
  )
}
