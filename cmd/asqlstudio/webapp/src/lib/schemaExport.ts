import { downloadFile } from './export'

/** True when running inside the Wails desktop webview. */
function isWails(): boolean {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return typeof (window as any)?.go?.main?.App?.SaveTextFile === 'function'
}

/**
 * Export an SVG element as a .svg file with inline styles.
 */
export async function exportSVG(svgElement: SVGSVGElement, filename = 'schema.svg'): Promise<void> {
  const clone = svgElement.cloneNode(true) as SVGSVGElement
  inlineStyles(clone)

  // Set viewBox to content bounds
  const bbox = svgElement.getBBox()
  const pad = 40
  clone.setAttribute('viewBox', `${bbox.x - pad} ${bbox.y - pad} ${bbox.width + pad * 2} ${bbox.height + pad * 2}`)
  clone.setAttribute('width', String(bbox.width + pad * 2))
  clone.setAttribute('height', String(bbox.height + pad * 2))
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')

  // Add white/dark background
  const bgRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect')
  bgRect.setAttribute('x', String(bbox.x - pad))
  bgRect.setAttribute('y', String(bbox.y - pad))
  bgRect.setAttribute('width', String(bbox.width + pad * 2))
  bgRect.setAttribute('height', String(bbox.height + pad * 2))
  bgRect.setAttribute('fill', getComputedStyle(document.documentElement).getPropertyValue('--bg-root').trim() || '#08090e')
  clone.insertBefore(bgRect, clone.firstChild)

  const serializer = new XMLSerializer()
  const svgString = serializer.serializeToString(clone)
  await downloadFile(svgString, filename, 'image/svg+xml')
}

/**
 * Export an SVG element as a .png file at 2x resolution.
 */
export async function exportPNG(svgElement: SVGSVGElement, filename = 'schema.png', scale = 2): Promise<void> {
  const bbox = svgElement.getBBox()
  const pad = 40
  const width = bbox.width + pad * 2
  const height = bbox.height + pad * 2

  const clone = svgElement.cloneNode(true) as SVGSVGElement
  inlineStyles(clone)
  clone.setAttribute('viewBox', `${bbox.x - pad} ${bbox.y - pad} ${width} ${height}`)
  clone.setAttribute('width', String(width))
  clone.setAttribute('height', String(height))
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')

  // Add background
  const bgRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect')
  bgRect.setAttribute('x', String(bbox.x - pad))
  bgRect.setAttribute('y', String(bbox.y - pad))
  bgRect.setAttribute('width', String(width))
  bgRect.setAttribute('height', String(height))
  bgRect.setAttribute('fill', getComputedStyle(document.documentElement).getPropertyValue('--bg-root').trim() || '#08090e')
  clone.insertBefore(bgRect, clone.firstChild)

  const serializer = new XMLSerializer()
  const svgString = serializer.serializeToString(clone)
  const blob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(blob)

  const img = new Image()
  img.onload = () => {
    const canvas = document.createElement('canvas')
    canvas.width = width * scale
    canvas.height = height * scale
    const ctx = canvas.getContext('2d')!
    ctx.scale(scale, scale)
    ctx.drawImage(img, 0, 0)
    URL.revokeObjectURL(url)

    canvas.toBlob((pngBlob) => {
      if (!pngBlob) return
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if (isWails()) {
        // In Wails, use native save dialog via Go IPC
        const reader = new FileReader()
        reader.onload = () => {
          ;(window as any).go.main.App.SaveBinaryFile(filename, reader.result as string)
        }
        reader.readAsDataURL(pngBlob)
        return
      }
      // Browser fallback
      const a = document.createElement('a')
      a.href = URL.createObjectURL(pngBlob)
      a.download = filename
      a.click()
      URL.revokeObjectURL(a.href)
    }, 'image/png')
  }
  img.src = url
}

/**
 * Inline computed styles on all elements within an SVG so it renders
 * correctly when extracted from the DOM.
 */
function inlineStyles(el: Element): void {
  const computed = getComputedStyle(el)
  const important = [
    'fill', 'stroke', 'stroke-width', 'stroke-dasharray', 'stroke-linecap',
    'stroke-linejoin', 'opacity', 'font-family', 'font-size', 'font-weight',
    'text-anchor', 'dominant-baseline', 'color',
  ]

  for (const prop of important) {
    const val = computed.getPropertyValue(prop)
    if (val) {
      (el as SVGElement).style?.setProperty(prop, val)
    }
  }

  for (const child of el.children) {
    inlineStyles(child)
  }
}
