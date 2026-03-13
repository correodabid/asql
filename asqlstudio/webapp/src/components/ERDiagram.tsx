import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import type { EntityDefinition, MultiDomainModel, SchemaColumn, SchemaModel, SchemaTable } from '../schema'
import { useERDiagram } from '../hooks/useERDiagram'
import { ERMinimap } from './ERMinimap'
import { IconDownload, IconGrid, IconImage, IconMaximize, IconMinus, IconPlus, IconRedo, IconSearch, IconUndo } from './Icons'
import { exportSVG, exportPNG } from '../lib/schemaExport'
import { downloadFile } from '../lib/export'

type Props = {
  model: SchemaModel
  selectedTable: number
  onSelectTable: (index: number) => void
  multiModel?: MultiDomainModel
  onDomainClick?: (domain: string) => void
  tableCounts?: Record<string, number>
  /** Mutation counts per table for WAL heat overlay (falls back to tableCounts if omitted) */
  walMutationCounts?: Record<string, number>
  /** Called when user requests adding a column to a table from the canvas */
  onAddColumn?: (tableName: string) => void
  /** Called when user drag-creates an FK from canvas */
  onCreateFK?: (fromTable: string, fromCol: string, toTable: string, toCol: string) => void
  /** Called when user clicks a column row — passes model table index and column index */
  onSelectColumn?: (tableIndex: number, colIndex: number) => void
  /** Called when user clicks an index row — passes model table index and index index */
  onSelectIndex?: (tableIndex: number, idxIndex: number) => void
  /** Called when the user wants to add a new table from the canvas toolbar */
  onAddTable?: () => void
  /** Called when the user wants to delete a table via context menu (name = table to remove) */
  onDeleteTable?: (tableName: string) => void
  /** Undo the last model change */
  onUndo?: () => void
  /** Redo the last undone model change */
  onRedo?: () => void
  canUndo?: boolean
  canRedo?: boolean
}

const TABLE_W = 220
const TABLE_HEADER_H = 36
const COL_ROW_H = 26
const IDX_ROW_H = 20
const IDX_HEADER_H = 22
const PADDING = 40
const GAP = 60

const DOMAIN_COLORS = [
  '#635bff', // indigo
  '#10b981', // emerald
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // violet
  '#06b6d4', // cyan
  '#ec4899', // pink
  '#84cc16', // lime
]

const ENTITY_COLORS = [
  '#22d3ee', // cyan
  '#a78bfa', // violet
  '#f472b6', // pink
  '#34d399', // emerald
  '#fbbf24', // amber
  '#fb923c', // orange
  '#818cf8', // indigo
  '#4ade80', // green
]

const DOMAIN_GROUP_PADDING = 30
const DOMAIN_GROUP_GAP = 80
const DOMAIN_LABEL_H = 36

const ENTITY_GROUP_PADDING = 24
const ENTITY_LABEL_H = 28

function indexSectionHeight(table: SchemaTable) {
  const indexCount = table.indexes?.length ?? 0
  if (indexCount === 0) return 0
  return IDX_HEADER_H + indexCount * IDX_ROW_H
}

function tableHeight(table: SchemaTable) {
  return TABLE_HEADER_H + table.columns.length * COL_ROW_H + 8 + indexSectionHeight(table)
}

type TablePos = { x: number; y: number; w: number; h: number; table: SchemaTable; index: number; domainColor?: string; tableKey?: string }

type Point = { x: number; y: number }

// ─── Entity-aware layout ─────────────────────────────────

function layoutWithEntities(tables: SchemaTable[], entities: EntityDefinition[]): TablePos[] {
  if (entities.length === 0) return layoutTables(tables)

  // Build entity membership
  const tableToEntity = new Map<string, EntityDefinition>()
  for (const entity of entities) {
    for (const t of entity.tables) {
      tableToEntity.set(t, entity)
    }
  }

  // Partition: entity tables (grouped) vs loose tables
  const entityClusters = new Map<string, { entity: EntityDefinition; tables: SchemaTable[] }>()
  const looseTables: SchemaTable[] = []

  for (const table of tables) {
    const entity = tableToEntity.get(table.name)
    if (entity) {
      let cluster = entityClusters.get(entity.name)
      if (!cluster) {
        cluster = { entity, tables: [] }
        entityClusters.set(entity.name, cluster)
      }
      cluster.tables.push(table)
    } else {
      looseTables.push(table)
    }
  }

  // Sort entity tables: root first, then alphabetical
  for (const cluster of entityClusters.values()) {
    cluster.tables.sort((a, b) => {
      if (a.name === cluster.entity.root_table) return -1
      if (b.name === cluster.entity.root_table) return 1
      return a.name.localeCompare(b.name)
    })
  }

  const positions: TablePos[] = []
  let globalIndex = 0

  // Layout each entity cluster as a vertical stack
  const clusterBoxes: { x: number; y: number; w: number; h: number }[] = []
  const clusterCols = Math.max(1, Math.ceil(Math.sqrt(entityClusters.size + (looseTables.length > 0 ? 1 : 0))))

  let cx = PADDING
  let cy = PADDING
  let rowMaxH = 0
  let colIdx = 0

  for (const cluster of entityClusters.values()) {
    let ty = cy + ENTITY_LABEL_H + ENTITY_GROUP_PADDING
    let maxW = 0

    for (const table of cluster.tables) {
      const h = tableHeight(table)
      positions.push({ x: cx + ENTITY_GROUP_PADDING, y: ty, w: TABLE_W, h, table, index: globalIndex++ })
      maxW = Math.max(maxW, TABLE_W)
      ty += h + 20
    }

    const clusterW = maxW + ENTITY_GROUP_PADDING * 2
    const clusterH = ty - cy
    clusterBoxes.push({ x: cx, y: cy, w: clusterW, h: clusterH })

    rowMaxH = Math.max(rowMaxH, clusterH)
    colIdx++

    if (colIdx >= clusterCols) {
      cx = PADDING
      cy += rowMaxH + GAP
      rowMaxH = 0
      colIdx = 0
    } else {
      cx += clusterW + GAP
    }
  }

  // Layout loose tables in remaining space
  if (looseTables.length > 0) {
    const looseCols = Math.max(1, Math.ceil(Math.sqrt(looseTables.length)))
    let lx = cx
    let ly = cy
    let lMaxH = 0

    looseTables.forEach((table, li) => {
      const h = tableHeight(table)
      if (li > 0 && li % looseCols === 0) {
        lx = cx
        ly += lMaxH + GAP
        lMaxH = 0
      }
      positions.push({ x: lx, y: ly, w: TABLE_W, h, table, index: globalIndex++ })
      lMaxH = Math.max(lMaxH, h)
      lx += TABLE_W + GAP
    })
  }

  return positions
}

function layoutTables(tables: SchemaTable[]): TablePos[] {
  const positions: TablePos[] = []
  const cols = Math.max(1, Math.ceil(Math.sqrt(tables.length)))
  let x = PADDING
  let y = PADDING
  let maxRowH = 0

  tables.forEach((table, index) => {
    const h = tableHeight(table)
    if (index > 0 && index % cols === 0) {
      x = PADDING
      y += maxRowH + GAP
      maxRowH = 0
    }
    positions.push({ x, y, w: TABLE_W, h, table, index })
    maxRowH = Math.max(maxRowH, h)
    x += TABLE_W + GAP
  })

  return positions
}

function computeTablePositions(
  tables: SchemaTable[],
  savedPositions: Record<string, Point>,
  entities?: EntityDefinition[],
): TablePos[] {
  const autoLayout = entities && entities.length > 0
    ? layoutWithEntities(tables, entities)
    : layoutTables(tables)
  return autoLayout.map((pos) => {
    const saved = savedPositions[pos.table.name]
    if (saved) {
      return { ...pos, x: saved.x, y: saved.y }
    }
    return pos
  })
}

type Rel = { from: TablePos; fromCol: string; to: TablePos; toCol: string; crossDomain?: boolean; entityColor?: string }

function findRelationships(positions: TablePos[], entities?: EntityDefinition[]): Rel[] {
  const nameMap = new Map<string, TablePos>()
  positions.forEach((p) => nameMap.set(p.table.name, p))

  // Build entity membership for coloring
  const tableToEntityIdx = new Map<string, number>()
  if (entities) {
    entities.forEach((entity, ei) => {
      for (const t of entity.tables) {
        tableToEntityIdx.set(t, ei)
      }
    })
  }

  const rels: Rel[] = []
  for (const pos of positions) {
    for (const col of pos.table.columns) {
      if (col.references?.table) {
        const target = nameMap.get(col.references.table)
        if (target) {
          // Check if both are in the same entity
          let entityColor: string | undefined
          const fromEi = tableToEntityIdx.get(pos.table.name)
          const toEi = tableToEntityIdx.get(target.table.name)
          if (fromEi !== undefined && fromEi === toEi) {
            entityColor = ENTITY_COLORS[fromEi % ENTITY_COLORS.length]
          }
          rels.push({ from: pos, fromCol: col.name, to: target, toCol: col.references.column, entityColor })
        }
      }
    }
  }
  return rels
}

// ─── Entity group computation ─────────────────────────────

type EntityGroup = {
  name: string
  rootTable: string
  tables: string[]
  color: string
  x: number
  y: number
  w: number
  h: number
}

function computeEntityGroups(
  entities: EntityDefinition[],
  tablePositions: TablePos[],
): EntityGroup[] {
  const posMap = new Map<string, TablePos>()
  tablePositions.forEach((p) => {
    const key = p.tableKey || p.table.name
    posMap.set(key, p)
    // Also map by plain name for single-domain mode
    if (p.tableKey && !posMap.has(p.table.name)) {
      posMap.set(p.table.name, p)
    }
  })

  const groups: EntityGroup[] = []
  entities.forEach((entity, ei) => {
    const memberPositions: TablePos[] = []
    for (const tableName of entity.tables) {
      const pos = posMap.get(tableName)
      if (pos) {
        memberPositions.push(pos)
      }
    }

    if (memberPositions.length === 0) return

    const minX = Math.min(...memberPositions.map(p => p.x)) - ENTITY_GROUP_PADDING
    const minY = Math.min(...memberPositions.map(p => p.y)) - ENTITY_LABEL_H - ENTITY_GROUP_PADDING
    const maxX = Math.max(...memberPositions.map(p => p.x + p.w)) + ENTITY_GROUP_PADDING
    const maxY = Math.max(...memberPositions.map(p => p.y + p.h)) + ENTITY_GROUP_PADDING

    groups.push({
      name: entity.name,
      rootTable: entity.root_table,
      tables: entity.tables,
      color: ENTITY_COLORS[ei % ENTITY_COLORS.length],
      x: minX,
      y: minY,
      w: maxX - minX,
      h: maxY - minY,
    })
  })

  return groups
}

// ─── Multi-domain layout ──────────────────────────────────

type DomainGroup = {
  domain: string
  color: string
  x: number
  y: number
  w: number
  h: number
}

function computeMultiDomainLayout(
  multiModel: MultiDomainModel,
  savedPositions: Record<string, Point>,
): { groups: DomainGroup[]; allPositions: TablePos[]; rels: Rel[] } {
  const groups: DomainGroup[] = []
  const allPositions: TablePos[] = []
  let globalIndex = 0

  // First pass: compute each domain's auto layout to determine bounding sizes
  const domainLayouts: { domain: string; color: string; autoPositions: TablePos[]; entities?: EntityDefinition[] }[] = []
  for (let di = 0; di < multiModel.domains.length; di++) {
    const domainModel = multiModel.domains[di]
    const color = DOMAIN_COLORS[di % DOMAIN_COLORS.length]
    const entities = domainModel.entities
    const autoLayout = entities && entities.length > 0
      ? layoutWithEntities(domainModel.tables, entities)
      : layoutTables(domainModel.tables)
    domainLayouts.push({ domain: domainModel.domain, color, autoPositions: autoLayout, entities })
  }

  // Arrange domain groups in a grid layout
  const domainCols = Math.max(1, Math.ceil(Math.sqrt(domainLayouts.length)))
  let groupX = PADDING
  let groupY = PADDING
  let rowMaxH = 0
  let colIndex = 0

  for (let di = 0; di < domainLayouts.length; di++) {
    const { domain, color, autoPositions: localLayout } = domainLayouts[di]

    if (localLayout.length === 0) continue

    // Determine the bounding box of auto-layout
    const autoMaxX = Math.max(...localLayout.map(p => p.x + p.w))
    const autoMaxY = Math.max(...localLayout.map(p => p.y + p.h))
    const domainContentW = autoMaxX
    const domainContentH = autoMaxY

    // Apply saved positions or offset each table
    const positioned = localLayout.map((pos) => {
      const key = `${domain}:${pos.table.name}`
      const saved = savedPositions[key]
      return {
        ...pos,
        x: (saved?.x ?? (pos.x + groupX)),
        y: (saved?.y ?? (pos.y + groupY + DOMAIN_LABEL_H)),
        index: globalIndex++,
        domainColor: color,
        tableKey: key,
      }
    })

    // Compute actual bounding box from positioned tables
    const minX = Math.min(...positioned.map(p => p.x))
    const minY = Math.min(...positioned.map(p => p.y))
    const maxX = Math.max(...positioned.map(p => p.x + p.w))
    const maxY = Math.max(...positioned.map(p => p.y + p.h))

    const gx = minX - DOMAIN_GROUP_PADDING
    const gy = minY - DOMAIN_LABEL_H - DOMAIN_GROUP_PADDING
    const gw = (maxX - minX) + DOMAIN_GROUP_PADDING * 2
    const gh = (maxY - minY) + DOMAIN_LABEL_H + DOMAIN_GROUP_PADDING * 2

    groups.push({ domain, color, x: gx, y: gy, w: gw, h: gh })
    allPositions.push(...positioned)

    // Advance grid position
    const usedW = domainContentW + DOMAIN_GROUP_PADDING * 2
    const usedH = domainContentH + DOMAIN_LABEL_H + DOMAIN_GROUP_PADDING * 2
    rowMaxH = Math.max(rowMaxH, usedH)
    colIndex++

    if (colIndex >= domainCols) {
      groupX = PADDING
      groupY += rowMaxH + DOMAIN_GROUP_GAP
      rowMaxH = 0
      colIndex = 0
    } else {
      groupX += usedW + DOMAIN_GROUP_GAP
    }
  }

  // Find relationships across all tables
  const keyMap = new Map<string, TablePos>()
  const nameMap = new Map<string, TablePos[]>()
  allPositions.forEach((p) => {
    if (p.tableKey) keyMap.set(p.tableKey, p)
    const list = nameMap.get(p.table.name) || []
    list.push(p)
    nameMap.set(p.table.name, list)
  })

  // Build entity membership across all domains for FK coloring
  const tableKeyToEntityIdx = new Map<string, number>()
  let entityOffset = 0
  for (const dl of domainLayouts) {
    if (dl.entities) {
      dl.entities.forEach((entity, ei) => {
        for (const t of entity.tables) {
          tableKeyToEntityIdx.set(`${dl.domain}:${t}`, entityOffset + ei)
        }
      })
      entityOffset += dl.entities.length
    }
  }

  const rels: Rel[] = []

  // Intra-domain FK relationships
  for (const pos of allPositions) {
    for (const col of pos.table.columns) {
      if (col.references?.table && pos.tableKey) {
        const domainName = pos.tableKey.split(':')[0]
        const targetKey = `${domainName}:${col.references.table}`
        const target = keyMap.get(targetKey)
        if (target) {
          let entityColor: string | undefined
          const fromEi = tableKeyToEntityIdx.get(pos.tableKey)
          const toEi = tableKeyToEntityIdx.get(targetKey)
          if (fromEi !== undefined && fromEi === toEi) {
            entityColor = ENTITY_COLORS[fromEi % ENTITY_COLORS.length]
          }
          rels.push({ from: pos, fromCol: col.name, to: target, toCol: col.references.column, entityColor })
        }
      }
    }
  }

  // Cross-domain VFK relationships
  for (const domainModel of multiModel.domains) {
    for (const table of domainModel.tables) {
      if (!table.versioned_foreign_keys) continue
      for (const vfk of table.versioned_foreign_keys) {
        const fromKey = `${domainModel.domain}:${table.name}`
        const toKey = `${vfk.references_domain}:${vfk.references_table}`
        const from = keyMap.get(fromKey)
        const to = keyMap.get(toKey)
        if (from && to) {
          rels.push({
            from,
            fromCol: vfk.column,
            to,
            toCol: vfk.references_column,
            crossDomain: true,
          })
        }
      }
    }
  }

  return { groups, allPositions, rels }
}

// ─── Collect all entities from multi-domain model ─────────

function collectAllEntities(multiModel: MultiDomainModel, _allPositions: TablePos[]): EntityDefinition[] {
  const all: EntityDefinition[] = []
  for (const domain of multiModel.domains) {
    if (domain.entities) {
      for (const entity of domain.entities) {
        // Qualify table names with domain prefix for multi-domain positions
        const qualifiedTables = entity.tables.map(t => `${domain.domain}:${t}`)
        all.push({
          name: entity.name,
          root_table: `${domain.domain}:${entity.root_table}`,
          tables: qualifiedTables,
        })
      }
    }
  }
  return all
}

function computeMultiDomainEntityGroups(
  multiModel: MultiDomainModel,
  allPositions: TablePos[],
): EntityGroup[] {
  const entities = collectAllEntities(multiModel, allPositions)
  const posMap = new Map<string, TablePos>()
  allPositions.forEach((p) => {
    if (p.tableKey) posMap.set(p.tableKey, p)
  })

  const groups: EntityGroup[] = []
  entities.forEach((entity, ei) => {
    const memberPositions: TablePos[] = []
    for (const qualifiedName of entity.tables) {
      const pos = posMap.get(qualifiedName)
      if (pos) memberPositions.push(pos)
    }
    if (memberPositions.length === 0) return

    const minX = Math.min(...memberPositions.map(p => p.x)) - ENTITY_GROUP_PADDING
    const minY = Math.min(...memberPositions.map(p => p.y)) - ENTITY_LABEL_H - ENTITY_GROUP_PADDING
    const maxX = Math.max(...memberPositions.map(p => p.x + p.w)) + ENTITY_GROUP_PADDING
    const maxY = Math.max(...memberPositions.map(p => p.y + p.h)) + ENTITY_GROUP_PADDING

    groups.push({
      name: entity.name,
      rootTable: entity.root_table,
      tables: entity.tables,
      color: ENTITY_COLORS[ei % ENTITY_COLORS.length],
      x: minX,
      y: minY,
      w: maxX - minX,
      h: maxY - minY,
    })
  })
  return groups
}

// ─── Relationship path ────────────────────────────────────

/**
 * Truncate `text` to fit within `maxPx` using an approximate per-character
 * pixel width. Appends '\u2026' (ellipsis) when truncated.
 * Good enough for monospace (JetBrains Mono) and Inter at typical canvas sizes.
 */
function truncText(text: string, maxPx: number, charPx: number): string {
  const maxChars = Math.floor(maxPx / charPx)
  if (text.length <= maxChars) return text
  return text.slice(0, Math.max(1, maxChars - 1)) + '\u2026'
}

function colY(pos: TablePos, colName: string) {
  const idx = pos.table.columns.findIndex((c) => c.name === colName)
  return pos.y + TABLE_HEADER_H + (idx >= 0 ? idx : 0) * COL_ROW_H + COL_ROW_H / 2
}

function RelationshipPath({ rel, isHovered, onHover, index, dimmed, highlightedColKey }: {
  rel: Rel
  isHovered: boolean
  onHover: (index: number | null) => void
  index: number
  dimmed?: boolean
  /** "tableKey:colName" — when set, brightens any rel whose endpoint column matches */
  highlightedColKey?: string | null
}) {
  const fromRight = rel.from.x + rel.from.w
  const toLeft = rel.to.x
  const fy = colY(rel.from, rel.fromCol)
  const ty = colY(rel.to, rel.toCol)

  const fromX = fromRight < toLeft ? fromRight : rel.from.x
  const toX = fromRight < toLeft ? toLeft : rel.to.x + rel.to.w

  const midX = (fromX + toX) / 2
  const pathD = `M ${fromX} ${fy} C ${midX} ${fy}, ${midX} ${ty}, ${toX} ${ty}`

  const isCross = rel.crossDomain
  const hasEntityColor = !!rel.entityColor
  const strokeColor = isCross ? '#f59e0b' : (hasEntityColor ? rel.entityColor! : 'var(--accent)')

  // Column-level highlight: glow when either end column is hovered
  const fromColKey = `${rel.from.tableKey || rel.from.table.name}:${rel.fromCol}`
  const toColKey   = `${rel.to.tableKey   || rel.to.table.name}:${rel.toCol}`
  const isColHL = highlightedColKey != null && (highlightedColKey === fromColKey || highlightedColKey === toColKey)

  const eh = isHovered || isColHL
  const hlColor = isColHL ? '#22d3ee' : strokeColor

  return (
    <g
      onMouseEnter={() => onHover(index)}
      onMouseLeave={() => onHover(null)}
      opacity={dimmed ? 0.1 : 1}
      style={{ transition: 'opacity 200ms' }}
    >
      {/* Invisible hit area for easier hover targeting */}
      <path
        d={pathD}
        fill="none"
        stroke="transparent"
        strokeWidth={14}
        style={{ cursor: 'pointer' }}
      />

      {/* Visible line */}
      <path
        d={pathD}
        fill="none"
        stroke={hlColor}
        strokeWidth={eh ? (isColHL ? 3 : 2.5) : (hasEntityColor ? 2 : 1.5)}
        strokeDasharray={isCross ? (eh ? '8 4' : '4 4') : (hasEntityColor ? 'none' : (eh ? 'none' : '6 3'))}
        opacity={eh ? 1 : (hasEntityColor ? 0.7 : 0.5)}
        markerEnd={eh ? 'url(#arrowhead-hover)' : 'url(#arrowhead)'}
        style={{ transition: 'opacity 150ms, stroke-width 150ms' }}
      />

      {/* Connection dot at FK source */}
      <circle cx={fromX} cy={fy} r={isColHL ? 4 : 3} fill={hlColor} opacity={eh ? 0.9 : 0.4} />

      {/* Connection dot at PK target */}
      <circle cx={toX} cy={ty} r={isColHL ? 4 : 3} fill={hlColor} opacity={eh ? 0.9 : 0.4} />

      {/* Cardinality labels on hover */}
      {eh && (
        <>
          <text
            x={fromX + (fromRight < toLeft ? -14 : 14)}
            y={fy - 10}
            fontSize="10"
            fontWeight="600"
            fill={isCross ? '#f59e0b' : (isColHL ? '#22d3ee' : (hasEntityColor ? strokeColor : 'var(--text-accent)'))}
            textAnchor="middle"
            fontFamily="Inter, sans-serif"
          >
            {isCross ? 'VFK' : 'N'}
          </text>
          <text
            x={toX + (fromRight < toLeft ? 14 : -14)}
            y={ty - 10}
            fontSize="10"
            fontWeight="600"
            fill={isCross ? '#f59e0b' : (isColHL ? '#22d3ee' : (hasEntityColor ? strokeColor : 'var(--text-accent)'))}
            textAnchor="middle"
            fontFamily="Inter, sans-serif"
          >
            1
          </text>
        </>
      )}
    </g>
  )
}

// ─── Entity contour SVG component ─────────────────────────

function EntityContour({ group, isHovered, onHover }: {
  group: EntityGroup
  isHovered: boolean
  onHover: (name: string | null) => void
}) {
  return (
    <g
      onMouseEnter={() => onHover(group.name)}
      onMouseLeave={() => onHover(null)}
      style={{ cursor: 'default' }}
    >
      {/* Contour */}
      <rect
        x={group.x}
        y={group.y}
        width={group.w}
        height={group.h}
        rx={16}
        fill={group.color + '08'}
        stroke={group.color}
        strokeWidth={isHovered ? 2 : 1.5}
        strokeOpacity={isHovered ? 0.6 : 0.25}
        filter={isHovered ? 'url(#entity-glow)' : undefined}
        style={{ transition: 'stroke-opacity 200ms, stroke-width 200ms' }}
      />

      {/* Top accent bar */}
      <rect
        x={group.x + 16}
        y={group.y}
        width={Math.min(group.w - 32, 60)}
        height={4}
        rx={2}
        fill={group.color}
        opacity={isHovered ? 0.8 : 0.5}
      />

      {/* Entity label */}
      <g>
        {/* Aggregate icon (stacked rectangles) */}
        <rect
          x={group.x + 14}
          y={group.y + 12}
          width={10}
          height={7}
          rx={1.5}
          fill="none"
          stroke={group.color}
          strokeWidth={1.2}
          opacity={0.7}
        />
        <rect
          x={group.x + 17}
          y={group.y + 9}
          width={10}
          height={7}
          rx={1.5}
          fill="none"
          stroke={group.color}
          strokeWidth={1.2}
          opacity={0.7}
        />

        {/* Entity name */}
        <text
          x={group.x + 32}
          y={group.y + 18}
          fontSize="10"
          fontWeight="700"
          fill={group.color}
          fontFamily="Inter, sans-serif"
          letterSpacing="0.08em"
          opacity={isHovered ? 1 : 0.8}
        >
          {group.name.toUpperCase()}
        </text>

        {/* Table count badge */}
        <text
          x={group.x + 34 + group.name.length * 7}
          y={group.y + 18}
          fontSize="9"
          fill={group.color}
          fontFamily="Inter, sans-serif"
          opacity={0.5}
        >
          ({group.tables.length})
        </text>
      </g>
    </g>
  )
}

// ─── Table card rendering ─────────────────────────────────

function TableCard({
  pos, isActive, isBeingDragged, onMouseDown, rootEntityColor, dimmed, rowCount,
  walHeatBg, highlightedColumns, fkHighlightedColumns, onColumnHover, onTableContextMenu,
  annotation, onAnnotationDblClick, onAddColumnClick, onFKDragStart,
  onSelectColumn, onSelectIndex,
}: {
  pos: TablePos
  isActive: boolean
  isBeingDragged: boolean
  onMouseDown: (e: React.MouseEvent) => void
  rootEntityColor?: string
  dimmed?: boolean
  rowCount?: number
  /** Background colour for WAL heat overlay on the table header */
  walHeatBg?: string
  /** Column names whose rows should be highlighted (search match) */
  highlightedColumns?: Set<string>
  /** Column names to highlight as FK relationship endpoints (on rel hover) */
  fkHighlightedColumns?: Set<string>
  /** Fired on column row hover — null colName = mouse left */
  onColumnHover?: (colName: string | null, cx: number, cy: number) => void
  /** Fired on right-click of the table card */
  onTableContextMenu?: (cx: number, cy: number) => void
  /** Existing table-level annotation */
  annotation?: string
  /** Fired on double-click of the table header */
  onAnnotationDblClick?: () => void
  /** Fired when user clicks the + column button in the header (canvas inline add) */
  onAddColumnClick?: () => void
  /** Fired when user starts dragging an FK handle from a column */
  onFKDragStart?: (colName: string, cx: number, cy: number) => void
  /** Fired when user clicks a column row to select it */
  onSelectColumn?: (colIndex: number) => void
  /** Fired when user clicks an index row to select it */
  onSelectIndex?: (idxIndex: number) => void
}) {
  const accentColor = pos.domainColor
  const headerFill = walHeatBg || (isActive ? 'var(--accent-subtle)' : (accentColor ? accentColor + '18' : 'rgba(255,255,255,0.03)'))

  return (
    <g
      className={`er-table-group ${isActive ? 'active' : ''} ${isBeingDragged ? 'dragging' : ''}`}
      onMouseDown={onMouseDown}
      onContextMenu={(e) => {
        if (onTableContextMenu) {
          e.preventDefault()
          e.stopPropagation()
          onTableContextMenu(e.clientX, e.clientY)
        }
      }}
      style={{ cursor: isBeingDragged ? 'grabbing' : 'grab', opacity: dimmed ? 0.2 : 1, transition: 'opacity 200ms' }}
    >
      {/* Shadow */}
      <rect
        x={pos.x + (isBeingDragged ? 4 : 2)}
        y={pos.y + (isBeingDragged ? 4 : 2)}
        width={pos.w}
        height={pos.h}
        rx={8}
        fill={isBeingDragged ? 'rgba(0,0,0,0.5)' : 'rgba(0,0,0,0.3)'}
      />

      {/* Card body */}
      <rect
        x={pos.x}
        y={pos.y}
        width={pos.w}
        height={pos.h}
        rx={8}
        fill="var(--bg-elevated)"
        stroke={isActive ? 'var(--accent)' : (accentColor || 'var(--border-strong)')}
        strokeWidth={isActive ? 2 : 1}
        strokeOpacity={isActive ? 1 : (accentColor ? 0.5 : 1)}
      />

      {/* Header bg */}
      <rect
        x={pos.x}
        y={pos.y}
        width={pos.w}
        height={TABLE_HEADER_H}
        rx={8}
        fill={headerFill}
        onDoubleClick={onAnnotationDblClick}
        style={onAnnotationDblClick ? { cursor: 'text' } : undefined}
      />
      {/* Square-off bottom header corners */}
      <rect
        x={pos.x}
        y={pos.y + TABLE_HEADER_H - 8}
        width={pos.w}
        height={8}
        fill={headerFill}
      />

      {/* Header divider */}
      <line
        x1={pos.x}
        y1={pos.y + TABLE_HEADER_H}
        x2={pos.x + pos.w}
        y2={pos.y + TABLE_HEADER_H}
        stroke="var(--border)"
      />

      {/* Table name */}
      <text
        x={pos.x + 14}
        y={pos.y + TABLE_HEADER_H / 2 + 1}
        dominantBaseline="middle"
        fill={isActive ? 'var(--text-accent)' : 'var(--text-primary)'}
        fontSize="12"
        fontWeight="600"
        fontFamily="Inter, sans-serif"
        onDoubleClick={onAnnotationDblClick}
        style={onAnnotationDblClick ? { cursor: 'text' } : undefined}
      >
        {truncText(pos.table.name, pos.w - 74, 6.0)}
      </text>

      {/* Annotation indicator ✎ */}
      {annotation && (
        <text
          x={pos.x + 14 + Math.min(pos.table.name.length * 7, 120) + 8}
          y={pos.y + TABLE_HEADER_H / 2 + 1}
          dominantBaseline="middle"
          fontSize="11"
          fill="#f59e0b"
          style={{ cursor: 'pointer' }}
          onDoubleClick={onAnnotationDblClick}
        >
          ✎
        </text>
      )}

      {/* Row count badge */}
      {rowCount !== undefined && (
        <text
          x={pos.x + pos.w - (rootEntityColor ? 52 : (onAddColumnClick ? 28 : 12))}
          y={pos.y + TABLE_HEADER_H / 2 + 1}
          dominantBaseline="middle"
          textAnchor="end"
          fill="var(--text-muted)"
          fontSize="10"
          fontFamily="'JetBrains Mono', monospace"
          opacity={0.6}
        >
          {rowCount.toLocaleString()}
        </text>
      )}

      {/* ROOT badge */}
      {rootEntityColor && (
        <g>
          <rect
            x={pos.x + pos.w - 48}
            y={pos.y + TABLE_HEADER_H / 2 - 8}
            width={36}
            height={16}
            rx={4}
            fill={rootEntityColor + '25'}
            stroke={rootEntityColor}
            strokeWidth={0.8}
            strokeOpacity={0.5}
          />
          <text
            x={pos.x + pos.w - 30}
            y={pos.y + TABLE_HEADER_H / 2 + 1}
            dominantBaseline="middle"
            textAnchor="middle"
            fontSize="8"
            fontWeight="700"
            fill={rootEntityColor}
            fontFamily="Inter, sans-serif"
            letterSpacing="0.06em"
          >
            ROOT
          </text>
        </g>
      )}

      {/* + Add column button in header */}
      {onAddColumnClick && (
        <g
          className="er-add-col-header-btn"
          onClick={(e) => { e.stopPropagation(); onAddColumnClick() }}
          style={{ cursor: 'pointer' }}
        >
          <rect
            x={pos.x + pos.w - 22}
            y={pos.y + TABLE_HEADER_H / 2 - 8}
            width={16}
            height={16}
            rx={4}
            fill="var(--accent)"
            opacity={0.15}
          />
          <text
            x={pos.x + pos.w - 14}
            y={pos.y + TABLE_HEADER_H / 2 + 1}
            dominantBaseline="middle"
            textAnchor="middle"
            fontSize="14"
            fontWeight="400"
            fill="var(--accent)"
            fontFamily="Inter, sans-serif"
          >
            +
          </text>
        </g>
      )}

      {/* Columns */}
      {(() => {
        const vfkCols = new Set<string>()
        for (const vfk of pos.table.versioned_foreign_keys ?? []) {
          vfkCols.add(vfk.column)
          vfkCols.add(vfk.lsn_column)
        }
        return pos.table.columns.map((col, ci) => {
          const cy = pos.y + TABLE_HEADER_H + ci * COL_ROW_H + COL_ROW_H / 2
          const isVFK = vfkCols.has(col.name)
          const isHL = highlightedColumns?.has(col.name) ?? false
          const isFKHL = fkHighlightedColumns?.has(col.name) ?? false
          const canDragFK = !!onFKDragStart && !col.primary_key
          return (
            <g
              key={`col-${ci}`}
              className="er-col-row"
              onMouseEnter={(e) => onColumnHover?.(col.name, e.clientX, e.clientY)}
              onMouseLeave={() => onColumnHover?.(null, 0, 0)}
              onClick={(e) => { if (onSelectColumn) { e.stopPropagation(); onSelectColumn(ci) } }}
              style={{ cursor: onSelectColumn ? 'pointer' : 'default' }}
            >
              {/* Hover / search highlight background */}
              <rect
                className="er-col-row-bg"
                x={pos.x + 1}
                y={cy - COL_ROW_H / 2}
                width={pos.w - 2}
                height={COL_ROW_H}
                fill={isFKHL ? 'rgba(99,91,255,0.18)' : isHL ? 'rgba(34,211,238,0.09)' : 'transparent'}
                rx={3}
              />
              {/* FK-hover: left accent bar */}
              {isFKHL && (
                <rect
                  x={pos.x + 1}
                  y={cy - COL_ROW_H / 2 + 2}
                  width={3}
                  height={COL_ROW_H - 4}
                  fill="var(--accent)"
                  rx={1.5}
                />
              )}

              {/* PK / FK / VK badge */}
              {col.primary_key && (
                <text x={pos.x + 10} y={cy + 1} dominantBaseline="middle" fontSize="9" fill="var(--text-warning)">PK</text>
              )}
              {col.references && !col.primary_key && (
                <text x={pos.x + 10} y={cy + 1} dominantBaseline="middle" fontSize="9" fill="var(--accent)">FK</text>
              )}
              {isVFK && !col.primary_key && !col.references && (
                <text x={pos.x + 10} y={cy + 1} dominantBaseline="middle" fontSize="9" fill="#f59e0b">VK</text>
              )}

              {/* Column name */}
              <text
                x={pos.x + 32}
                y={cy + 1}
                dominantBaseline="middle"
                fill={isHL ? '#22d3ee' : 'var(--text-primary)'}
                fontSize="11"
                fontFamily="'JetBrains Mono', monospace"
              >
                {truncText(col.name, pos.w - 106, 6.7)}
              </text>

              {/* Column type */}
              <text
                x={pos.x + pos.w - (canDragFK ? 22 : 12)}
                y={cy + 1}
                dominantBaseline="middle"
                textAnchor="end"
                fill="var(--text-muted)"
                fontSize="10"
                fontFamily="'JetBrains Mono', monospace"
              >
                {col.type}
              </text>

              {/* FK drag handle — drag from here to another column to create FK */}
              {canDragFK && (
                <circle
                  cx={pos.x + pos.w - 9}
                  cy={cy}
                  r={4.5}
                  fill="var(--accent)"
                  opacity={0.25}
                  className="er-fk-drag-handle"
                  style={{ cursor: 'crosshair' }}
                  onMouseDown={(e) => {
                    e.stopPropagation()
                    onFKDragStart(col.name, e.clientX, e.clientY)
                  }}
                />
              )}
            </g>
          )
        })
      })()}

      {/* Indexes section */}
      {(pos.table.indexes?.length ?? 0) > 0 && (() => {
        const idxBaseY = pos.y + TABLE_HEADER_H + pos.table.columns.length * COL_ROW_H + 4
        return (
          <g>
            {/* Divider */}
            <line
              x1={pos.x + 8}
              y1={idxBaseY}
              x2={pos.x + pos.w - 8}
              y2={idxBaseY}
              stroke="var(--border)"
              strokeDasharray="3 2"
              opacity={0.6}
            />
            {/* Section label */}
            <text
              x={pos.x + 10}
              y={idxBaseY + IDX_HEADER_H / 2 + 2}
              dominantBaseline="middle"
              fontSize="9"
              fontWeight="600"
              fill="var(--text-muted)"
              fontFamily="Inter, sans-serif"
            >
              IDX
            </text>
            {pos.table.indexes!.map((idx, ii) => {
              const iy = idxBaseY + IDX_HEADER_H + ii * IDX_ROW_H + IDX_ROW_H / 2
              return (
                <g
                  key={`idx-${ii}`}
                  className="er-idx-row"
                  onClick={(e) => { if (onSelectIndex) { e.stopPropagation(); onSelectIndex(ii) } }}
                  style={{ cursor: onSelectIndex ? 'pointer' : 'default' }}
                >
                  {/* Hover background */}
                  <rect
                    className="er-idx-row-bg"
                    x={pos.x + 1}
                    y={iy - IDX_ROW_H / 2}
                    width={pos.w - 2}
                    height={IDX_ROW_H}
                    fill="transparent"
                    rx={3}
                  />
                  <text
                    x={pos.x + 32}
                    y={iy}
                    dominantBaseline="middle"
                    fill="var(--text-secondary)"
                    fontSize="10"
                    fontFamily="'JetBrains Mono', monospace"
                  >
                    {truncText(idx.name, pos.w - 84, 6.1)}
                  </text>
                  <text
                    x={pos.x + pos.w - 12}
                    y={iy}
                    dominantBaseline="middle"
                    textAnchor="end"
                    fill="var(--text-muted)"
                    fontSize="9"
                    fontFamily="'JetBrains Mono', monospace"
                  >
                    {idx.method}
                  </text>
                </g>
              )
            })}
          </g>
        )
      })()}
    </g>
  )
}

// ─── Entity legend panel ──────────────────────────────────

function EntityLegend({ groups, hoveredEntity, onHover, onFocus, isolatedEntity, onIsolate }: {
  groups: EntityGroup[]
  hoveredEntity: string | null
  onHover: (name: string | null) => void
  onFocus: (group: EntityGroup) => void
  isolatedEntity: string | null
  onIsolate: (name: string | null) => void
}) {
  const [collapsed, setCollapsed] = useState(false)

  if (groups.length === 0) return null

  return (
    <div className="er-entity-legend">
      <button
        className="er-entity-legend-header"
        onClick={() => setCollapsed(!collapsed)}
      >
        <span className="er-entity-legend-title">Entities</span>
        <span className="er-entity-legend-count">{groups.length}</span>
        {isolatedEntity && <span className="er-entity-legend-isolated-badge">isolated</span>}
        <span className={`er-entity-legend-chevron ${collapsed ? '' : 'open'}`}>&#9650;</span>
      </button>
      {!collapsed && (
        <div className="er-entity-legend-list">
          {groups.map((group) => (
            <div
              key={group.name}
              className={`er-entity-legend-item ${hoveredEntity === group.name ? 'active' : ''} ${isolatedEntity === group.name ? 'isolated' : ''}`}
              onMouseEnter={() => onHover(group.name)}
              onMouseLeave={() => onHover(null)}
            >
              <button
                className="er-entity-legend-item-main"
                onClick={() => onFocus(group)}
              >
                <span className="er-entity-legend-dot" style={{ background: group.color }} />
                <span className="er-entity-legend-name">{group.name}</span>
                <span className="er-entity-legend-meta">{group.tables.length} tables</span>
              </button>
              <button
                className={`er-isolate-btn ${isolatedEntity === group.name ? 'active' : ''}`}
                title={isolatedEntity === group.name ? 'Show all entities' : 'Isolate this entity'}
                onClick={(e) => {
                  e.stopPropagation()
                  onIsolate(isolatedEntity === group.name ? null : group.name)
                }}
              >
                {isolatedEntity === group.name ? '⊙' : '◎'}
              </button>
            </div>
          ))}
          {isolatedEntity !== null && (
            <button
              className="er-isolate-clear"
              onClick={() => onIsolate(null)}
            >
              ✕ Clear isolation
            </button>
          )}
        </div>
      )}
    </div>
  )
}

// ─── Main component ───────────────────────────────────────

export function ERDiagram({ model, selectedTable, onSelectTable, multiModel, onDomainClick, tableCounts, walMutationCounts, onAddColumn, onCreateFK, onSelectColumn, onSelectIndex, onAddTable, onDeleteTable, onUndo, onRedo, canUndo, canRedo }: Props) {
  const isMulti = !!multiModel && multiModel.domains.length > 0

  // ── Search
  const [erSearch, setErSearch] = useState('')
  const [searchVisible, setSearchVisible] = useState(false)

  // ── Column interactions
  const [hoveredColumnKey, setHoveredColumnKey]   = useState<string | null>(null)
  const [columnTooltip, setColumnTooltip]          = useState<{ x: number; y: number; col: SchemaColumn; tableName: string } | null>(null)
  const tooltipTimerRef                            = useRef<ReturnType<typeof setTimeout> | null>(null)

  // ── Context menu
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; tableKey: string; tableName: string } | null>(null)

  // ── Annotations (table-level notes)
  const [annotations, setAnnotations]           = useState<Record<string, string>>({})
  const [editingAnnotation, setEditingAnnotation] = useState<{
    tableKey: string; tableName: string; x: number; y: number; current: string
  } | null>(null)

  // ── WAL heat overlay toggle
  const [walOverlay, setWalOverlay] = useState(false)

  // ── Entity isolation
  const [isolatedEntity, setIsolatedEntity] = useState<string | null>(null)

  // ── FK drag-to-create
  const [fkDrag, setFkDrag] = useState<{
    fromTableKey: string; fromCol: string
    svgX: number; svgY: number; curSVGX: number; curSVGY: number
  } | null>(null)

  const svgRef = useRef<SVGSVGElement>(null)

  const {
    containerRef,
    positions: savedPositions,
    zoom,
    pan,
    hoveredRelationship,
    hoveredEntity,
    isDragging,
    isPanning,
    draggedTable,
    handleTableMouseDown,
    handleMouseMove,
    handleMouseUp,
    handleBackgroundMouseDown,
    zoomIn,
    zoomOut,
    fitToScreen,
    resetLayout,
    setHoveredRelationship,
    setHoveredEntity,
    setPan,
  } = useERDiagram(model, onSelectTable)

  // Single-domain mode
  const singlePositions = isMulti ? [] : computeTablePositions(model.tables, savedPositions, model.entities)
  const singleRels = isMulti ? [] : findRelationships(singlePositions, model.entities)

  // Multi-domain mode
  const multi = isMulti
    ? computeMultiDomainLayout(multiModel, savedPositions)
    : { groups: [] as DomainGroup[], allPositions: [] as TablePos[], rels: [] as Rel[] }

  const allTablePositions = isMulti ? multi.allPositions : singlePositions
  const allRels = isMulti ? multi.rels : singleRels

  // Compute entity groups
  const entityGroups = isMulti
    ? computeMultiDomainEntityGroups(multiModel, multi.allPositions)
    : computeEntityGroups(model.entities ?? [], singlePositions)

  // Build root table lookup: tableName|tableKey → entity color
  const rootTableColors = new Map<string, string>()
  entityGroups.forEach((group) => {
    rootTableColors.set(group.rootTable, group.color)
  })

  // Build entity membership for spotlight: tableName|tableKey → entity name
  const tableToEntityName = new Map<string, string>()
  entityGroups.forEach((group) => {
    for (const t of group.tables) {
      tableToEntityName.set(t, group.name)
    }
  })

  // ── Isolation: filter visible positions when an entity is isolated
  const visibleTablePositions = useMemo(() => {
    if (isolatedEntity === null) return allTablePositions
    return allTablePositions.filter(pos => {
      const key = pos.tableKey || pos.table.name
      return tableToEntityName.get(key) === isolatedEntity
    })
  }, [isolatedEntity, allTablePositions, tableToEntityName])

  const visibleTableKeySet = useMemo(() => {
    const s = new Set<string>()
    for (const p of visibleTablePositions) s.add(p.tableKey || p.table.name)
    return s
  }, [visibleTablePositions])

  const visibleRels = useMemo(() => {
    if (isolatedEntity === null) return allRels
    return allRels.filter(rel => {
      const fk = rel.from.tableKey || rel.from.table.name
      const tk = rel.to.tableKey || rel.to.table.name
      return visibleTableKeySet.has(fk) || visibleTableKeySet.has(tk)
    })
  }, [isolatedEntity, allRels, visibleTableKeySet])

  // ── FK highlight map: which column in each table to accent when a rel is hovered
  const fkHighlightMap = useMemo(() => {
    if (hoveredRelationship === null) return null
    const rel = visibleRels[hoveredRelationship]
    if (!rel) return null
    const map = new Map<string, Set<string>>()
    const fromKey = rel.from.tableKey || rel.from.table.name
    const toKey   = rel.to.tableKey   || rel.to.table.name
    map.set(fromKey, new Set([rel.fromCol]))
    // merge in case from === to (self-ref)
    const existing = map.get(toKey)
    if (existing) existing.add(rel.toCol)
    else map.set(toKey, new Set([rel.toCol]))
    return map
  }, [hoveredRelationship, visibleRels])
  const searchMatchedKeys = useMemo(() => {
    if (!erSearch.trim()) return null
    const q = erSearch.trim().toLowerCase()
    const matched = new Set<string>()
    for (const pos of visibleTablePositions) {
      const key = pos.tableKey || pos.table.name
      if (
        pos.table.name.toLowerCase().includes(q) ||
        pos.table.columns.some(c => c.name.toLowerCase().includes(q))
      ) {
        matched.add(key)
      }
    }
    return matched
  }, [erSearch, visibleTablePositions])

  // Column names matched by search (for per-column highlighting)
  const searchMatchedCols = useMemo(() => {
    if (!erSearch.trim()) return null
    const q = erSearch.trim().toLowerCase()
    const s = new Set<string>()
    for (const pos of visibleTablePositions) {
      for (const c of pos.table.columns) {
        if (c.name.toLowerCase().includes(q)) s.add(c.name)
      }
    }
    return s
  }, [erSearch, visibleTablePositions])

  // ── WAL heat
  const walMaxCount = useMemo(() => {
    const src = walMutationCounts ?? tableCounts ?? {}
    return Math.max(1, ...Object.values(src))
  }, [walMutationCounts, tableCounts])

  function walHeatBgFor(tableName: string): string | undefined {
    if (!walOverlay) return undefined
    const src = walMutationCounts ?? tableCounts ?? {}
    const count = src[tableName] ?? 0
    if (count === 0) return undefined
    const ratio = Math.min(count / walMaxCount, 1)
    const r = Math.round(239 * ratio)
    const g = Math.round(68 * (1 - ratio * 0.5))
    const b = Math.round(68 * (1 - ratio * 0.8))
    return `rgba(${r},${g},${b},${0.08 + ratio * 0.22})`
  }

  // ── SVG coordinate conversion
  function clientToSVG(clientX: number, clientY: number) {
    const el = containerRef.current
    if (!el) return { x: clientX, y: clientY }
    const rect = el.getBoundingClientRect()
    return { x: (clientX - rect.left - pan.x) / zoom, y: (clientY - rect.top - pan.y) / zoom }
  }

  // ── Column hover → tooltip + highlight FK lines
  const handleColumnHover = useCallback((tableKey: string, colName: string | null, clientX: number, clientY: number) => {
    if (tooltipTimerRef.current) clearTimeout(tooltipTimerRef.current)
    if (colName === null) {
      setHoveredColumnKey(null)
      tooltipTimerRef.current = setTimeout(() => setColumnTooltip(null), 120)
      return
    }
    setHoveredColumnKey(`${tableKey}:${colName}`)
    const pos = visibleTablePositions.find(p => (p.tableKey || p.table.name) === tableKey)
    const col = pos?.table.columns.find(c => c.name === colName)
    if (col) {
      tooltipTimerRef.current = setTimeout(() => {
        setColumnTooltip({ x: clientX, y: clientY, col, tableName: pos!.table.name })
      }, 420)
    }
  }, [visibleTablePositions])

  // Cleanup tooltip timer on unmount
  useEffect(() => () => { if (tooltipTimerRef.current) clearTimeout(tooltipTimerRef.current) }, [])

  // ── Context menu
  const handleTableContextMenu = useCallback((tableKey: string, tableName: string, clientX: number, clientY: number) => {
    setColumnTooltip(null)
    setContextMenu({ x: clientX, y: clientY, tableKey, tableName })
  }, [])

  useEffect(() => {
    if (!contextMenu) return
    const close = () => setContextMenu(null)
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [contextMenu])

  // ── DDL generator
  function generateDDL(table: SchemaTable): string {
    const cols = table.columns.map(c => {
      const pk = c.primary_key ? ' PRIMARY KEY' : ''
      const nn = !c.nullable && !c.primary_key ? ' NOT NULL' : ''
      const uq = c.unique && !c.primary_key ? ' UNIQUE' : ''
      const df = c.default_value ? ` DEFAULT ${c.default_value}` : ''
      const ref = c.references ? ` REFERENCES ${c.references.table}(${c.references.column})` : ''
      return `  ${c.name} ${c.type}${pk}${nn}${uq}${df}${ref}`
    }).join(',\n')
    return `CREATE TABLE ${table.name} (\n${cols}\n);`
  }

  // ── Markdown export
  function exportMarkdown() {
    const lines: string[] = ['# Schema\n']
    for (const p of visibleTablePositions) {
      const t = p.table
      lines.push(`## ${t.name}\n`)
      lines.push('| Column | Type | Nullable | Constraints |')
      lines.push('|--------|------|----------|-------------|')
      for (const c of t.columns) {
        const constraints: string[] = []
        if (c.primary_key) constraints.push('PK')
        if (!c.nullable && !c.primary_key) constraints.push('NOT NULL')
        if (c.unique && !c.primary_key) constraints.push('UNIQUE')
        if (c.references) constraints.push(`→ ${c.references.table}.${c.references.column}`)
        lines.push(`| \`${c.name}\` | \`${c.type}\` | ${c.nullable ? 'Yes' : 'No'} | ${constraints.join(', ')} |`)
      }
      lines.push('')
    }
    void downloadFile(lines.join('\n'), 'schema.md', 'text/markdown')
  }

  // ── FK drag-to-create helpers
  function findColumnAtSVGPoint(x: number, y: number, excludeKey?: string) {
    for (const pos of visibleTablePositions) {
      const key = pos.tableKey || pos.table.name
      if (key === excludeKey) continue
      if (x < pos.x || x > pos.x + pos.w) continue
      for (let ci = 0; ci < pos.table.columns.length; ci++) {
        const top = pos.y + TABLE_HEADER_H + ci * COL_ROW_H
        if (y >= top && y <= top + COL_ROW_H) {
          return { tableKey: key, tableName: pos.table.name, colName: pos.table.columns[ci].name }
        }
      }
    }
    return null
  }

  const handleFKDragStart = useCallback((tableKey: string, colName: string, clientX: number, clientY: number) => {
    const pt = clientToSVG(clientX, clientY)
    setFkDrag({ fromTableKey: tableKey, fromCol: colName, svgX: pt.x, svgY: pt.y, curSVGX: pt.x, curSVGY: pt.y })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pan, zoom])

  const handleMouseMoveEx = useCallback((e: React.MouseEvent) => {
    handleMouseMove(e)
    if (fkDrag) {
      const pt = clientToSVG(e.clientX, e.clientY)
      setFkDrag(prev => prev ? { ...prev, curSVGX: pt.x, curSVGY: pt.y } : null)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [handleMouseMove, fkDrag, pan, zoom])

  const handleMouseUpEx = useCallback((e: React.MouseEvent) => {
    handleMouseUp()
    if (fkDrag) {
      const pt = clientToSVG(e.clientX, e.clientY)
      const target = findColumnAtSVGPoint(pt.x, pt.y, fkDrag.fromTableKey)
      if (target && onCreateFK) {
        const fromPos = visibleTablePositions.find(p => (p.tableKey || p.table.name) === fkDrag.fromTableKey)
        onCreateFK(fromPos?.table.name ?? fkDrag.fromTableKey, fkDrag.fromCol, target.tableName, target.colName)
      }
      setFkDrag(null)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [handleMouseUp, fkDrag, onCreateFK, visibleTablePositions, pan, zoom])

  // ── Entity / rel helpers
  const isRelInEntity = (rel: Rel) => {
    if (!hoveredEntity) return true
    const fromKey = rel.from.tableKey || rel.from.table.name
    const toKey   = rel.to.tableKey   || rel.to.table.name
    return tableToEntityName.get(fromKey) === hoveredEntity || tableToEntityName.get(toKey) === hoveredEntity
  }

  const cursor = isDragging ? 'grabbing' : isPanning ? 'all-scroll' : fkDrag ? 'crosshair' : 'default'

  return (
    <div
      ref={containerRef}
      className="er-canvas"
      onMouseMove={handleMouseMoveEx}
      onMouseUp={handleMouseUpEx}
      style={{ cursor }}
    >
      <svg ref={svgRef} width="100%" height="100%" className="er-svg">
        <defs>
          <pattern
            id="er-grid-dots"
            width={24}
            height={24}
            patternUnits="userSpaceOnUse"
            patternTransform={`translate(${pan.x},${pan.y}) scale(${zoom})`}
          >
            <circle cx={12} cy={12} r={0.75} fill="rgba(255,255,255,0.04)" />
          </pattern>

          <marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="var(--accent)" opacity="0.7" />
          </marker>
          <marker id="arrowhead-hover" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="var(--accent)" opacity="1" />
          </marker>

          {/* Entity glow filter */}
          <filter id="entity-glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur in="SourceGraphic" stdDeviation="4" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Background */}
        <rect width="100%" height="100%" fill="var(--bg-main)" />
        <rect width="100%" height="100%" fill="url(#er-grid-dots)" />

        {/* Content group with pan/zoom transform */}
        <g
          transform={`translate(${pan.x},${pan.y}) scale(${zoom})`}
          onMouseDown={handleBackgroundMouseDown}
        >
          {/* Invisible background rect for pan interaction */}
          <rect x={-10000} y={-10000} width={20000} height={20000} fill="transparent" />

          {/* Layer 1: Domain group rectangles (multi-domain only) */}
          {isMulti && multi.groups.map((group) => (
            <g
              key={`domain-group-${group.domain}`}
              onClick={() => onDomainClick?.(group.domain)}
              style={{ cursor: onDomainClick ? 'pointer' : 'default' }}
            >
              <rect
                x={group.x} y={group.y} width={group.w} height={group.h}
                rx={12}
                fill={group.color + '0a'}
                stroke={group.color}
                strokeWidth={1.5}
                strokeOpacity={0.3}
                strokeDasharray="8 4"
              />
              <text
                x={group.x + 16} y={group.y + 22}
                fontSize="14" fontWeight="700"
                fill={group.color} fontFamily="Inter, sans-serif" opacity={0.8}
              >
                {group.domain}
              </text>
            </g>
          ))}

          {/* Layer 2: Entity contours */}
          {entityGroups.map((group) => (
            <EntityContour
              key={`entity-${group.name}`}
              group={group}
              isHovered={hoveredEntity === group.name}
              onHover={setHoveredEntity}
            />
          ))}

          {/* Layer 3: Relationships */}
          {visibleRels.map((rel, i) => (
            <RelationshipPath
              key={`rel-${i}`}
              rel={rel}
              index={i}
              isHovered={hoveredRelationship === i}
              onHover={setHoveredRelationship}
              dimmed={hoveredEntity !== null && !isRelInEntity(rel)}
              highlightedColKey={hoveredColumnKey}
            />
          ))}

          {/* Layer 4: Tables */}
          {visibleTablePositions.map((pos) => {
            const isActive    = !isMulti && pos.index === selectedTable
            const tableKey    = pos.tableKey || pos.table.name
            const isBeingDragged = draggedTable === tableKey
            const rootColor   = rootTableColors.get(tableKey)
            const entityName  = tableToEntityName.get(tableKey)
            const entityDimmed = hoveredEntity !== null && entityName !== hoveredEntity
            const searchDimmed = searchMatchedKeys !== null && !searchMatchedKeys.has(tableKey)
            const dimmed      = entityDimmed || searchDimmed
            return (
              <TableCard
                key={`table-${tableKey}`}
                pos={pos}
                isActive={isActive}
                isBeingDragged={isBeingDragged}
                onMouseDown={(e) => handleTableMouseDown(tableKey, { x: pos.x, y: pos.y }, e)}
                rootEntityColor={rootColor}
                dimmed={dimmed}
                rowCount={tableCounts?.[pos.table.name]}
                walHeatBg={walHeatBgFor(pos.table.name)}
                highlightedColumns={searchMatchedCols ?? undefined}
                fkHighlightedColumns={fkHighlightMap?.get(tableKey)}
                onColumnHover={(colName, cx, cy) => handleColumnHover(tableKey, colName, cx, cy)}
                onTableContextMenu={(cx, cy) => handleTableContextMenu(tableKey, pos.table.name, cx, cy)}
                annotation={annotations[tableKey]}
                onAnnotationDblClick={() => {
                  const el = containerRef.current
                  if (!el) return
                  const rect = el.getBoundingClientRect()
                  setEditingAnnotation({
                    tableKey,
                    tableName: pos.table.name,
                    x: pos.x * zoom + pan.x + rect.left,
                    y: pos.y * zoom + pan.y + rect.top,
                    current: annotations[tableKey] ?? '',
                  })
                }}
                onAddColumnClick={onAddColumn ? () => onAddColumn(pos.table.name) : undefined}
                onFKDragStart={onCreateFK ? (colName, cx, cy) => handleFKDragStart(tableKey, colName, cx, cy) : undefined}
                onSelectColumn={onSelectColumn ? (ci) => {
                  const modelIdx = model.tables.findIndex(t => t.name === pos.table.name)
                  if (modelIdx >= 0) onSelectColumn(modelIdx, ci)
                } : undefined}
                onSelectIndex={onSelectIndex ? (ii) => {
                  const modelIdx = model.tables.findIndex(t => t.name === pos.table.name)
                  if (modelIdx >= 0) onSelectIndex(modelIdx, ii)
                } : undefined}
              />
            )
          })}

          {/* FK drag-in-progress line */}
          {fkDrag && (
            <>
              <line
                x1={fkDrag.svgX}   y1={fkDrag.svgY}
                x2={fkDrag.curSVGX} y2={fkDrag.curSVGY}
                stroke="#22d3ee"
                strokeWidth={2 / zoom}
                strokeDasharray={`${6 / zoom} ${3 / zoom}`}
                opacity={0.85}
              />
              <circle cx={fkDrag.curSVGX} cy={fkDrag.curSVGY} r={5 / zoom} fill="#22d3ee" opacity={0.7} />
            </>
          )}
        </g>
      </svg>

      {/* ── Column tooltip overlay ────────────────── */}
      {columnTooltip && (
        <div
          className="er-col-tooltip"
          style={{ left: columnTooltip.x + 18, top: columnTooltip.y - 10 }}
          onMouseEnter={() => { if (tooltipTimerRef.current) clearTimeout(tooltipTimerRef.current) }}
          onMouseLeave={() => setColumnTooltip(null)}
        >
          <div className="er-col-tooltip-name">{columnTooltip.col.name}</div>
          <div className="er-col-tooltip-row"><span>Type</span><span>{columnTooltip.col.type}</span></div>
          <div className="er-col-tooltip-row"><span>Nullable</span><span>{columnTooltip.col.nullable ? 'Yes' : 'No'}</span></div>
          {columnTooltip.col.unique && (
            <div className="er-col-tooltip-row"><span>Unique</span><span>Yes</span></div>
          )}
          {columnTooltip.col.primary_key && (
            <div className="er-col-tooltip-row"><span>Primary Key</span><span>Yes</span></div>
          )}
          {columnTooltip.col.default_value && (
            <div className="er-col-tooltip-row"><span>Default</span><span>{columnTooltip.col.default_value}</span></div>
          )}
          {columnTooltip.col.references && (
            <div className="er-col-tooltip-row">
              <span>References</span>
              <span>{columnTooltip.col.references.table}.{columnTooltip.col.references.column}</span>
            </div>
          )}
          {columnTooltip.tableName && annotations[columnTooltip.tableName] && (
            <div className="er-col-tooltip-note">{annotations[columnTooltip.tableName]}</div>
          )}
        </div>
      )}

      {/* ── Context menu overlay ──────────────────── */}
      {contextMenu && (
        <div
          className="er-context-menu"
          style={{ left: contextMenu.x, top: contextMenu.y }}
          onMouseDown={(e) => e.stopPropagation()}
        >
          <button className="er-context-menu-item" onClick={() => {
            navigator.clipboard?.writeText(contextMenu.tableName)
            setContextMenu(null)
          }}>
            Copy table name
          </button>
          <button className="er-context-menu-item" onClick={() => {
            const p = visibleTablePositions.find(p => (p.tableKey || p.table.name) === contextMenu.tableKey)
            if (p) navigator.clipboard?.writeText(generateDDL(p.table))
            setContextMenu(null)
          }}>
            Copy CREATE TABLE
          </button>
          <div className="er-context-menu-sep" />
          {onAddColumn && (
            <button className="er-context-menu-item" onClick={() => {
              onAddColumn(contextMenu.tableName)
              setContextMenu(null)
            }}>
              Add column…
            </button>
          )}
          <button className="er-context-menu-item" onClick={() => {
            const p = visibleTablePositions.find(p => (p.tableKey || p.table.name) === contextMenu.tableKey)
            if (p) {
              const el = containerRef.current
              const rect = el?.getBoundingClientRect()
              setEditingAnnotation({
                tableKey: contextMenu.tableKey,
                tableName: contextMenu.tableName,
                x: p.x * zoom + pan.x + (rect?.left ?? 0),
                y: p.y * zoom + pan.y + (rect?.top ?? 0),
                current: annotations[contextMenu.tableKey] ?? '',
              })
            }
            setContextMenu(null)
          }}>
            {annotations[contextMenu.tableKey] ? 'Edit note…' : 'Add note…'}
          </button>
          {annotations[contextMenu.tableKey] && (
            <button className="er-context-menu-item er-context-menu-item-danger" onClick={() => {
              setAnnotations(prev => { const n = { ...prev }; delete n[contextMenu.tableKey]; return n })
              setContextMenu(null)
            }}>
              Remove note
            </button>
          )}
          {onDeleteTable && model.tables.length > 1 && (
            <>
              <div className="er-context-menu-sep" />
              <button className="er-context-menu-item er-context-menu-item-danger" onClick={() => {
                onDeleteTable(contextMenu.tableName)
                setContextMenu(null)
              }}>
                Delete table…
              </button>
            </>
          )}
          <div className="er-context-menu-sep" />
          <button className="er-context-menu-item" onClick={() => {
            const p = visibleTablePositions.find(p => (p.tableKey || p.table.name) === contextMenu.tableKey)
            if (p) fitToScreen([p])
            setContextMenu(null)
          }}>
            Focus table
          </button>
        </div>
      )}

      {/* ── Annotation editor overlay ─────────────── */}
      {editingAnnotation && (
        <div
          className="er-annotation-editor"
          style={{ left: editingAnnotation.x, top: editingAnnotation.y - 44 }}
          onMouseDown={(e) => e.stopPropagation()}
        >
          <div className="er-annotation-editor-label">Note for <strong>{editingAnnotation.tableName}</strong></div>
          <textarea
            className="er-annotation-textarea"
            defaultValue={editingAnnotation.current}
            autoFocus
            rows={3}
            placeholder="Add a note or annotation…"
            onBlur={(e) => {
              const val = e.target.value.trim()
              setAnnotations(prev =>
                val
                  ? { ...prev, [editingAnnotation.tableKey]: val }
                  : (() => { const n = { ...prev }; delete n[editingAnnotation.tableKey]; return n })()
              )
              setEditingAnnotation(null)
            }}
            onKeyDown={(e) => {
              if (e.key === 'Escape') setEditingAnnotation(null)
              if (e.key === 'Enter' && !e.shiftKey) (e.target as HTMLTextAreaElement).blur()
            }}
          />
        </div>
      )}

      {/* Entity legend panel */}
      <EntityLegend
        groups={entityGroups}
        hoveredEntity={hoveredEntity}
        onHover={setHoveredEntity}
        onFocus={(group) => fitToScreen([{ x: group.x, y: group.y, w: group.w, h: group.h }])}
        isolatedEntity={isolatedEntity}
        onIsolate={setIsolatedEntity}
      />

      {/* ── Canvas toolbar ────────────────────────── */}
      <div className="er-toolbar">
        {/* Add table + undo/redo */}
        {(onAddTable || onUndo) && (
          <>
            {onAddTable && (
              <button className="er-toolbar-btn" onClick={onAddTable} title="Add table">
                <IconPlus />
              </button>
            )}
            {onUndo && (
              <button className="er-toolbar-btn" onClick={onUndo} disabled={!canUndo} title="Undo (⌘Z)" style={{ opacity: canUndo ? 1 : 0.3 }}>
                <IconUndo />
              </button>
            )}
            {onRedo && (
              <button className="er-toolbar-btn" onClick={onRedo} disabled={!canRedo} title="Redo (⌘⇧Z)" style={{ opacity: canRedo ? 1 : 0.3 }}>
                <IconRedo />
              </button>
            )}
            <span className="er-toolbar-sep" />
          </>
        )}
        {/* Search */}
        <button
          className="er-toolbar-btn"
          onClick={() => { setSearchVisible(!searchVisible); if (searchVisible) setErSearch('') }}
          title="Search tables / columns (⌘K)"
        >
          <IconSearch />
        </button>
        {searchVisible && (
          <input
            className="er-search-input"
            type="text"
            placeholder="Table or column…"
            value={erSearch}
            onChange={(e) => setErSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && searchMatchedKeys && searchMatchedKeys.size > 0) {
                const matched = visibleTablePositions.filter(p => searchMatchedKeys.has(p.tableKey || p.table.name))
                fitToScreen(matched)
              }
              if (e.key === 'Escape') { setErSearch(''); setSearchVisible(false) }
            }}
            autoFocus
            spellCheck={false}
          />
        )}
        <span className="er-toolbar-sep" />

        {/* Zoom controls */}
        <button className="er-toolbar-btn" onClick={zoomOut} title="Zoom out">
          <IconMinus />
        </button>
        <span className="er-toolbar-zoom">{Math.round(zoom * 100)}%</span>
        <button className="er-toolbar-btn" onClick={zoomIn} title="Zoom in">
          <IconPlus />
        </button>
        <span className="er-toolbar-sep" />

        {/* Fit / reset */}
        <button
          className="er-toolbar-btn"
          onClick={() => fitToScreen(visibleTablePositions)}
          title="Fit to screen"
        >
          <IconMaximize />
        </button>
        <button className="er-toolbar-btn" onClick={resetLayout} title="Auto-arrange">
          <IconGrid />
        </button>
        <span className="er-toolbar-sep" />

        {/* WAL heat overlay toggle */}
        <button
          className={`er-toolbar-btn ${walOverlay ? 'active' : ''}`}
          onClick={() => setWalOverlay(!walOverlay)}
          title={walOverlay ? 'Hide write-frequency heat overlay' : 'Show write-frequency heat overlay'}
          style={{ fontSize: '13px' }}
        >
          🌡
        </button>
        <span className="er-toolbar-sep" />

        {/* Export */}
        <button
          className="er-toolbar-btn"
          onClick={() => svgRef.current && exportSVG(svgRef.current)}
          title="Export as SVG"
        >
          <IconDownload />
        </button>
        <button
          className="er-toolbar-btn"
          onClick={() => svgRef.current && exportPNG(svgRef.current)}
          title="Export as PNG"
        >
          <IconImage />
        </button>
        <button
          className="er-toolbar-btn"
          onClick={exportMarkdown}
          title="Export as Markdown"
          style={{ fontSize: '12px', fontWeight: 600 }}
        >
          MD
        </button>
      </div>

      {/* Minimap */}
      {visibleTablePositions.length > 1 && containerRef.current && (
        <ERMinimap
          tables={visibleTablePositions}
          zoom={zoom}
          pan={pan}
          containerWidth={containerRef.current.clientWidth}
          containerHeight={containerRef.current.clientHeight}
          onPan={(p) => setPan(p)}
        />
      )}
    </div>
  )
}
