export type WorkspaceTab = {
  id: string
  label: string
  sql: string
  result: QueryResult | null
  results: QueryResult[]
  error: string | null
  loading: boolean
  tableName: string | null
  selectedRow: number | null
  explainPlan: ExplainPlan | null
}

export type QueryResult = {
  columns: string[]
  rows: Record<string, unknown>[]
  rowCount: number
  duration: number
  status: string
  route?: string
  consistency?: string
  asOfLSN?: number
}

export type ExplainPlan = {
  operation: string
  domain: string
  table: string
  planShape: Record<string, unknown>
  accessPlan?: AccessPlan
}

export type AccessPlan = {
  strategy: string
  table_rows: number
  estimated_rows?: number
  index_used?: string
  index_type?: string
  index_column?: string
  candidates?: { strategy: string; cost: number; chosen?: boolean }[]
  joins?: { table: string; join_type: string; strategy: string; table_rows: number; index_used?: string }[]
}

export type HistoryEntry = {
  sql: string
  ts: number
  ok: boolean
  duration: number
  rowCount: number
}

export type TxState = {
  txId: string
  domains: string[]
  mode: string
}

export type CellEdit = {
  rowIndex: number
  columnName: string
  originalValue: unknown
  currentValue: string
}

export type ForeignKeyLink = {
  column: string
  refTable: string
  refColumn: string
}

export type ReverseFK = {
  table: string
  column: string
  refColumn: string
}

export type TableInfo = {
  name: string
  pk_columns: string[]
}
