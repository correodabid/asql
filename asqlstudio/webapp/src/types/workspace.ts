export type WorkspaceTab = {
  id: string
  label: string
  sql: string
  explainEnabled: boolean
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

export type ExplainCandidate = {
  strategy: string
  cost?: number
  detail?: string
  chosen?: boolean
  rejected_reason?: string
}

export type ExplainPrunedCandidate = {
  strategy: string
  detail?: string
  reason: string
}

export type AccessPlan = {
  strategy: string
  table_rows: number
  estimated_rows?: number
  index_used?: string
  index_type?: string
  index_column?: string
  indexed_predicates?: string[]
  residual_predicate?: string
  candidates?: ExplainCandidate[]
  pruned_candidates?: ExplainPrunedCandidate[]
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

export type AssistantQueryPlan = {
  status: string
  question: string
  domain: string
  mode: string
  planner?: 'deterministic' | 'llm' | string
  provider?: string
  model?: string
  summary: string
  sql: string
  primary_table?: string
  matched_tables?: string[]
  matched_columns?: string[]
  assumptions?: string[]
  warnings?: string[]
  confidence?: 'high' | 'medium' | 'low' | string
}

export type AssistantLLMRequest = {
  enabled: boolean
  provider: string
  base_url?: string
  model?: string
  api_key?: string
  allow_fallback?: boolean
}
