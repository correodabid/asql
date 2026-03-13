export type SchemaReference = {
  table: string
  column: string
}

export type SchemaColumn = {
  name: string
  type: string
  nullable: boolean
  primary_key: boolean
  unique: boolean
  default_value?: string
  references?: SchemaReference
}

export type SchemaIndex = {
  name: string
  columns: string[]
  method: 'hash' | 'btree'
}

export type VersionedFK = {
  column: string
  lsn_column: string
  references_domain: string
  references_table: string
  references_column: string
}

export type SchemaTable = {
  name: string
  columns: SchemaColumn[]
  indexes?: SchemaIndex[]
  versioned_foreign_keys?: VersionedFK[]
}

export type EntityDefinition = {
  name: string
  root_table: string
  tables: string[]
}

export type SchemaModel = {
  domain: string
  tables: SchemaTable[]
  entities?: EntityDefinition[]
}

export type MultiDomainModel = {
  domains: SchemaModel[]
}

export type DiffOperation = {
  type: string
  table: string
  column?: string
  statement?: string
  safe: boolean
  breaking?: boolean
  reason?: string
}

export type DiffResponse = {
  domain: string
  safe: boolean
  operations: DiffOperation[]
  statements: string[]
  warnings?: string[]
}

export type ApplySafeDiffResponse = {
  status: string
  tx_id: string
  domain: string
  diff_safe: boolean
  applied_count: number
  unsafe_count: number
  executed_statements: string[]
  warnings?: string[]
}

export type DDLResponse = {
  ddl: string
  statements: string[]
}

export const DEFAULT_MODEL: SchemaModel = {
  domain: 'default',
  tables: [],
}

export const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value)) as T
