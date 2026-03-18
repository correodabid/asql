export namespace httpapi {
	
	export class TimelineCommitsRequest {
	    from_lsn?: number;
	    to_lsn?: number;
	    limit?: number;
	    domain?: string;
	
	    static createFrom(source: any = {}) {
	        return new TimelineCommitsRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.from_lsn = source["from_lsn"];
	        this.to_lsn = source["to_lsn"];
	        this.limit = source["limit"];
	        this.domain = source["domain"];
	    }
	}

}

export namespace studioapp {
	
	export class assistantLLMModelCatalog {
	    id: string;
	    label?: string;
	
	    static createFrom(source: any = {}) {
	        return new assistantLLMModelCatalog(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.label = source["label"];
	    }
	}
	export class assistantLLMProviderCatalog {
	    id: string;
	    label: string;
	    transport: string;
	    default_base_url?: string;
	    supports_custom_base_url?: boolean;
	    supports_custom_model?: boolean;
	    model_placeholder?: string;
	    api_key_mode?: string;
	    api_key_label?: string;
	    api_key_placeholder?: string;
	    models?: assistantLLMModelCatalog[];
	
	    static createFrom(source: any = {}) {
	        return new assistantLLMProviderCatalog(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.label = source["label"];
	        this.transport = source["transport"];
	        this.default_base_url = source["default_base_url"];
	        this.supports_custom_base_url = source["supports_custom_base_url"];
	        this.supports_custom_model = source["supports_custom_model"];
	        this.model_placeholder = source["model_placeholder"];
	        this.api_key_mode = source["api_key_mode"];
	        this.api_key_label = source["api_key_label"];
	        this.api_key_placeholder = source["api_key_placeholder"];
	        this.models = this.convertValues(source["models"], assistantLLMModelCatalog);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class assistantLLMCatalog {
	    default_provider: string;
	    providers: assistantLLMProviderCatalog[];
	
	    static createFrom(source: any = {}) {
	        return new assistantLLMCatalog(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.default_provider = source["default_provider"];
	        this.providers = this.convertValues(source["providers"], assistantLLMProviderCatalog);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	
	export class assistantLLMSettings {
	    enabled?: boolean;
	    provider?: string;
	    base_url?: string;
	    model?: string;
	    api_key?: string;
	    temperature?: number;
	    allow_fallback?: boolean;
	
	    static createFrom(source: any = {}) {
	        return new assistantLLMSettings(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.enabled = source["enabled"];
	        this.provider = source["provider"];
	        this.base_url = source["base_url"];
	        this.model = source["model"];
	        this.api_key = source["api_key"];
	        this.temperature = source["temperature"];
	        this.allow_fallback = source["allow_fallback"];
	    }
	}
	export class assistantQueryRequest {
	    question: string;
	    domains: string[];
	    llm?: assistantLLMSettings;
	
	    static createFrom(source: any = {}) {
	        return new assistantQueryRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.question = source["question"];
	        this.domains = source["domains"];
	        this.llm = this.convertValues(source["llm"], assistantLLMSettings);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class assistantQueryResponse {
	    status: string;
	    question: string;
	    domain: string;
	    mode: string;
	    planner?: string;
	    provider?: string;
	    model?: string;
	    summary: string;
	    sql: string;
	    primary_table?: string;
	    matched_tables?: string[];
	    matched_columns?: string[];
	    assumptions?: string[];
	    warnings?: string[];
	    confidence?: string;
	
	    static createFrom(source: any = {}) {
	        return new assistantQueryResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.status = source["status"];
	        this.question = source["question"];
	        this.domain = source["domain"];
	        this.mode = source["mode"];
	        this.planner = source["planner"];
	        this.provider = source["provider"];
	        this.model = source["model"];
	        this.summary = source["summary"];
	        this.sql = source["sql"];
	        this.primary_table = source["primary_table"];
	        this.matched_tables = source["matched_tables"];
	        this.matched_columns = source["matched_columns"];
	        this.assumptions = source["assumptions"];
	        this.warnings = source["warnings"];
	        this.confidence = source["confidence"];
	    }
	}
	export class beginRequest {
	    mode: string;
	    domains: string[];
	
	    static createFrom(source: any = {}) {
	        return new beginRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.mode = source["mode"];
	        this.domains = source["domains"];
	    }
	}
	export class connectionSwitchRequest {
	    pgwire_endpoint: string;
	    follower_endpoint?: string;
	    peer_endpoints?: string[];
	    admin_endpoints?: string[];
	    auth_token?: string;
	    admin_auth_token?: string;
	    data_dir?: string;
	
	    static createFrom(source: any = {}) {
	        return new connectionSwitchRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.pgwire_endpoint = source["pgwire_endpoint"];
	        this.follower_endpoint = source["follower_endpoint"];
	        this.peer_endpoints = source["peer_endpoints"];
	        this.admin_endpoints = source["admin_endpoints"];
	        this.auth_token = source["auth_token"];
	        this.admin_auth_token = source["admin_auth_token"];
	        this.data_dir = source["data_dir"];
	    }
	}
	export class entityVersionHistoryRequest {
	    domain: string;
	    entity_name: string;
	    root_pk: string;
	
	    static createFrom(source: any = {}) {
	        return new entityVersionHistoryRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.domain = source["domain"];
	        this.entity_name = source["entity_name"];
	        this.root_pk = source["root_pk"];
	    }
	}
	export class executeBatchRequest {
	    tx_id: string;
	    statements: string[];
	
	    static createFrom(source: any = {}) {
	        return new executeBatchRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.tx_id = source["tx_id"];
	        this.statements = source["statements"];
	    }
	}
	export class executeRequest {
	    tx_id: string;
	    sql: string;
	
	    static createFrom(source: any = {}) {
	        return new executeRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.tx_id = source["tx_id"];
	        this.sql = source["sql"];
	    }
	}
	export class explainRequest {
	    sql: string;
	    domains?: string[];
	
	    static createFrom(source: any = {}) {
	        return new explainRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sql = source["sql"];
	        this.domains = source["domains"];
	    }
	}
	export class fixtureExportRequest {
	    file_path: string;
	    domains: string[];
	    name?: string;
	    description?: string;
	
	    static createFrom(source: any = {}) {
	        return new fixtureExportRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.file_path = source["file_path"];
	        this.domains = source["domains"];
	        this.name = source["name"];
	        this.description = source["description"];
	    }
	}
	export class readQueryRequest {
	    sql: string;
	    domains: string[];
	    consistency?: string;
	    max_lag?: number;
	
	    static createFrom(source: any = {}) {
	        return new readQueryRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sql = source["sql"];
	        this.domains = source["domains"];
	        this.consistency = source["consistency"];
	        this.max_lag = source["max_lag"];
	    }
	}
	export class readQueryResponse {
	    status: string;
	    rows?: any[];
	    route: string;
	    consistency: string;
	    as_of_lsn: number;
	    leader_lsn: number;
	    follower_lsn?: number;
	    lag: number;
	
	    static createFrom(source: any = {}) {
	        return new readQueryResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.status = source["status"];
	        this.rows = source["rows"];
	        this.route = source["route"];
	        this.consistency = source["consistency"];
	        this.as_of_lsn = source["as_of_lsn"];
	        this.leader_lsn = source["leader_lsn"];
	        this.follower_lsn = source["follower_lsn"];
	        this.lag = source["lag"];
	    }
	}
	export class rowHistoryRequest {
	    sql: string;
	    domains?: string[];
	
	    static createFrom(source: any = {}) {
	        return new rowHistoryRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sql = source["sql"];
	        this.domains = source["domains"];
	    }
	}
	export class schemaApplyStatementsRequest {
	    domain: string;
	    statements: string[];
	
	    static createFrom(source: any = {}) {
	        return new schemaApplyStatementsRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.domain = source["domain"];
	        this.statements = source["statements"];
	    }
	}
	export class schemaDDLReference {
	    table: string;
	    column: string;
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLReference(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.table = source["table"];
	        this.column = source["column"];
	    }
	}
	export class schemaDDLColumn {
	    name: string;
	    type: string;
	    nullable: boolean;
	    primary_key: boolean;
	    unique: boolean;
	    default_value?: string;
	    references?: schemaDDLReference;
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLColumn(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.type = source["type"];
	        this.nullable = source["nullable"];
	        this.primary_key = source["primary_key"];
	        this.unique = source["unique"];
	        this.default_value = source["default_value"];
	        this.references = this.convertValues(source["references"], schemaDDLReference);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class schemaDDLEntity {
	    name: string;
	    root_table: string;
	    tables: string[];
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLEntity(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.root_table = source["root_table"];
	        this.tables = source["tables"];
	    }
	}
	export class schemaDDLIndex {
	    name: string;
	    columns: string[];
	    method: string;
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLIndex(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.columns = source["columns"];
	        this.method = source["method"];
	    }
	}
	
	export class schemaDDLVersionedFK {
	    column: string;
	    lsn_column: string;
	    references_domain: string;
	    references_table: string;
	    references_column: string;
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLVersionedFK(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.column = source["column"];
	        this.lsn_column = source["lsn_column"];
	        this.references_domain = source["references_domain"];
	        this.references_table = source["references_table"];
	        this.references_column = source["references_column"];
	    }
	}
	export class schemaDDLTable {
	    name: string;
	    columns: schemaDDLColumn[];
	    indexes?: schemaDDLIndex[];
	    versioned_foreign_keys?: schemaDDLVersionedFK[];
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLTable(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.columns = this.convertValues(source["columns"], schemaDDLColumn);
	        this.indexes = this.convertValues(source["indexes"], schemaDDLIndex);
	        this.versioned_foreign_keys = this.convertValues(source["versioned_foreign_keys"], schemaDDLVersionedFK);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class schemaDDLRequest {
	    domain: string;
	    tables: schemaDDLTable[];
	    entities?: schemaDDLEntity[];
	
	    static createFrom(source: any = {}) {
	        return new schemaDDLRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.domain = source["domain"];
	        this.tables = this.convertValues(source["tables"], schemaDDLTable);
	        this.entities = this.convertValues(source["entities"], schemaDDLEntity);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	
	export class schemaDiffRequest {
	    base: schemaDDLRequest;
	    target: schemaDDLRequest;
	
	    static createFrom(source: any = {}) {
	        return new schemaDiffRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.base = this.convertValues(source["base"], schemaDDLRequest);
	        this.target = this.convertValues(source["target"], schemaDDLRequest);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class schemaLoadBaselineRequest {
	    domain?: string;
	
	    static createFrom(source: any = {}) {
	        return new schemaLoadBaselineRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.domain = source["domain"];
	    }
	}
	export class temporalLookupRequest {
	    domain: string;
	    table_name: string;
	    primary_key: string;
	    entity_name?: string;
	    entity_root_pk?: string;
	
	    static createFrom(source: any = {}) {
	        return new temporalLookupRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.domain = source["domain"];
	        this.table_name = source["table_name"];
	        this.primary_key = source["primary_key"];
	        this.entity_name = source["entity_name"];
	        this.entity_root_pk = source["entity_root_pk"];
	    }
	}
	export class timeTravelRequest {
	    sql: string;
	    domains: string[];
	    lsn?: number;
	    logical_timestamp?: number;
	
	    static createFrom(source: any = {}) {
	        return new timeTravelRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sql = source["sql"];
	        this.domains = source["domains"];
	        this.lsn = source["lsn"];
	        this.logical_timestamp = source["logical_timestamp"];
	    }
	}
	export class txRequest {
	    tx_id: string;
	
	    static createFrom(source: any = {}) {
	        return new txRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.tx_id = source["tx_id"];
	    }
	}

}

