package studioapp

type readRoutingStatsResponse struct {
	Counts map[string]uint64 `json:"counts,omitempty"`
}

type replicationLagResponse struct {
	LeaderLSN   uint64 `json:"leader_lsn"`
	FollowerLSN uint64 `json:"follower_lsn"`
	Lag         uint64 `json:"lag"`
}

type readQueryRequest struct {
	SQL         string   `json:"sql"`
	Domains     []string `json:"domains"`
	Consistency string   `json:"consistency,omitempty"`
	MaxLag      uint64   `json:"max_lag,omitempty"`
}

type readQueryResponse struct {
	Status      string                   `json:"status"`
	Rows        []map[string]interface{} `json:"rows,omitempty"`
	Route       string                   `json:"route"`
	Consistency string                   `json:"consistency"`
	AsOfLSN     uint64                   `json:"as_of_lsn"`
	LeaderLSN   uint64                   `json:"leader_lsn"`
	FollowerLSN uint64                   `json:"follower_lsn,omitempty"`
	Lag         uint64                   `json:"lag"`
}

type beginRequest struct {
	Mode    string   `json:"mode"`
	Domains []string `json:"domains"`
}

type executeRequest struct {
	TxID string `json:"tx_id"`
	SQL  string `json:"sql"`
}

type executeBatchRequest struct {
	TxID       string   `json:"tx_id"`
	Statements []string `json:"statements"`
}

type txRequest struct {
	TxID string `json:"tx_id"`
}

type timeTravelRequest struct {
	SQL              string   `json:"sql"`
	Domains          []string `json:"domains"`
	LSN              uint64   `json:"lsn,omitempty"`
	LogicalTimestamp uint64   `json:"logical_timestamp,omitempty"`
}

type rowHistoryRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type entityVersionHistoryRequest struct {
	Domain     string `json:"domain"`
	EntityName string `json:"entity_name"`
	RootPK     string `json:"root_pk"`
}

type temporalLookupRequest struct {
	Domain       string `json:"domain"`
	TableName    string `json:"table_name"`
	PrimaryKey   string `json:"primary_key"`
	EntityName   string `json:"entity_name,omitempty"`
	EntityRootPK string `json:"entity_root_pk,omitempty"`
}

type fixtureExportRequest struct {
	FilePath    string   `json:"file_path"`
	Domains     []string `json:"domains"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
}

type fixtureExportPathRequest struct {
	SuggestedName string `json:"suggested_name,omitempty"`
}

type explainRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type assistantQueryRequest struct {
	Question string   `json:"question"`
	Domains  []string `json:"domains"`
	LLM      *assistantLLMSettings `json:"llm,omitempty"`
}

type assistantLLMSettings struct {
	Enabled       bool    `json:"enabled,omitempty"`
	Provider      string  `json:"provider,omitempty"`
	BaseURL       string  `json:"base_url,omitempty"`
	Model         string  `json:"model,omitempty"`
	APIKey        string  `json:"api_key,omitempty"`
	Temperature   float64 `json:"temperature,omitempty"`
	AllowFallback bool    `json:"allow_fallback,omitempty"`
}

type assistantQueryResponse struct {
	Status         string   `json:"status"`
	Question       string   `json:"question"`
	Domain         string   `json:"domain"`
	Mode           string   `json:"mode"`
	Planner        string   `json:"planner,omitempty"`
	Provider       string   `json:"provider,omitempty"`
	Model          string   `json:"model,omitempty"`
	Summary        string   `json:"summary"`
	SQL            string   `json:"sql"`
	PrimaryTable   string   `json:"primary_table,omitempty"`
	MatchedTables  []string `json:"matched_tables,omitempty"`
	MatchedColumns []string `json:"matched_columns,omitempty"`
	Assumptions    []string `json:"assumptions,omitempty"`
	Warnings       []string `json:"warnings,omitempty"`
	Confidence     string   `json:"confidence,omitempty"`
}

type clusterGroupStatus struct {
	Group        string `json:"group"`
	LeaderID     string `json:"leader_id"`
	Term         uint64 `json:"term"`
	FencingToken string `json:"fencing_token,omitempty"`
	LeaseActive  bool   `json:"lease_active"`
	LastLSN      uint64 `json:"last_lsn"`
}

type clusterStatusResponse struct {
	Groups []clusterGroupStatus `json:"groups"`
}

type clusterNodeInfo struct {
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	Role      string `json:"role"`
	LSN       uint64 `json:"lsn"`
	Lag       uint64 `json:"lag"`
	Reachable bool   `json:"reachable"`
}

type clusterNodeStatusResponse struct {
	Nodes []clusterNodeInfo `json:"nodes"`
}

type connectionConfigResponse struct {
	PgwireEndpoint           string   `json:"pgwire_endpoint"`
	FollowerEndpoint         string   `json:"follower_endpoint,omitempty"`
	PeerEndpoints            []string `json:"peer_endpoints,omitempty"`
	AdminEndpoints           []string `json:"admin_endpoints,omitempty"`
	AuthTokenConfigured      bool     `json:"auth_token_configured"`
	AdminAuthTokenConfigured bool     `json:"admin_auth_token_configured"`
	DataDir                  string   `json:"data_dir,omitempty"`
}

type connectionSwitchRequest struct {
	PgwireEndpoint   string   `json:"pgwire_endpoint"`
	FollowerEndpoint string   `json:"follower_endpoint,omitempty"`
	PeerEndpoints    []string `json:"peer_endpoints,omitempty"`
	AdminEndpoints   []string `json:"admin_endpoints,omitempty"`
	AuthToken        string   `json:"auth_token,omitempty"`
	AdminAuthToken   string   `json:"admin_auth_token,omitempty"`
	DataDir          string   `json:"data_dir,omitempty"`
}

type connectionSwitchResponse struct {
	Status     string                   `json:"status"`
	Connection connectionConfigResponse `json:"connection"`
}

type schemaTableInfo struct {
	Name      string   `json:"name"`
	PKColumns []string `json:"pk_columns"`
}

type schemaTablesResponse struct {
	Tables []schemaTableInfo `json:"tables"`
}
