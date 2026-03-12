package main

import (
	"embed"
	"flag"
	"io/fs"
	"log/slog"
	"os"
	"strings"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
)

//go:embed all:web
var assets embed.FS

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

type explainRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

// envOr returns the value of the named environment variable, or fallback if unset/empty.
// This lets Wails dev mode be configured via env vars (e.g. ASQL_PGWIRE_ENDPOINT=...) without
// relying on -appargs, which Wails v2 dev mode does not forward to flag.Parse reliably.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	pgwireEndpoint := flag.String("pgwire-endpoint", envOr("ASQL_PGWIRE_ENDPOINT", "127.0.0.1:5433"), "ASQL pgwire endpoint")
	followerEndpoint := flag.String("follower-endpoint", os.Getenv("ASQL_FOLLOWER_ENDPOINT"), "optional follower ASQL pgwire endpoint for lag view")
	peerEndpointsFlag := flag.String("peer-endpoints", os.Getenv("ASQL_PEER_ENDPOINTS"), "comma-separated pgwire endpoints for all cluster nodes (enables full multi-node status)")
	adminEndpointsFlag := flag.String("admin-endpoints", os.Getenv("ASQL_ADMIN_ENDPOINTS"), "comma-separated admin HTTP endpoints for cluster metrics/health (for example 127.0.0.1:9091,127.0.0.1:9092)")
	authToken := flag.String("auth-token", os.Getenv("ASQL_AUTH_TOKEN"), "optional password for pgwire auth")
	dataDir := flag.String("data-dir", envOr("ASQL_DATA_DIR", ".asql"), "local ASQL data directory for recovery workflows")
	clusterGroups := flag.String("groups", os.Getenv("ASQL_GROUPS"), "comma-separated domain groups for cluster HA panel")
	// Legacy flag aliases kept for backwards compatibility.
	_ = flag.String("grpc-endpoint", "", "[deprecated] use -pgwire-endpoint")
	_ = flag.String("follower-grpc-endpoint", "", "[deprecated] use -follower-endpoint")
	_ = flag.String("http-addr", ":9080", "[deprecated] HTTP mode removed; use wails desktop")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	token := strings.TrimSpace(*authToken)
	engine := newEngineClient(*pgwireEndpoint, token)

	var follower *engineClient
	if endpoint := strings.TrimSpace(*followerEndpoint); endpoint != "" {
		follower = newEngineClient(endpoint, token)
	}

	// Build ordered peer list for cluster node status probing.
	// The first entry is always the leader engine so that ClusterNodeStatus()
	// can identify it via pointer equality.
	var peerEngines []*engineClient
	if raw := strings.TrimSpace(*peerEndpointsFlag); raw != "" {
		seenLeader := false
		for _, ep := range strings.Split(raw, ",") {
			if ep = strings.TrimSpace(ep); ep == "" {
				continue
			}
			// Re-use the already-created engine for the leader endpoint so that
			// pointer comparison in ClusterNodeStatus works correctly.
			if ep == strings.TrimSpace(*pgwireEndpoint) && !seenLeader {
				peerEngines = append(peerEngines, engine)
				seenLeader = true
			} else {
				peerEngines = append(peerEngines, newEngineClient(ep, token))
			}
		}
	}

	var groups []string
	if g := strings.TrimSpace(*clusterGroups); g != "" {
		for _, part := range strings.Split(g, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				groups = append(groups, trimmed)
			}
		}
	}

	var adminEndpoints []string
	if raw := strings.TrimSpace(*adminEndpointsFlag); raw != "" {
		for _, ep := range strings.Split(raw, ",") {
			if ep = strings.TrimSpace(ep); ep != "" {
				adminEndpoints = append(adminEndpoints, ep)
			}
		}
	}

	app := newApp(engine, follower, peerEngines, groups, adminEndpoints, *dataDir, logger)

	webContent, _ := fs.Sub(assets, "web")
	if err := wails.Run(&options.App{
		Title:         "ASQL Studio",
		Width:         1440,
		Height:        900,
		MinWidth:      1024,
		MinHeight:     600,
		DisableResize: false,
		Mac: &mac.Options{
			Preferences: &mac.Preferences{
				FullscreenEnabled: mac.Enabled,
			},
		},
		AssetServer: &assetserver.Options{
			Assets: webContent,
		},
		OnStartup: app.startup,
		Bind:      []interface{}{app},
	}); err != nil {
		logger.Error("studio terminated", slog.String("error", err.Error()))
		os.Exit(1)
	}
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
	NodeID    string `json:"node_id"`   // human-readable name (e.g. "node-b"); falls back to addr if unknown
	Addr      string `json:"addr"`      // pgwire address always present (e.g. "127.0.0.1:5434")
	Role      string `json:"role"`      // "leader" or "follower"
	LSN       uint64 `json:"lsn"`
	Lag       uint64 `json:"lag"`
	Reachable bool   `json:"reachable"`
}

type clusterNodeStatusResponse struct {
	Nodes []clusterNodeInfo `json:"nodes"`
}

type schemaTableInfo struct {
	Name      string   `json:"name"`
	PKColumns []string `json:"pk_columns"`
}

type schemaTablesResponse struct {
	Tables []schemaTableInfo `json:"tables"`
}
