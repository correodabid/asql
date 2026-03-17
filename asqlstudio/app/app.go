package studioapp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"asql/pkg/fixtures"

	"github.com/jackc/pgx/v5"
	"github.com/wailsapp/wails/v2/pkg/runtime"

	api "asql/pkg/adminapi"
)

// App is the Wails application struct. All exported methods become IPC endpoints
// callable from the frontend via window.go.main.App.<MethodName>(args).
type App struct {
	ctx            context.Context
	logger         *slog.Logger
	engine         *engineClient
	schemaInvoker  engineInvoker
	followerEngine *engineClient
	peersMu        sync.RWMutex
	peerEngines    []*engineClient // all cluster nodes for status probing; grows as new nodes join
	leaderClient   *engineClient   // current active leader; nil means use a.engine
	routingStats   *readRoutingStats
	clusterGroups  []string
	adminEndpoints []string
	adminToken     string
	dataDir        string
}

// newApp constructs an App ready for wails.Run.
// peers is the full list of cluster pgwire endpoints (may include the leader).
// When peers is non-empty, ClusterNodeStatus probes all of them; otherwise it
// falls back to {engine, followerEngine} for single/two-node backward compat.
func newApp(engine *engineClient, follower *engineClient, peers []*engineClient, groups []string, adminEndpoints []string, adminToken string, dataDir string, logger *slog.Logger) *App {
	return &App{
		logger:         logger,
		engine:         engine,
		followerEngine: follower,
		peerEngines:    peers,
		routingStats:   newReadRoutingStats(),
		clusterGroups:  groups,
		adminEndpoints: adminEndpoints,
		adminToken:     strings.TrimSpace(adminToken),
		dataDir:        strings.TrimSpace(dataDir),
	}
}

type clusterAdminHealthResponse struct {
	Status         string   `json:"status"`
	Ready          bool     `json:"ready"`
	Live           bool     `json:"live"`
	ClusterMode    bool     `json:"cluster_mode"`
	NodeID         string   `json:"node_id,omitempty"`
	RaftRole       string   `json:"raft_role,omitempty"`
	LeaderID       string   `json:"leader_id,omitempty"`
	CurrentTerm    uint64   `json:"current_term,omitempty"`
	LastDurableLSN uint64   `json:"last_durable_lsn"`
	Reasons        []string `json:"reasons,omitempty"`
}

type clusterAdminRetentionResponse struct {
	DataDir           string `json:"data_dir,omitempty"`
	HeadLSN           uint64 `json:"head_lsn"`
	OldestRetainedLSN uint64 `json:"oldest_retained_lsn"`
	LastRetainedLSN   uint64 `json:"last_retained_lsn"`
	SegmentCount      int    `json:"segment_count"`
	DiskSnapshotCount int    `json:"disk_snapshot_count"`
	MaxDiskSnapshots  int    `json:"max_disk_snapshots"`
}

type clusterAdminSnapshotCatalogResponse struct {
	Snapshots []struct {
		FileName string `json:"file_name"`
	} `json:"snapshots"`
}

type clusterAdminFailoverHistoryResponse struct {
	Transitions []clusterFailoverTransition `json:"transitions"`
}

type clusterDiagnosticsNode struct {
	Endpoint           string   `json:"endpoint"`
	NodeID             string   `json:"node_id,omitempty"`
	Status             string   `json:"status"`
	Ready              bool     `json:"ready"`
	Live               bool     `json:"live"`
	RaftRole           string   `json:"raft_role,omitempty"`
	LeaderID           string   `json:"leader_id,omitempty"`
	CurrentTerm        uint64   `json:"current_term,omitempty"`
	LastDurableLSN     uint64   `json:"last_durable_lsn"`
	Reasons            []string `json:"reasons,omitempty"`
	HeadLSN            uint64   `json:"head_lsn,omitempty"`
	OldestRetainedLSN  uint64   `json:"oldest_retained_lsn,omitempty"`
	LastRetainedLSN    uint64   `json:"last_retained_lsn,omitempty"`
	SegmentCount       int      `json:"segment_count,omitempty"`
	DiskSnapshotCount  int      `json:"disk_snapshot_count,omitempty"`
	SnapshotCatalogLen int      `json:"snapshot_catalog_len,omitempty"`
	MaxDiskSnapshots   int      `json:"max_disk_snapshots,omitempty"`
	Error              string   `json:"error,omitempty"`
}

type clusterFailoverTransition struct {
	Phase     string `json:"phase"`
	GroupName string `json:"group_name"`
	Term      uint64 `json:"term"`
	NodeID    string `json:"node_id"`
}

type clusterDiagnosticsSummary struct {
	ReachableNodes      int    `json:"reachable_nodes"`
	ReadyNodes          int    `json:"ready_nodes"`
	TotalSegments       int    `json:"total_segments"`
	TotalSnapshots      int    `json:"total_snapshots"`
	HighestDurableLSN   uint64 `json:"highest_durable_lsn"`
	WorstReplicationLag uint64 `json:"worst_replication_lag"`
}

type clusterDiagnosticsResponse struct {
	EngineStats     *api.EngineStatsResponse    `json:"engine_stats,omitempty"`
	AdminNodes      []clusterDiagnosticsNode    `json:"admin_nodes"`
	FailoverHistory []clusterFailoverTransition `json:"failover_history,omitempty"`
	Summary         clusterDiagnosticsSummary   `json:"summary"`
}

// startup is called by Wails when the app window is ready; stores the lifetime context.
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	go a.startClusterWatcher(ctx)
}

// getPeers returns a snapshot of the current peer engine list (thread-safe).
func (a *App) getPeers() []*engineClient {
	a.peersMu.RLock()
	defer a.peersMu.RUnlock()
	out := make([]*engineClient, len(a.peerEngines))
	copy(out, a.peerEngines)
	return out
}

// getLeaderClient returns the known active leader engine client. Falls back to
// a.engine when no leader has been detected yet (standalone or first start).
func (a *App) getLeaderClient() *engineClient {
	a.peersMu.RLock()
	defer a.peersMu.RUnlock()
	if a.leaderClient != nil {
		return a.leaderClient
	}
	return a.engine
}

// bootstrapViaParameterStatus dials each addr with a raw pgx connection (no
// pool) and reads the asql_cluster_leader and asql_cluster_peers ParameterStatus
// messages emitted by the server during the pgwire startup handshake.
//
// It returns as soon as one addr responds successfully, giving back the leader
// pgwire address and the full list of known peer pgwire addresses. Both the
// leader and all peers are normalized (bare ":port" → "127.0.0.1:port").
//
// This fallback lets the Studio recover cluster topology when all pool
// connections to the current leader are dead: any surviving follower gossips
// the full topology on every handshake.
func (a *App) bootstrapViaParameterStatus(ctx context.Context, addrs []string) (leaderAddr string, peers []string) {
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addr = normalizeAddr(addr)
		if addr == "" {
			continue
		}
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}

		connStr := buildConnStr(addr, a.engine.password)
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			continue
		}
		raw := conn.PgConn()
		leader := normalizeAddr(raw.ParameterStatus("asql_cluster_leader"))
		peersRaw := raw.ParameterStatus("asql_cluster_peers")
		_ = conn.Close(ctx)

		if leader == "" && peersRaw == "" {
			continue // standalone node, no cluster info
		}
		var peerList []string
		for _, p := range strings.Split(peersRaw, ",") {
			if norm := normalizeAddr(strings.TrimSpace(p)); norm != "" {
				peerList = append(peerList, norm)
			}
		}
		return leader, peerList
	}
	return "", nil
}

// normalizeAddr converts bare ":port" addresses to "127.0.0.1:port" so that
// pgx can connect to them reliably. Addresses that already contain a host are
// returned unchanged.
func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

// syncNewPeers asks any reachable cluster node for the full member list,
// backfills nodeID on existing clients, and adds newly discovered nodes.
// It tries the known leader first, then a.engine, then remaining peers so
// topology discovery survives a leader crash.
func (a *App) syncNewPeers(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Build deduplicated candidate list: leader first, then engine, then rest.
	a.peersMu.RLock()
	candidates := make([]*engineClient, 0, len(a.peerEngines)+1)
	if a.leaderClient != nil {
		candidates = append(candidates, a.leaderClient)
	}
	if a.leaderClient != a.engine {
		candidates = append(candidates, a.engine)
	}
	for _, ec := range a.peerEngines {
		if ec != a.leaderClient && ec != a.engine {
			candidates = append(candidates, ec)
		}
	}
	a.peersMu.RUnlock()

	var members []ClusterMember
	var err error
	for _, ec := range candidates {
		members, err = ec.ClusterMembers(probeCtx)
		if err == nil {
			break
		}
	}
	if err != nil {
		// All pool-based candidates failed. Fall back to raw ParameterStatus
		// discovery: open a one-shot pgx connection to each known addr and read
		// asql_cluster_leader / asql_cluster_peers from the handshake. This works
		// even when the only live nodes are followers — any node gossips the full
		// topology in its startup ParameterStatus.
		var knownAddrs []string
		a.peersMu.RLock()
		for _, ec := range a.peerEngines {
			knownAddrs = append(knownAddrs, ec.addr)
		}
		if a.engine != nil {
			knownAddrs = append(knownAddrs, a.engine.addr)
		}
		a.peersMu.RUnlock()
		leaderAddr, peerAddrs := a.bootstrapViaParameterStatus(probeCtx, knownAddrs)
		if leaderAddr == "" && len(peerAddrs) == 0 {
			return // still nothing reachable
		}
		a.logger.Info("syncNewPeers: recovered topology via ParameterStatus",
			slog.String("leader", leaderAddr),
			slog.Any("peers", peerAddrs),
		)
		// Re-run pool-based sync after adding peers discovered this way.
		// We'll just add the new addresses directly.
		a.peersMu.Lock()
		known2 := make(map[string]struct{}, len(a.peerEngines))
		for _, ec := range a.peerEngines {
			known2[ec.addr] = struct{}{}
		}
		token := a.engine.password
		for _, addr := range peerAddrs {
			addr = normalizeAddr(addr)
			if addr == "" {
				continue
			}
			if _, found := known2[addr]; found {
				continue
			}
			if addr == a.engine.addr {
				continue
			}
			a.logger.Info("syncNewPeers(param): adding peer", slog.String("addr", addr))
			ec := newEngineClient(addr, token)
			a.peerEngines = append(a.peerEngines, ec)
			known2[addr] = struct{}{}
		}
		a.peersMu.Unlock()
		return
	}

	a.peersMu.Lock()
	defer a.peersMu.Unlock()

	// Build pgwire→nodeID look-up from fresh member list.
	// Normalize all addresses (e.g. ":5434" → "127.0.0.1:5434") so that
	// the map keys match the engineClient.addr values, which are always
	// normalized on creation via newEngineClient.
	memberNodeIDs := make(map[string]string, len(members))
	for _, m := range members {
		if m.PgwireAddress != "" && m.NodeID != "" {
			memberNodeIDs[normalizeAddr(m.PgwireAddress)] = m.NodeID
		}
	}

	// Backfill nodeID on a.engine and existing peers that don't have it yet.
	if a.engine.nodeID == "" {
		if nid, ok := memberNodeIDs[a.engine.addr]; ok {
			a.engine.nodeID = nid
		}
	}
	for _, ec := range a.peerEngines {
		if ec.nodeID == "" {
			if nid, ok := memberNodeIDs[ec.addr]; ok {
				ec.nodeID = nid
			}
		}
	}

	// Build known-addr set using normalized addresses so we don't add duplicates.
	known := make(map[string]struct{}, len(a.peerEngines))
	for _, ec := range a.peerEngines {
		known[ec.addr] = struct{}{}
	}

	token := a.engine.password
	for _, m := range members {
		if m.PgwireAddress == "" {
			continue // static peer — no pgwire address advertised
		}
		normAddr := normalizeAddr(m.PgwireAddress)
		if _, found := known[normAddr]; found {
			continue // already tracked
		}
		if m.IsSelf {
			continue // skip self (a.engine already handles it)
		}
		a.logger.Info("cluster watcher: discovered new node",
			slog.String("node_id", m.NodeID),
			slog.String("pgwire", normAddr),
		)
		ec := newEngineClient(normAddr, token)
		ec.nodeID = m.NodeID
		a.peerEngines = append(a.peerEngines, ec)
	}
}

// syncCurrentLeader queries every reachable peer for its Raft role via
// SHOW asql_node_role, and updates a.leaderClient to point to the node that
// self-reports as "leader".  This uses the Raft-authoritative state machine
// rather than the heartbeat lease table, so it survives Raft leader elections.
func (a *App) syncCurrentLeader(ctx context.Context) {
	if len(a.getPeers()) == 0 {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	type roleResult struct {
		ec  *engineClient
		res nodeRoleResult
		err error
	}
	peers := a.getPeers()
	ch := make(chan roleResult, len(peers))
	for _, ec := range peers {
		ec := ec
		go func() {
			res, err := ec.NodeRole(probeCtx)
			ch <- roleResult{ec: ec, res: res, err: err}
		}()
	}

	var leaderEC *engineClient
	for range peers {
		r := <-ch
		if r.err != nil {
			a.logger.Debug("syncCurrentLeader: peer unreachable",
				slog.String("addr", r.ec.addr), slog.String("error", r.err.Error()))
			continue
		}
		a.logger.Debug("syncCurrentLeader: peer role",
			slog.String("addr", r.ec.addr), slog.String("role", r.res.Role))
		if r.res.Role == "leader" {
			leaderEC = r.ec
		}
	}

	if leaderEC == nil {
		a.logger.Debug("syncCurrentLeader: no leader self-reported across all peers")
		return
	}

	a.peersMu.Lock()
	defer a.peersMu.Unlock()
	if leaderEC != a.leaderClient {
		a.logger.Info("cluster watcher: leader updated",
			slog.String("addr", leaderEC.addr),
			slog.String("node_id", leaderEC.nodeID),
		)
		a.leaderClient = leaderEC
	}
}

// startClusterWatcher runs as a goroutine for the lifetime of the app window.
// It polls all configured peer engines at a tight interval (~100 ms) and emits
// the "cluster:node-status" Wails event whenever any node's LSN changes, or as
// a heartbeat every 2 s to keep the UI current after reconnects.
//
// Peers are re-read on every tick (via getPeers()) so that nodes discovered
// after startup (hot-join) are included automatically.
// Every ~5 s a separate call to syncNewPeers() polls the leader for the
// complete member list and adds unknown nodes to peerEngines.
func (a *App) startClusterWatcher(ctx context.Context) {
	// Check that we have at least one probe target.
	initProbes := a.getPeers()
	if len(initProbes) == 0 {
		if a.followerEngine != nil {
			a.peersMu.Lock()
			a.peerEngines = []*engineClient{a.engine, a.followerEngine}
			a.peersMu.Unlock()
		} else if a.engine != nil {
			a.peersMu.Lock()
			a.peerEngines = []*engineClient{a.engine}
			a.peersMu.Unlock()
		} else {
			return
		}
	}

	const (
		pollInterval       = 100 * time.Millisecond
		heartbeatInterval  = 2 * time.Second
		syncPeersInterval  = 2 * time.Second // re-discover peers and leader every 2 s
		leaderSyncDebounce = 1 * time.Second // min gap between leader-unreachable re-syncs
	)

	// Use map[addr]lsn so new peers slot in cleanly without index alignment.
	prevLSNs := make(map[string]uint64)
	lastEmit := time.Time{}
	lastSync := time.Time{}
	lastLeaderFailSync := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}

		// Discover new peers from the leader every syncPeersInterval.
		if time.Since(lastSync) >= syncPeersInterval {
			a.syncNewPeers(ctx)
			a.syncCurrentLeader(ctx)
			lastSync = time.Now()
		}

		probes := a.getPeers() // always re-read — may have grown
		if len(probes) == 0 {
			continue
		}

		type result struct {
			addr string
			lsn  uint64
			err  error
		}
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ch := make(chan result, len(probes))
		for _, ec := range probes {
			ec := ec
			go func() {
				lsn, err := a.fetchLastLSN(probeCtx, ec)
				ch <- result{addr: ec.addr, lsn: lsn, err: err}
			}()
		}
		currLSNs := make(map[string]uint64, len(probes))
		currErrs := make(map[string]error, len(probes))
		for range probes {
			r := <-ch
			currLSNs[r.addr] = r.lsn
			currErrs[r.addr] = r.err
		}
		cancel()

		// If the current leader is unreachable, trigger a fast leader re-sync so
		// getLeaderClient() points to the new leader before the next UI paint.
		// Debounced to leaderSyncDebounce to avoid stampede on sustained failure.
		if lc := a.getLeaderClient(); currErrs[lc.addr] != nil {
			if time.Since(lastLeaderFailSync) >= leaderSyncDebounce {
				a.syncCurrentLeader(ctx)
				lastLeaderFailSync = time.Now()
			}
		}

		// Determine if anything changed.
		changed := false
		for _, ec := range probes {
			if currLSNs[ec.addr] != prevLSNs[ec.addr] {
				changed = true
				prevLSNs[ec.addr] = currLSNs[ec.addr]
			}
		}
		if !changed && time.Since(lastEmit) < heartbeatInterval {
			continue
		}

		// Build the payload.
		leaderEC := a.getLeaderClient()
		leaderLSN := currLSNs[leaderEC.addr]
		nodes := make([]clusterNodeInfo, 0, len(probes))
		for _, ec := range probes {
			role := "follower"
			if ec == leaderEC {
				role = "leader"
			}
			displayID := ec.addr
			if ec.nodeID != "" {
				displayID = ec.nodeID
			}
			if currErrs[ec.addr] != nil {
				nodes = append(nodes, clusterNodeInfo{
					NodeID:    displayID,
					Addr:      ec.addr,
					Role:      role,
					Reachable: false,
				})
				continue
			}
			lag := uint64(0)
			if role == "follower" && leaderLSN > currLSNs[ec.addr] {
				lag = leaderLSN - currLSNs[ec.addr]
			}
			nodes = append(nodes, clusterNodeInfo{
				NodeID:    displayID,
				Addr:      ec.addr,
				Role:      role,
				LSN:       currLSNs[ec.addr],
				Lag:       lag,
				Reachable: true,
			})
		}

		payload := clusterNodeStatusResponse{Nodes: nodes}
		runtime.EventsEmit(a.ctx, "cluster:node-status", payload)
		lastEmit = time.Now()
	}
}

func (a *App) reqCtx() (context.Context, context.CancelFunc) {
	if a.ctx == nil {
		return context.WithTimeout(context.Background(), 5*time.Minute)
	}
	return context.WithTimeout(a.ctx, 5*time.Minute)
}

// ── Health ────────────────────────────────────────────────────────────────────

// Health returns a simple liveness indicator.
func (a *App) Health() (map[string]interface{}, error) {
	return map[string]interface{}{"status": "ok"}, nil
}

// ── Domains ───────────────────────────────────────────────────────────────────

// Domains returns the list of domain names from the engine schema snapshot.
func (a *App) Domains() (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	snapshot, err := a.engine.SchemaSnapshot(ctx, &api.SchemaSnapshotRequest{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(snapshot.Domains))
	for _, d := range snapshot.Domains {
		if name := strings.TrimSpace(d.Name); name != "" {
			names = append(names, name)
		}
	}
	return map[string]interface{}{"domains": names}, nil
}

// ── Transactions ──────────────────────────────────────────────────────────────

// Begin opens a new transaction on the given domains.
func (a *App) Begin(req beginRequest) (map[string]interface{}, error) {
	if len(req.Domains) == 0 {
		return nil, fmt.Errorf("domains are required")
	}
	if strings.TrimSpace(req.Mode) == "" {
		req.Mode = "domain"
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.BeginTx(ctx, &api.BeginTxRequest{Mode: req.Mode, Domains: req.Domains})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// Execute runs a single SQL statement inside an open transaction.
func (a *App) Execute(req executeRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.TxID) == "" || strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("tx_id and sql are required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.Execute(ctx, &api.ExecuteRequest{TxID: req.TxID, SQL: req.SQL})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// ExecuteBatch runs multiple SQL statements inside an open transaction.
func (a *App) ExecuteBatch(req executeBatchRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.TxID) == "" || len(req.Statements) == 0 {
		return nil, fmt.Errorf("tx_id and statements are required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.ExecuteBatch(ctx, &api.ExecuteBatchRequest{TxID: req.TxID, Statements: req.Statements})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// Commit commits the transaction identified by tx_id.
func (a *App) Commit(req txRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.TxID) == "" {
		return nil, fmt.Errorf("tx_id is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.CommitTx(ctx, &api.CommitTxRequest{TxID: req.TxID})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// Rollback aborts the transaction identified by tx_id.
func (a *App) Rollback(req txRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.TxID) == "" {
		return nil, fmt.Errorf("tx_id is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.RollbackTx(ctx, &api.RollbackTxRequest{TxID: req.TxID})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// ── Queries ───────────────────────────────────────────────────────────────────

// TimeTravel executes a SQL query at a specific LSN or logical timestamp.
func (a *App) TimeTravel(req timeTravelRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("sql is required")
	}
	if req.LSN == 0 && req.LogicalTimestamp == 0 {
		return nil, fmt.Errorf("lsn or logical_timestamp is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.TimeTravelQuery(ctx, &api.TimeTravelQueryRequest{
		SQL:              req.SQL,
		Domains:          req.Domains,
		LSN:              req.LSN,
		LogicalTimestamp: req.LogicalTimestamp,
	})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// RowHistory returns the change history for rows matching a SQL predicate.
func (a *App) RowHistory(req rowHistoryRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("sql is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.RowHistory(ctx, &api.RowHistoryRequest{
		SQL:     req.SQL,
		Domains: req.Domains,
	})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// EntityVersionHistory returns entity-level version history for a root PK.
func (a *App) EntityVersionHistory(req entityVersionHistoryRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.Domain) == "" {
		return nil, fmt.Errorf("domain is required")
	}
	if strings.TrimSpace(req.EntityName) == "" {
		return nil, fmt.Errorf("entity_name is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.EntityVersionHistory(ctx, &api.EntityVersionHistoryRequest{
		Domain:     req.Domain,
		EntityName: req.EntityName,
		RootPK:     req.RootPK,
	})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// TemporalLookup resolves the current helper surface for a specific row/entity context.
func (a *App) TemporalLookup(req temporalLookupRequest) (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	client := a.getLeaderClient()
	if client == nil {
		return nil, fmt.Errorf("leader client is not available")
	}
	resp, err := client.TemporalLookup(ctx, req)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"status":                  "OK",
		"current_lsn":             nil,
		"row_lsn":                 nil,
		"resolve_reference":       nil,
		"resolve_reference_error": strings.TrimSpace(resp.ResolveReferenceErr),
		"entity_version":          nil,
		"entity_head_lsn":         nil,
		"entity_version_lsn":      nil,
	}
	if resp.CurrentLSN != nil {
		result["current_lsn"] = *resp.CurrentLSN
	}
	if resp.RowLSN != nil {
		result["row_lsn"] = *resp.RowLSN
	}
	if resp.ResolveReference != nil {
		result["resolve_reference"] = *resp.ResolveReference
	}
	if resp.EntityVersion != nil {
		result["entity_version"] = *resp.EntityVersion
	}
	if resp.EntityHeadLSN != nil {
		result["entity_head_lsn"] = *resp.EntityHeadLSN
	}
	if resp.EntityVersionLSN != nil {
		result["entity_version_lsn"] = *resp.EntityVersionLSN
	}
	return result, nil
}

// Explain returns the query execution plan for a SQL statement.
func (a *App) Explain(req explainRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("sql is required")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.ExplainQuery(ctx, &api.ExplainQueryRequest{
		SQL:     req.SQL,
		Domains: req.Domains,
	})
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// ReadQuery executes a read-only SQL query with optional consistency routing.
func (a *App) ReadQuery(req readQueryRequest) (*readQueryResponse, error) {
	if strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("sql is required")
	}
	if len(req.Domains) == 0 {
		return nil, fmt.Errorf("domains are required")
	}

	policy := normalizeReadConsistency(req.Consistency)
	ctx, cancel := a.reqCtx()
	defer cancel()

	leaderLSN, err := a.fetchLastLSN(ctx, a.engine)
	if err != nil {
		return nil, err
	}

	followerLSN := uint64(0)
	hasFollower := a.followerEngine != nil
	followerUnavailable := false
	if hasFollower {
		followerLSN, err = a.fetchLastLSN(ctx, a.followerEngine)
		if err != nil {
			followerLSN = 0
			hasFollower = false
			followerUnavailable = true
		}
	}

	decision := DecideReadRoute(ReadRouteInput{
		Consistency: policy,
		LeaderLSN:   leaderLSN,
		FollowerLSN: followerLSN,
		HasFollower: hasFollower,
		MaxLag:      req.MaxLag,
	})
	a.recordReadRoutingMetrics(readRoutingMetricInput{
		Consistency:         policy,
		Decision:            decision,
		HasFollower:         hasFollower,
		MaxLag:              req.MaxLag,
		FollowerUnavailable: followerUnavailable,
	})

	target := a.engine
	asOfLSN := leaderLSN
	if decision.Route == ReadRouteFollower {
		target = a.followerEngine
		asOfLSN = followerLSN
		if asOfLSN > leaderLSN {
			asOfLSN = leaderLSN
		}
	}

	ttResp, err := target.TimeTravelQuery(ctx, &api.TimeTravelQueryRequest{
		SQL:     req.SQL,
		Domains: req.Domains,
		LSN:     asOfLSN,
	})
	if err != nil {
		return nil, err
	}

	return &readQueryResponse{
		Status:      ttResp.Status,
		Rows:        ttResp.Rows,
		Route:       string(decision.Route),
		Consistency: string(policy),
		AsOfLSN:     asOfLSN,
		LeaderLSN:   leaderLSN,
		FollowerLSN: followerLSN,
		Lag:         decision.Lag,
	}, nil
}

// ── Stats ─────────────────────────────────────────────────────────────────────

// ReadRoutingStats returns routing decision counters.
func (a *App) ReadRoutingStats() (map[string]interface{}, error) {
	counts := map[string]uint64{}
	if a.routingStats != nil {
		counts = a.routingStats.snapshot()
	}
	return map[string]interface{}{"counts": counts}, nil
}

// ScanStrategyStats returns scan strategy statistics from the engine.
func (a *App) ScanStrategyStats() (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.ScanStrategyStats(ctx)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// EngineStats returns general engine runtime metrics.
func (a *App) EngineStats() (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.EngineStats(ctx)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// TimelineCommits returns commits from the WAL timeline.
func (a *App) TimelineCommits(req api.TimelineCommitsRequest) (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	resp, err := a.engine.TimelineCommits(ctx, &req)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// ── Schema ────────────────────────────────────────────────────────────────────

// SchemaDDL generates a DDL script from a schema descriptor payload.
func (a *App) SchemaDDL(req schemaDDLRequest) (map[string]interface{}, error) {
	resp, err := BuildSchemaDDLScript(req)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// SchemaLoadBaseline loads the current schema from the engine as a DDL baseline.
func (a *App) SchemaLoadBaseline(req schemaLoadBaselineRequest) (map[string]interface{}, error) {
	domain, err := normalizeSchemaDomain(req.Domain)
	if err != nil {
		return nil, err
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	snapshot, err := invoker.SchemaSnapshot(ctx, &api.SchemaSnapshotRequest{Domains: []string{domain}})
	if err != nil {
		return nil, err
	}

	baseline, err := schemaSnapshotToDDL(snapshot, domain)
	if err != nil {
		return nil, err
	}
	return structToMap(schemaLoadBaselineResponse{Status: "BASELINE_LOADED", Baseline: baseline})
}

// SchemaLoadAllBaselines loads DDL baselines for every known domain.
func (a *App) SchemaLoadAllBaselines() (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	snapshot, err := invoker.SchemaSnapshot(ctx, &api.SchemaSnapshotRequest{})
	if err != nil {
		return nil, err
	}

	baselines, err := schemaSnapshotAllDomainsToDDL(snapshot)
	if err != nil {
		return nil, err
	}
	return structToMap(schemaLoadAllBaselinesResponse{Status: "OK", Baselines: baselines})
}

// SchemaDiff computes the diff between two DDL schemas.
func (a *App) SchemaDiff(req schemaDiffRequest) (map[string]interface{}, error) {
	resp, err := BuildSchemaDiff(req.Base, req.Target)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// SchemaApply generates and applies a DDL plan to the engine.
func (a *App) SchemaApply(req schemaDDLRequest) (map[string]interface{}, error) {
	domain, err := normalizeSchemaDomain(req.Domain)
	if err != nil {
		return nil, err
	}

	plan, err := BuildSchemaDDLScript(req)
	if err != nil {
		return nil, err
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	resp, err := applySchemaDDLPlan(ctx, invoker, domain, plan)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// schemaApplyStatementsRequest carries raw DDL statements to execute directly.
type schemaApplyStatementsRequest struct {
	Domain     string   `json:"domain"`
	Statements []string `json:"statements"`
}

// SchemaApplyStatements executes a list of raw DDL statements inside a schema
// transaction. This is used by the schema designer when the user clicks
// "Apply" on individual or batched DDL statements.
func (a *App) SchemaApplyStatements(req schemaApplyStatementsRequest) (map[string]interface{}, error) {
	domain, err := normalizeSchemaDomain(req.Domain)
	if err != nil {
		return nil, err
	}
	if len(req.Statements) == 0 {
		return nil, fmt.Errorf("statements are required")
	}

	plan := schemaDDLResponse{Statements: req.Statements}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	resp, err := applySchemaDDLPlan(ctx, invoker, domain, plan)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// SchemaApplySafeDiff applies only safe (non-destructive) diff operations.
func (a *App) SchemaApplySafeDiff(req schemaDiffRequest) (map[string]interface{}, error) {
	diff, err := BuildSchemaDiff(req.Base, req.Target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	resp, err := applySafeSchemaDiff(ctx, invoker, diff)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

// SchemaTables returns the tables (and primary key columns) for a domain.
func (a *App) SchemaTables(domain string) (map[string]interface{}, error) {
	if domain == "" {
		domain = "default"
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	snapshot, err := invoker.SchemaSnapshot(ctx, &api.SchemaSnapshotRequest{Domains: []string{domain}})
	if err != nil {
		return nil, err
	}

	tables := make([]schemaTableInfo, 0)
	for _, d := range snapshot.Domains {
		if d.Name != domain {
			continue
		}
		for _, t := range d.Tables {
			var pks []string
			for _, c := range t.Columns {
				if c.PrimaryKey {
					pks = append(pks, c.Name)
				}
			}
			tables = append(tables, schemaTableInfo{Name: t.Name, PKColumns: pks})
		}
	}

	return structToMap(schemaTablesResponse{Tables: tables})
}

// ── Replication ───────────────────────────────────────────────────────────────

// ReplicationLastLSN returns the last known LSN from the leader.
func (a *App) ReplicationLastLSN() (map[string]interface{}, error) {
	ctx, cancel := a.reqCtx()
	defer cancel()

	lsn, err := a.fetchLastLSN(ctx, a.engine)
	if err != nil {
		return nil, err
	}
	return structToMap(&api.LastLSNResponse{LSN: lsn})
}

// ReplicationLag returns the lag between leader and follower LSNs.
func (a *App) ReplicationLag() (map[string]interface{}, error) {
	if a.followerEngine == nil {
		return nil, fmt.Errorf("follower-engine-endpoint is required for lag")
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	leaderLSN, err := a.fetchLastLSN(ctx, a.engine)
	if err != nil {
		return nil, err
	}
	followerLSN, err := a.fetchLastLSN(ctx, a.followerEngine)
	if err != nil {
		return nil, err
	}

	resp := replicationLagResponse{LeaderLSN: leaderLSN, FollowerLSN: followerLSN}
	if resp.LeaderLSN > resp.FollowerLSN {
		resp.Lag = resp.LeaderLSN - resp.FollowerLSN
	}
	return structToMap(resp)
}

// ── Cluster ───────────────────────────────────────────────────────────────────

// ClusterGroups returns the configured domain-group names.
func (a *App) ClusterGroups() (map[string]interface{}, error) {
	groups := a.clusterGroups
	if groups == nil {
		groups = []string{}
	}
	return map[string]interface{}{"groups": groups}, nil
}

// ClusterStatus returns leadership state for a comma-separated list of groups.
// Uses Raft-authoritative SHOW commands instead of the heartbeat lease table,
// so leader identity is always accurate regardless of lease state.
func (a *App) ClusterStatus(groups string) (map[string]interface{}, error) {
	var groupNames []string
	if g := strings.TrimSpace(groups); g != "" {
		for _, part := range strings.Split(g, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				groupNames = append(groupNames, trimmed)
			}
		}
	} else {
		groupNames = a.clusterGroups
	}

	if len(groupNames) == 0 {
		return structToMap(clusterStatusResponse{Groups: []clusterGroupStatus{}})
	}

	// Probe all peers in parallel for their Raft role.
	ctx, cancel := context.WithTimeout(a.reqCtx0(), 4*time.Second)
	defer cancel()

	peers := a.getPeers()
	if len(peers) == 0 {
		peers = []*engineClient{a.engine}
	}

	type roleResult struct {
		ec  *engineClient
		res nodeRoleResult
		lsn uint64
		err error
	}
	ch := make(chan roleResult, len(peers))
	for _, ec := range peers {
		ec := ec
		go func() {
			res, err := ec.NodeRole(ctx)
			lsn, _ := a.fetchLastLSN(ctx, ec)
			ch <- roleResult{ec: ec, res: res, lsn: lsn, err: err}
		}()
	}
	var leaderRole nodeRoleResult
	var leaderLSN uint64
	for range peers {
		r := <-ch
		if r.err == nil && r.res.Role == "leader" {
			leaderRole = r.res
			leaderLSN = r.lsn
		}
	}

	results := make([]clusterGroupStatus, 0, len(groupNames))
	for _, group := range groupNames {
		leaderID := leaderRole.RaftLeaderID
		if leaderID == "" {
			leaderID = leaderRole.NodeID
		}
		results = append(results, clusterGroupStatus{
			Group:        group,
			LeaderID:     leaderID,
			Term:         leaderRole.RaftTerm,
			FencingToken: group + ":" + fmt.Sprintf("%d", leaderRole.RaftTerm),
			LeaseActive:  leaderRole.Role == "leader",
			LastLSN:      leaderLSN,
		})
	}
	return structToMap(clusterStatusResponse{Groups: results})
}

// ClusterNodeStatus returns per-node LSN and reachability for every cluster
// endpoint configured on the studio (-peer-endpoints). Falls back to the
// {leader, follower} pair for backwards-compat single/two-node setups.
//
// All nodes are probed in parallel with a 3 s deadline so a single unreachable
// node never blocks the UI refresh.
// Role is determined from SHOW asql_node_role (Raft-authoritative) so the
// topology diagram always reflects the actual elected leader, not a cached pointer.
func (a *App) ClusterNodeStatus() (map[string]interface{}, error) {
	// Short timeout per probe so the UI never stalls waiting for a dead node.
	ctx, cancel := context.WithTimeout(a.reqCtx0(), 3*time.Second)
	defer cancel()

	// Determine which engines to probe.
	probes := a.getPeers()
	if len(probes) == 0 {
		probes = []*engineClient{a.engine}
		if a.followerEngine != nil {
			probes = append(probes, a.followerEngine)
		}
	}

	type probeResult struct {
		idx     int
		lsn     uint64
		role    nodeRoleResult
		lsnErr  error
		roleErr error
	}
	ch := make(chan probeResult, len(probes))
	for i, ec := range probes {
		i, ec := i, ec
		go func() {
			lsn, lsnErr := a.fetchLastLSN(ctx, ec)
			role, roleErr := ec.NodeRole(ctx)
			ch <- probeResult{idx: i, lsn: lsn, role: role, lsnErr: lsnErr, roleErr: roleErr}
		}()
	}
	lsns := make([]uint64, len(probes))
	roles := make([]nodeRoleResult, len(probes))
	lsnErrs := make([]error, len(probes))
	for range probes {
		r := <-ch
		lsns[r.idx] = r.lsn
		roles[r.idx] = r.role
		lsnErrs[r.idx] = r.lsnErr
		// roleErr is non-fatal: reachability is determined by LSN probe only.
	}

	// Determine leader LSN from the node that self-reports as Raft leader.
	// Fall back to the cached leaderClient pointer if no node self-reports.
	leaderEC := a.getLeaderClient()
	leaderLSN := uint64(0)
	for i, r := range roles {
		if r.Role == "leader" && lsnErrs[i] == nil {
			leaderLSN = lsns[i]
			break
		}
	}
	// Pointer-based fallback for leader LSN when Raft SHOW is unavailable.
	if leaderLSN == 0 {
		for i, ec := range probes {
			if ec == leaderEC && lsnErrs[i] == nil {
				leaderLSN = lsns[i]
				break
			}
		}
	}

	nodes := make([]clusterNodeInfo, 0, len(probes))
	for i, ec := range probes {
		// A node is unreachable when the LSN probe fails (can't connect at all).
		unreachable := lsnErrs[i] != nil

		// Role from Raft SHOW; fall back to pointer comparison with leaderClient.
		role := roles[i].Role
		if role == "" {
			if ec == leaderEC {
				role = "leader"
			} else {
				role = "follower"
			}
		}

		nodeID := ec.addr
		if ec.nodeID != "" {
			nodeID = ec.nodeID
		}
		if unreachable {
			a.logger.Debug("cluster node status: unreachable",
				slog.String("addr", nodeID), slog.String("error", lsnErrs[i].Error()))
			nodes = append(nodes, clusterNodeInfo{
				NodeID:    nodeID,
				Addr:      ec.addr,
				Role:      role,
				Reachable: false,
			})
			continue
		}
		lag := uint64(0)
		if role != "leader" && leaderLSN > lsns[i] {
			lag = leaderLSN - lsns[i]
		}
		nodes = append(nodes, clusterNodeInfo{
			NodeID:    nodeID,
			Addr:      ec.addr,
			Role:      role,
			LSN:       lsns[i],
			Lag:       lag,
			Reachable: true,
		})
	}

	return structToMap(clusterNodeStatusResponse{Nodes: nodes})
}

// ClusterDiagnostics returns rich cluster/operator diagnostics by combining
// engine stats with admin HTTP health/retention/failover surfaces.
func (a *App) ClusterDiagnostics() (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(a.reqCtx0(), 4*time.Second)
	defer cancel()

	var engineStats *api.EngineStatsResponse
	if a.engine != nil {
		stats, err := a.engine.EngineStats(ctx)
		if err == nil {
			engineStats = stats
		}
	}

	adminEndpoints := make([]string, 0, len(a.adminEndpoints))
	seen := map[string]struct{}{}
	for _, endpoint := range a.adminEndpoints {
		trimmed := strings.TrimSpace(endpoint)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		adminEndpoints = append(adminEndpoints, trimmed)
	}

	nodes := make([]clusterDiagnosticsNode, 0, len(adminEndpoints))
	if len(adminEndpoints) > 0 {
		type result struct {
			node clusterDiagnosticsNode
		}
		ch := make(chan result, len(adminEndpoints))
		for _, endpoint := range adminEndpoints {
			endpoint := endpoint
			go func() {
				node := clusterDiagnosticsNode{Endpoint: endpoint, Status: "unreachable"}

				var health clusterAdminHealthResponse
				if err := a.fetchClusterAdminJSON(ctx, endpoint, "/api/v1/health", &health); err != nil {
					node.Error = err.Error()
					ch <- result{node: node}
					return
				}

				node.NodeID = health.NodeID
				node.Status = health.Status
				node.Ready = health.Ready
				node.Live = health.Live
				node.RaftRole = health.RaftRole
				node.LeaderID = health.LeaderID
				node.CurrentTerm = health.CurrentTerm
				node.LastDurableLSN = health.LastDurableLSN
				node.Reasons = health.Reasons

				var retention clusterAdminRetentionResponse
				if err := a.fetchClusterAdminJSON(ctx, endpoint, "/api/v1/wal-retention", &retention); err == nil {
					node.HeadLSN = retention.HeadLSN
					node.OldestRetainedLSN = retention.OldestRetainedLSN
					node.LastRetainedLSN = retention.LastRetainedLSN
					node.SegmentCount = retention.SegmentCount
					node.DiskSnapshotCount = retention.DiskSnapshotCount
					node.MaxDiskSnapshots = retention.MaxDiskSnapshots
				}

				var catalog clusterAdminSnapshotCatalogResponse
				if err := a.fetchClusterAdminJSON(ctx, endpoint, "/api/v1/snapshot-catalog", &catalog); err == nil {
					node.SnapshotCatalogLen = len(catalog.Snapshots)
					if node.DiskSnapshotCount == 0 {
						node.DiskSnapshotCount = len(catalog.Snapshots)
					}
				}

				ch <- result{node: node}
			}()
		}

		for range adminEndpoints {
			r := <-ch
			nodes = append(nodes, r.node)
		}
		sort.Slice(nodes, func(i, j int) bool {
			left := nodes[i].NodeID
			if left == "" {
				left = nodes[i].Endpoint
			}
			right := nodes[j].NodeID
			if right == "" {
				right = nodes[j].Endpoint
			}
			return left < right
		})
	}

	failoverHistory := make([]clusterFailoverTransition, 0)
	for _, endpoint := range adminEndpoints {
		var history clusterAdminFailoverHistoryResponse
		if err := a.fetchClusterAdminJSON(ctx, endpoint, "/api/v1/failover-history", &history); err == nil {
			failoverHistory = history.Transitions
			break
		}
	}

	summary := clusterDiagnosticsSummary{}
	for _, node := range nodes {
		if node.Live {
			summary.ReachableNodes++
		}
		if node.Ready {
			summary.ReadyNodes++
		}
		summary.TotalSegments += node.SegmentCount
		summary.TotalSnapshots += node.DiskSnapshotCount
		if node.LastDurableLSN > summary.HighestDurableLSN {
			summary.HighestDurableLSN = node.LastDurableLSN
		}
	}
	for _, node := range nodes {
		if summary.HighestDurableLSN > node.LastDurableLSN {
			lag := summary.HighestDurableLSN - node.LastDurableLSN
			if lag > summary.WorstReplicationLag {
				summary.WorstReplicationLag = lag
			}
		}
	}

	return structToMap(clusterDiagnosticsResponse{
		EngineStats:     engineStats,
		AdminNodes:      nodes,
		FailoverHistory: failoverHistory,
		Summary:         summary,
	})
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (a *App) fetchClusterAdminJSON(ctx context.Context, endpoint, path string, target any) error {
	return a.fetchClusterAdminJSONWithMethod(ctx, endpoint, http.MethodGet, path, nil, target)
}

func (a *App) fetchClusterAdminJSONWithMethod(ctx context.Context, endpoint, method, path string, payload any, target any) error {
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("admin endpoint is required")
	}
	base := strings.TrimSpace(endpoint)
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	url := strings.TrimRight(base, "/") + path
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode %s request: %w", url, err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token := strings.TrimSpace(a.adminToken); token != "" {
		if strings.HasPrefix(strings.ToLower(token), "bearer ") {
			req.Header.Set("Authorization", token)
		} else {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		message := strings.TrimSpace(string(body))
		if message == "" {
			return fmt.Errorf("%s returned %s", url, resp.Status)
		}
		return fmt.Errorf("%s returned %s: %s", url, resp.Status, message)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", url, err)
	}
	return nil
}

func (a *App) primaryAdminEndpoint() (string, error) {
	for _, endpoint := range a.adminEndpoints {
		if trimmed := strings.TrimSpace(endpoint); trimmed != "" {
			return trimmed, nil
		}
	}
	return "", fmt.Errorf("admin endpoint is required")
}

func (a *App) callRecoveryAdmin(path string, request any, target any) error {
	return a.callPrimaryAdmin(http.MethodPost, path, request, target)
}

func (a *App) callPrimaryAdmin(method, path string, request any, target any) error {
	endpoint, err := a.primaryAdminEndpoint()
	if err != nil {
		return err
	}
	return a.fetchClusterAdminJSONWithMethod(a.reqCtx0(), endpoint, method, path, request, target)
}

func (a *App) reqCtx0() context.Context {
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

func (a *App) recoveryDataDir(requested string) string {
	if trimmed := strings.TrimSpace(requested); trimmed != "" {
		return trimmed
	}
	return a.dataDir
}

// structToMap converts any JSON-serialisable struct to map[string]interface{}.
func structToMap(v interface{}) (map[string]interface{}, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (a *App) RecoveryDefaults() (map[string]interface{}, error) {
	return map[string]interface{}{"data_dir": a.dataDir}, nil
}

func (a *App) SecurityListPrincipals() (map[string]interface{}, error) {
	var resp api.ListPrincipalsResponse
	if err := a.callPrimaryAdmin(http.MethodGet, "/api/v1/security/principals", nil, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityBootstrapAdmin(principal, password string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(password) == "" {
		return nil, fmt.Errorf("password is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/bootstrap-admin", api.BootstrapAdminPrincipalRequest{
		Principal: principal,
		Password:  password,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityCreateUser(principal, password string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(password) == "" {
		return nil, fmt.Errorf("password is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/users", api.CreateUserRequest{
		Principal: principal,
		Password:  password,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityCreateRole(principal string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/roles", api.CreateRoleRequest{
		Principal: principal,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityGrantPrivilege(principal, privilege string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(privilege) == "" {
		return nil, fmt.Errorf("privilege is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/privileges/grant", api.GrantPrivilegeRequest{
		Principal: principal,
		Privilege: privilege,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityGrantHistoricalAccess(principal string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/privileges/grant", api.GrantPrivilegeRequest{
		Principal: principal,
		Privilege: string("SELECT_HISTORY"),
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityGrantRole(principal, role string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(role) == "" {
		return nil, fmt.Errorf("role is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/roles/grant", api.GrantRoleRequest{
		Principal: principal,
		Role:      role,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityRevokeRole(principal, role string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(role) == "" {
		return nil, fmt.Errorf("role is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/roles/revoke", api.RevokeRoleRequest{
		Principal: principal,
		Role:      role,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityRevokePrivilege(principal, privilege string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(privilege) == "" {
		return nil, fmt.Errorf("privilege is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/privileges/revoke", api.RevokePrivilegeRequest{
		Principal: principal,
		Privilege: privilege,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecuritySetPassword(principal, password string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(password) == "" {
		return nil, fmt.Errorf("password is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/passwords/set", api.SetPasswordRequest{
		Principal: principal,
		Password:  password,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityDisablePrincipal(principal string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/principals/disable", api.DisablePrincipalRequest{
		Principal: principal,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityEnablePrincipal(principal string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/principals/enable", api.EnablePrincipalRequest{
		Principal: principal,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) SecurityDeletePrincipal(principal string) (map[string]interface{}, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("principal is required")
	}
	var resp api.SecurityMutationResponse
	if err := a.callPrimaryAdmin(http.MethodPost, "/api/v1/security/principals/delete", api.DeletePrincipalRequest{
		Principal: principal,
	}, &resp); err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) RecoveryCreateBackup(dataDir, backupDir string) (map[string]interface{}, error) {
	resolvedDataDir := a.recoveryDataDir(dataDir)
	if resolvedDataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if strings.TrimSpace(backupDir) == "" {
		return nil, fmt.Errorf("backup directory is required")
	}
	var manifest api.BaseBackupManifest
	err := a.callRecoveryAdmin("/api/v1/recovery/backup-create", api.RecoveryCreateBackupRequest{
		DataDir:   resolvedDataDir,
		BackupDir: strings.TrimSpace(backupDir),
	}, &manifest)
	if err != nil {
		return nil, err
	}
	return structToMap(manifest)
}

func (a *App) RecoveryBackupManifest(backupDir string) (map[string]interface{}, error) {
	if strings.TrimSpace(backupDir) == "" {
		return nil, fmt.Errorf("backup directory is required")
	}
	var manifest api.BaseBackupManifest
	err := a.callRecoveryAdmin("/api/v1/recovery/backup-manifest", api.RecoveryBackupManifestRequest{
		BackupDir: strings.TrimSpace(backupDir),
	}, &manifest)
	if err != nil {
		return nil, err
	}
	return structToMap(manifest)
}

func (a *App) RecoveryVerifyBackup(backupDir string) (map[string]interface{}, error) {
	if strings.TrimSpace(backupDir) == "" {
		return nil, fmt.Errorf("backup directory is required")
	}
	var resp api.RecoveryVerifyBackupResponse
	err := a.callRecoveryAdmin("/api/v1/recovery/backup-verify", api.RecoveryVerifyBackupRequest{
		BackupDir: strings.TrimSpace(backupDir),
	}, &resp)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) RecoveryRestoreLSN(backupDir, targetDataDir string, lsn uint64) (map[string]interface{}, error) {
	if strings.TrimSpace(backupDir) == "" {
		return nil, fmt.Errorf("backup directory is required")
	}
	if strings.TrimSpace(targetDataDir) == "" {
		return nil, fmt.Errorf("target data directory is required")
	}
	if lsn == 0 {
		return nil, fmt.Errorf("lsn must be greater than zero")
	}
	var result api.RestoreResult
	err := a.callRecoveryAdmin("/api/v1/recovery/restore-lsn", api.RecoveryRestoreLSNRequest{
		BackupDir:     strings.TrimSpace(backupDir),
		TargetDataDir: strings.TrimSpace(targetDataDir),
		LSN:           lsn,
	}, &result)
	if err != nil {
		return nil, err
	}
	return structToMap(result)
}

func (a *App) RecoveryRestoreTimestamp(backupDir, targetDataDir string, logicalTimestamp uint64) (map[string]interface{}, error) {
	if strings.TrimSpace(backupDir) == "" {
		return nil, fmt.Errorf("backup directory is required")
	}
	if strings.TrimSpace(targetDataDir) == "" {
		return nil, fmt.Errorf("target data directory is required")
	}
	if logicalTimestamp == 0 {
		return nil, fmt.Errorf("logical timestamp must be greater than zero")
	}
	var result api.RestoreResult
	err := a.callRecoveryAdmin("/api/v1/recovery/restore-timestamp", api.RecoveryRestoreTimestampRequest{
		BackupDir:        strings.TrimSpace(backupDir),
		TargetDataDir:    strings.TrimSpace(targetDataDir),
		LogicalTimestamp: logicalTimestamp,
	}, &result)
	if err != nil {
		return nil, err
	}
	return structToMap(result)
}

func (a *App) RecoverySnapshotCatalog(dataDir string) (map[string]interface{}, error) {
	resolvedDataDir := a.recoveryDataDir(dataDir)
	if resolvedDataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	var resp api.RecoverySnapshotCatalogResponse
	err := a.callRecoveryAdmin("/api/v1/recovery/snapshot-catalog", api.RecoverySnapshotCatalogRequest{
		DataDir: resolvedDataDir,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return structToMap(resp)
}

func (a *App) RecoveryWALRetention(dataDir string) (map[string]interface{}, error) {
	resolvedDataDir := a.recoveryDataDir(dataDir)
	if resolvedDataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	var state api.WALRetentionState
	err := a.callRecoveryAdmin("/api/v1/recovery/wal-retention", api.RecoveryWALRetentionRequest{
		DataDir: resolvedDataDir,
	}, &state)
	if err != nil {
		return nil, err
	}
	if state.DataDir == "" {
		state.DataDir = resolvedDataDir
	}
	return structToMap(state)
}

func (a *App) fetchLastLSN(ctx context.Context, client *engineClient) (uint64, error) {
	if client == nil {
		return 0, fmt.Errorf("engine client is required")
	}
	resp, err := client.LastLSN(ctx)
	if err != nil {
		return 0, err
	}
	return resp.LSN, nil
}

// PickFixtureFile shows a native open dialog for fixture JSON files.
func (a *App) PickFixtureFile() (string, error) {
	path, err := runtime.OpenFileDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "Open ASQL fixture",
		Filters: []runtime.FileFilter{{
			DisplayName: "ASQL fixture",
			Pattern:     "*.json",
		}},
	})
	if err != nil || path == "" {
		return "", err
	}
	return path, nil
}

// PickFixtureExportFile shows a native save dialog for exported fixture files.
func (a *App) PickFixtureExportFile(suggestedName string) (string, error) {
	if strings.TrimSpace(suggestedName) == "" {
		suggestedName = "fixture-export-v1.json"
	}
	path, err := runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{
		Title:           "Save ASQL fixture",
		DefaultFilename: suggestedName,
		Filters: []runtime.FileFilter{{
			DisplayName: "ASQL fixture",
			Pattern:     "*.json",
		}},
	})
	if err != nil || path == "" {
		return "", err
	}
	return path, nil
}

// FixtureValidate validates a fixture file, including a deterministic dry run.
func (a *App) FixtureValidate(path string) (map[string]interface{}, error) {
	fixture, err := fixtures.LoadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	ctx, cancel := a.reqCtx()
	defer cancel()
	if err := fixtures.ValidateDryRun(ctx, fixture); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"status": "validated",
		"file":   strings.TrimSpace(path),
		"name":   fixture.Name,
		"steps":  len(fixture.Steps),
	}, nil
}

// FixtureLoad validates then applies a fixture to the current leader endpoint.
func (a *App) FixtureLoad(path string) (map[string]interface{}, error) {
	fixture, err := fixtures.LoadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	ctx, cancel := a.reqCtx()
	defer cancel()
	if err := fixtures.ValidateDryRun(ctx, fixture); err != nil {
		return nil, err
	}
	client := a.getLeaderClient()
	if client == nil {
		return nil, fmt.Errorf("leader client is not available")
	}
	if err := client.ApplyFixture(ctx, fixture); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"status": "loaded",
		"file":   strings.TrimSpace(path),
		"name":   fixture.Name,
		"steps":  len(fixture.Steps),
	}, nil
}

// FixtureExport exports the selected domains into a deterministic fixture file.
func (a *App) FixtureExport(req fixtureExportRequest) (map[string]interface{}, error) {
	if strings.TrimSpace(req.FilePath) == "" {
		return nil, fmt.Errorf("file_path is required")
	}
	if len(req.Domains) == 0 {
		return nil, fmt.Errorf("at least one domain is required")
	}
	ctx, cancel := a.reqCtx()
	defer cancel()
	client := a.getLeaderClient()
	if client == nil {
		return nil, fmt.Errorf("leader client is not available")
	}
	fixture, err := client.ExportFixture(ctx, fixtures.ExportOptions{
		Domains:     req.Domains,
		Name:        req.Name,
		Description: req.Description,
	})
	if err != nil {
		return nil, err
	}
	if err := fixtures.SaveFile(strings.TrimSpace(req.FilePath), fixture); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"status": "exported",
		"file":   strings.TrimSpace(req.FilePath),
		"name":   fixture.Name,
		"steps":  len(fixture.Steps),
	}, nil
}

// ── File export (Wails desktop downloads) ────────────────────────────────────

// SaveTextFile shows a native save dialog and writes UTF-8 text to the chosen path.
func (a *App) SaveTextFile(suggestedName, content string) error {
	path, err := runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{
		Title:           "Save File",
		DefaultFilename: suggestedName,
	})
	if err != nil || path == "" {
		return err
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("SaveTextFile: write %s: %w", path, err)
	}
	return nil
}

// SaveBinaryFile shows a native save dialog and writes base64-encoded binary
// (or a data-URL with "data:...;base64," prefix) to the chosen path.
func (a *App) SaveBinaryFile(suggestedName, base64data string) error {
	path, err := runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{
		Title:           "Save File",
		DefaultFilename: suggestedName,
	})
	if err != nil || path == "" {
		return err
	}
	// Strip optional data-URL prefix (e.g. "data:image/png;base64,")
	if idx := strings.Index(base64data, ","); idx != -1 {
		base64data = base64data[idx+1:]
	}
	data, err := base64.StdEncoding.DecodeString(base64data)
	if err != nil {
		return fmt.Errorf("SaveBinaryFile: base64 decode: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("SaveBinaryFile: write %s: %w", path, err)
	}
	return nil
}

func (a *App) recordReadRoutingMetrics(input readRoutingMetricInput) {
	if a.routingStats == nil {
		return
	}
	a.routingStats.record(input)
}
