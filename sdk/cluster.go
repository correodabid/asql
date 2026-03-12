// Package asqlsdk provides a topology-aware connection pool for ASQL clusters.
//
// Any ASQL node accepts connections, but only the leader accepts writes. The
// SDK bootstraps cluster topology on first connect by reading the
// asql_cluster_leader and asql_cluster_peers ParameterStatus messages sent by
// the server during the pgwire startup handshake.
//
// On a write against a follower node (SQLSTATE 25006, Hint "asql_leader=…"),
// the SDK automatically reconnects to the new leader and retries the
// statement once — making leader failover transparent to the application.
//
// Typical usage:
//
//	cluster, err := asqlsdk.Connect(ctx, asqlsdk.Config{
//	    Seeds: []string{"host1:5433", "host2:5434", "host3:5435"},
//	})
//	if err != nil { ... }
//	defer cluster.Close()
//
//	tag, err := cluster.Exec(ctx, "INSERT INTO orders ...")
//	rows, err := cluster.Query(ctx, "SELECT * FROM orders WHERE ...")
package asqlsdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds the configuration for a cluster-aware connection pool.
type Config struct {
	// Seeds is a list of pgwire addresses ("host:port") for any node in the
	// cluster. Provide all known addresses for maximum startup resilience.
	Seeds []string

	// User, Password, Database are the standard pgwire credentials.
	// Defaults: user="asql", database="asql".
	User     string
	Password string
	Database string

	// MaxConns is the maximum number of connections in each node pool.
	// 0 uses the pgx default (4 per CPU).
	MaxConns int32
}

// Cluster is a topology-aware connection pool for an ASQL cluster.
//
// It maintains a connection pool to the current leader and refreshes topology
// automatically when a follower-redirect error is received. All exported
// methods are safe for concurrent use.
type Cluster struct {
	mu         sync.RWMutex
	cfg        Config
	leaderAddr string       // pgwire address of the current leader
	peers      []string     // all known pgwire addresses (including self)
	pool       *pgxpool.Pool // pool always points to the current leader
	closed     bool
}

// Connect establishes a Cluster by bootstrapping topology from the first
// reachable seed node. It returns an error if no seed is reachable.
func Connect(ctx context.Context, cfg Config) (*Cluster, error) {
	if len(cfg.Seeds) == 0 {
		return nil, errors.New("asqlsdk: at least one seed address is required")
	}
	c := &Cluster{cfg: cfg}
	if err := c.bootstrap(ctx, cfg.Seeds); err != nil {
		return nil, fmt.Errorf("asqlsdk: bootstrap failed: %w", err)
	}
	return c, nil
}

// Exec runs a SQL statement on the leader pool.
//
// If the target node is no longer the leader (SQLSTATE 25006 with an
// asql_leader hint), the SDK reconnects to the advertised leader and retries
// the statement once before returning the error.
func (c *Cluster) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	for attempt := 0; attempt < 2; attempt++ {
		pool := c.getPool()
		tag, err := pool.Exec(ctx, sql, args...)
		if err == nil {
			return tag, nil
		}
		newLeader := parseRedirectHint(err)
		if newLeader == "" {
			return tag, err
		}
		// Follower-redirect: rebuild pool targeting the new leader.
		seeds := append([]string{newLeader}, c.getKnownAddrs()...)
		if refreshErr := c.bootstrap(ctx, seeds); refreshErr != nil {
			return tag, err // return the original error if reconnect fails
		}
	}
	return pgconn.CommandTag{}, errors.New("asqlsdk: max redirect attempts exceeded")
}

// Query executes a SQL query on the leader pool and returns the result rows.
//
// Like Exec, it retries once on a follower-redirect error.
func (c *Cluster) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	for attempt := 0; attempt < 2; attempt++ {
		pool := c.getPool()
		rows, err := pool.Query(ctx, sql, args...)
		if err == nil {
			return rows, nil
		}
		newLeader := parseRedirectHint(err)
		if newLeader == "" {
			return rows, err
		}
		seeds := append([]string{newLeader}, c.getKnownAddrs()...)
		if refreshErr := c.bootstrap(ctx, seeds); refreshErr != nil {
			return rows, err
		}
	}
	return nil, errors.New("asqlsdk: max redirect attempts exceeded")
}

// QueryRow executes a SQL query expected to return at most one row.
// It does not retry on redirect — use Query for retry-safe reads.
func (c *Cluster) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.getPool().QueryRow(ctx, sql, args...)
}

// Refresh re-discovers cluster topology from the known peers and seeds.
// Call this periodically if you want the pool to follow a new leader after
// an application-level failover rather than waiting for the next write error.
func (c *Cluster) Refresh(ctx context.Context) error {
	return c.bootstrap(ctx, c.getKnownAddrs())
}

// LeaderAddr returns the pgwire address of the currently tracked leader.
func (c *Cluster) LeaderAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leaderAddr
}

// Peers returns all pgwire addresses currently known to the SDK.
func (c *Cluster) Peers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.peers))
	copy(out, c.peers)
	return out
}

// Close closes the underlying connection pool and frees resources.
func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool != nil {
		c.pool.Close()
		c.pool = nil
	}
	c.closed = true
}

// ── internal helpers ──────────────────────────────────────────────────────────

func (c *Cluster) getPool() *pgxpool.Pool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pool
}

// getKnownAddrs returns all currently known pgwire addresses (leader + peers +
// configured seeds), deduplicated, leader first.
func (c *Cluster) getKnownAddrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	addrs := make([]string, 0, 1+len(c.peers)+len(c.cfg.Seeds))
	if c.leaderAddr != "" {
		addrs = append(addrs, c.leaderAddr)
	}
	addrs = append(addrs, c.peers...)
	addrs = append(addrs, c.cfg.Seeds...)
	return dedupe(addrs)
}

// bootstrap connects to the first reachable address in seeds, reads topology
// from the server's ParameterStatus messages, and (re)creates the leader pool.
func (c *Cluster) bootstrap(ctx context.Context, seeds []string) error {
	var lastErr error
	for _, seed := range seeds {
		leaderAddr, peers, err := discoverTopology(ctx, c.cfg, seed)
		if err != nil {
			lastErr = err
			continue
		}
		// Standalone node: use the seed itself as the "leader".
		if leaderAddr == "" {
			leaderAddr = seed
		}

		pool, err := newLeaderPool(ctx, c.cfg, leaderAddr)
		if err != nil {
			lastErr = err
			continue
		}

		c.mu.Lock()
		if c.pool != nil {
			c.pool.Close()
		}
		c.pool = pool
		c.leaderAddr = leaderAddr
		c.peers = dedupe(append([]string{leaderAddr}, peers...))
		c.mu.Unlock()
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("no reachable seed found")
}

// discoverTopology opens a single non-pooled connection to addr, reads the
// asql_cluster_leader and asql_cluster_peers ParameterStatus values emitted
// during the pgwire startup handshake, and closes the connection.
func discoverTopology(ctx context.Context, cfg Config, addr string) (leader string, peers []string, err error) {
	connCfg, err := pgx.ParseConfig(connString(cfg, addr))
	if err != nil {
		return "", nil, fmt.Errorf("parse config for %s: %w", addr, err)
	}
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return "", nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close(ctx) //nolint:errcheck

	leader = conn.PgConn().ParameterStatus("asql_cluster_leader")
	if raw := conn.PgConn().ParameterStatus("asql_cluster_peers"); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}
	}
	return leader, peers, nil
}

// newLeaderPool creates a pgxpool.Pool pointing at addr and pings it to verify
// connectivity.
func newLeaderPool(ctx context.Context, cfg Config, addr string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(connString(cfg, addr))
	if err != nil {
		return nil, fmt.Errorf("parse pool config for %s: %w", addr, err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool for %s: %w", addr, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", addr, err)
	}
	return pool, nil
}

// parseRedirectHint returns the leader address from a SQLSTATE 25006 error
// whose Hint field contains "asql_leader=host:port". Returns "" for any other
// error, meaning the caller should not retry.
func parseRedirectHint(err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return ""
	}
	if pgErr.Code != "25006" {
		return ""
	}
	// Hint format: "asql_leader=host:port" (may have other words around it)
	for _, part := range strings.Fields(pgErr.Hint) {
		if strings.HasPrefix(part, "asql_leader=") {
			return strings.TrimPrefix(part, "asql_leader=")
		}
	}
	return ""
}

// connString builds a libpq-style DSN for the given addr ("host:port").
func connString(cfg Config, addr string) string {
	user := cfg.User
	if user == "" {
		user = "asql"
	}
	db := cfg.Database
	if db == "" {
		db = "asql"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// addr has no port — treat as host with default port
		host, port = addr, "5433"
	}
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s sslmode=disable connect_timeout=5",
		host, port, user, db,
	)
	if cfg.Password != "" {
		dsn += " password=" + cfg.Password
	}
	return dsn
}

// dedupe returns a new slice with duplicates removed, preserving order.
func dedupe(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
