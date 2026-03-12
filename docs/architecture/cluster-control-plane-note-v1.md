# ASQL Cluster Control-Plane Note (v1)

Date: 2026-03-11

## Purpose

This note documents the cluster control-plane that should be treated as the
current production path, and inventories the remaining legacy/transitional
heartbeat-led paths that still exist in the codebase.

## Production cluster runtime

The canonical clustered runtime is:

- `internal/server/pgwire` as the main server,
- a gRPC sidecar for cluster RPCs,
- `internal/cluster/raft` for leader election and quorum replication,
- engine commits routed through the Raft quorum path.

In this runtime:

1. `pgwire.Server` builds the engine, WAL store, audit store, heartbeat loop,
	 Raft transport, and `RaftNode`.
2. The engine is wired with `SetRaftCommitter(...)`, so clustered writes go
	 through Raft instead of the standalone direct-WAL append path.
3. The gRPC sidecar is started first so peers can reach the node for
	 `RequestVote` / `AppendEntries`.
4. The heartbeat loop still runs, but Raft is authoritative for role
	 determination via `SetRaftRoleSource(...)`.
5. When Raft is active, the standalone streaming replicator is skipped because
	 replication is handled by Raft `AppendEntries`.

## Production control-plane semantics

### Leader election

- Raft elects the writable leader.
- The heartbeat loop must not independently define writable leadership in the
	production runtime.
- Heartbeat role behavior is subordinate to the Raft role source when both are
	present.

### Quorum commit

- In cluster mode, the engine commit path must use the `RaftCommitter`.
- The direct WAL append path is the standalone path only.
- A clustered write is considered accepted only after the Raft batch is
	applied through quorum.

### Replication

- Production replication is Raft log replication via `AppendEntries`.
- The old persistent stream replicator remains useful for legacy/transitional
	modes but is not the production replication authority.

### Fencing and failover

- Leadership/fencing metadata still flows through the coordinator/heartbeat
	machinery.
- In the production runtime, pgwire and the cluster gRPC sidecar resolve
	leader identity from a Raft-backed authority.
- The coordinator lease table remains fencing metadata, not an independent
	source of writable authority.
- Heartbeat peer gossip is retained for peer metadata propagation and
	failover support, but production leader discovery and write acceptance must
	not diverge from Raft.

## Remaining heartbeat-led paths outside the canonical runtime

### 1) Standalone gRPC server cluster startup

File:
- `internal/server/grpc/server.go`

Current behavior:
- When `NodeID` and `Peers` are set, the standalone gRPC server starts a
	heartbeat loop and persistent stream replicator.
- This path does not boot the full pgwire + Raft production runtime.

Status:
- legacy/transitional,
- not the canonical production cluster runtime.

### 2) Standalone gRPC config cluster fields

File:
- `internal/server/grpc/config.go`

Current behavior:
- Still exposes heartbeat-led cluster configuration directly from the
	standalone gRPC server package.

Status:
- transitional API surface.

### 3) Heartbeat leader source adapter

File:
- `internal/server/grpc/server.go`

Current behavior:
- `NewHeartbeatLeaderSource(...)` adapts the heartbeat loop into a
	`ClusterLeader` for the persistent stream replicator.

Status:
- legacy/transitional fallback,
- should not be the production authority for leader discovery.

### 4) Heartbeat-based stream replication fallback

Files:
- `internal/server/grpc/server.go`
- `internal/server/pgwire/server.go`

Current behavior:
- The persistent stream replicator can still run from heartbeat-derived leader
	information when Raft is absent.
- In the pgwire runtime this fallback is already skipped when Raft is active.

Status:
- fallback-only,
- not production replication for clustered deployments.

### 5) Heartbeat loop role/lease machinery

File:
- `internal/cluster/heartbeat/loop.go`

Current behavior:
- The loop still contains lease-based role tracking, leader discovery,
	follower replication hooks, and failover triggering logic.
- When Raft role injection is present, it behaves as a subordinate helper.

Status:
- mixed: still runtime-relevant for transitional control-plane support,
- but should not remain an independent source of writable truth.

## Path classification

### Production

- `internal/server/pgwire` clustered runtime
- `internal/cluster/raft`
- gRPC sidecar used by pgwire for Raft and cluster RPCs
- engine commit path through `RaftCommitter`

### Legacy / transitional

- standalone `internal/server/grpc` heartbeat-led cluster startup
- heartbeat-derived `ClusterLeader` adapter
- persistent stream replication driven by heartbeat leader discovery
- any path where heartbeat semantics can define effective writable authority

### Test/supporting

- heartbeat and failover simulation helpers/tests
- replication helpers used for non-production validation paths

## Immediate execution implications for Epic W

1. Keep the pgwire + Raft path as the only documented production cluster
	 runtime.
2. Remove or hard-disable heartbeat-led writable cluster mode outside that
	 runtime.
3. Converge leader discovery, write acceptance, fencing, and failover on one
	 authority model.
4. Keep heartbeat only where it is explicitly subordinate or transitional.
