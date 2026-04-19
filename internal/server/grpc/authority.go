package grpc

import (
	"strings"

	"github.com/correodabid/asql/internal/cluster/coordinator"
	"github.com/correodabid/asql/internal/cluster/raft"
)

type clusterAuthority interface {
	LeaderID(groups []string) string
	CanAcceptWrite(group, nodeID, fencing string) bool
}

type raftLeaderState interface {
	IsLeader() bool
	LeaderID() string
}

type raftClusterAuthority struct {
	localNodeID string
	raftNode    raftLeaderState
	leadership  *coordinator.LeadershipManager
}

func newRaftClusterAuthority(localNodeID string, raftNode *raft.RaftNode, leadership *coordinator.LeadershipManager) clusterAuthority {
	if strings.TrimSpace(localNodeID) == "" || raftNode == nil {
		return nil
	}
	return &raftClusterAuthority{
		localNodeID: strings.TrimSpace(localNodeID),
		raftNode:    raftNode,
		leadership:  leadership,
	}
}

func (authority *raftClusterAuthority) LeaderID(_ []string) string {
	if authority == nil || authority.raftNode == nil {
		return ""
	}
	if authority.raftNode.IsLeader() {
		return authority.localNodeID
	}
	return strings.TrimSpace(authority.raftNode.LeaderID())
}

func (authority *raftClusterAuthority) CanAcceptWrite(group, nodeID, fencing string) bool {
	if authority == nil || authority.raftNode == nil || authority.leadership == nil {
		return false
	}
	if !authority.raftNode.IsLeader() {
		return false
	}
	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedNodeID == "" || trimmedNodeID != authority.localNodeID {
		return false
	}
	return authority.leadership.CanAcceptWrite(strings.ToLower(strings.TrimSpace(group)), trimmedNodeID, strings.TrimSpace(fencing))
}