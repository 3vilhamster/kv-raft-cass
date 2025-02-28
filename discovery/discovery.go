package discovery

import "time"

// Discovery defines the interface for Raft cluster discovery mechanisms
type Discovery interface {
	// GetClusterNodes returns a list of all known node URLs in the cluster
	GetClusterNodes() ([]string, error)

	// GetJoinEndpoints returns a list of HTTP endpoints for joining the cluster
	GetJoinEndpoints() ([]string, error)

	// JoinCluster attempts to join the cluster with the given node information
	JoinCluster(nodeID uint64, raftAddress string, initialBackoff time.Duration, maxRetries int) error
}
