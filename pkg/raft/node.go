package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/pkg/discovery"
)

// node implements the Node interface
type node struct {
	raftPort int
	address  string

	proposeC    <-chan string
	confChangeC <-chan raftpb.ConfChange
	commitC     chan<- *Commit
	errorC      chan<- error

	id          uint64
	join        bool
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	raftNode   raft.Node
	storage    Storage
	transport  *rafthttp.Transport
	snapCount  uint64
	stopc      chan struct{}
	httpstopc  chan struct{}
	httpdonec  chan struct{}
	lastLeader bool

	logger           *zap.Logger
	leaderProcess    LeaderProcess
	discovery        discovery.Discovery
	bootstrapAllowed bool

	mu sync.RWMutex
}

// newNode creates a new Raft node
func newNode(config Config, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (Node, <-chan *Commit, <-chan error) {
	commitC := make(chan *Commit)
	errorC := make(chan error)

	n := &node{
		raftPort: config.RaftPort,
		address:  config.Address,

		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          config.NodeID,
		join:        config.Join,
		getSnapshot: config.GetSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger:           config.Logger,
		discovery:        config.Discovery,
		bootstrapAllowed: config.BootstrapAllowed,
		storage:          config.Storage,
		leaderProcess:    config.LeaderProcess,
	}

	go n.startRaft()
	return n, commitC, errorC
}

func (n *node) ConfState() raftpb.ConfState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.confState
}

// IsLeader returns whether this node is the leader
func (n *node) IsLeader() bool {
	if n.raftNode == nil {
		return false
	}
	return n.raftNode.Status().Lead == n.id
}

// GetLocalURL returns the URL of this node
func (n *node) GetLocalURL() string {
	return fmt.Sprintf("http://%s", n.address)
}

// GetLeaderURL returns the URL of the current leader
func (n *node) GetLeaderURL() (string, error) {
	// Check if we're the leader
	if n.IsLeader() {
		return n.GetLocalURL(), nil
	}

	// Get leader ID from Raft state
	leaderID := n.raftNode.Status().Lead
	if leaderID == 0 {
		return "", errors.New("no leader elected yet")
	}

	// Use the discovery mechanism to get the leader URL
	if n.discovery != nil {
		leaderURL, err := n.discovery.GetNodeURL(leaderID)
		if err == nil {
			return leaderURL, nil
		}
		n.logger.Debug("failed to get leader URL from discovery",
			zap.Error(err),
			zap.Uint64("leader_id", leaderID))
	}

	// If discovery doesn't have the information or isn't available,
	// we can't determine the leader URL
	return "", errors.New("leader URL not found")
}

// Process is called by the transport to process messages
func (n *node) Process(ctx context.Context, m raftpb.Message) error {
	return n.raftNode.Step(ctx, m)
}

// IsIDRemoved returns whether a node ID has been removed
func (n *node) IsIDRemoved(id uint64) bool {
	return false
}

// ReportUnreachable is called when a node becomes unreachable
func (n *node) ReportUnreachable(id uint64) {
	n.raftNode.ReportUnreachable(id)
}

// ReportSnapshot is called when a snapshot status changes
func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.raftNode.ReportSnapshot(id, status)
}

// Stop stops the Raft node and releases resources
func (n *node) Stop() {
	n.stopHTTP()
	close(n.commitC)
	close(n.errorC)
	n.raftNode.Stop()
}

// stopHTTP stops the HTTP server
func (n *node) stopHTTP() {
	n.logger.Debug("stopping http")
	n.transport.Stop()
	n.logger.Debug("transport stopped, closing httpstopc")
	close(n.httpstopc)
	n.logger.Debug("waiting for ack on httpdonec")
	<-n.httpdonec
	n.logger.Debug("httpdonec closed")
}

// joinCluster attempts to join an existing cluster
func (n *node) joinCluster() error {
	if n.discovery == nil {
		return errors.New("no discovery service configured")
	}
	return n.discovery.JoinCluster(n.id, n.address, 500*time.Millisecond, 10)
}

// startRaft initializes and starts the Raft node
func (n *node) startRaft() {
	c := &raft.Config{
		ID:                        n.id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		PreVote:                   true,
		CheckQuorum:               true,
		DisableProposalForwarding: false,
	}

	// Initialize transport before creating node
	n.initTransport()

	if n.join {
		n.logger.Info("joining existing cluster", zap.Uint64("id", n.id))
		n.raftNode = raft.RestartNode(c)
		if err := n.joinCluster(); err != nil {
			n.logger.Error("failed to join cluster", zap.Error(err))
			// We'll continue anyway and retry later
		}
	} else {
		shouldBootstrap := true

		if n.discovery != nil {
			// Try to find existing nodes
			nodes, err := n.discovery.GetClusterNodes()
			if err == nil && len(nodes) > 0 {
				// Other nodes exist, we should join instead
				n.logger.Info("existing nodes found, switching to join mode",
					zap.Int("found_nodes", len(nodes)),
					zap.Strings("nodes", nodes))
				shouldBootstrap = false
				n.join = true

				// Restart in joining mode
				n.raftNode = raft.RestartNode(c)

				// Try to join
				if err := n.joinCluster(); err != nil {
					n.logger.Error("failed to join existing cluster", zap.Error(err))
					// We'll continue anyway
				}
			}
		}

		if shouldBootstrap {
			if !n.bootstrapAllowed {
				n.logger.Fatal("attempted to bootstrap but not allowed")
			}

			// Bootstrap as single-node cluster
			peers := make([]raft.Peer, 1)
			peers[0] = raft.Peer{ID: n.id}

			n.logger.Info("bootstrapping new cluster",
				zap.Uint64("id", n.id),
				zap.Int("peer_count", len(peers)))

			n.raftNode = raft.StartNode(c, peers)
		}
	}

	// Start the transport and processing services
	go n.serveRaft()
	go n.serveChannels()
}
