package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/pkg/discovery"
)

// Node defines the interface for a Raft node
type Node interface {
	// IsLeader returns whether this node is currently the leader
	IsLeader() bool

	// GetLeaderURL returns the URL of the current leader
	GetLeaderURL() (string, error)

	// GetLocalURL returns the URL of this node
	GetLocalURL() string

	// Process is called by the transport to process messages
	Process(ctx context.Context, m raftpb.Message) error

	// IsIDRemoved returns whether a node ID has been removed
	IsIDRemoved(id uint64) bool

	// ReportUnreachable is called when a node becomes unreachable
	ReportUnreachable(id uint64)

	// ReportSnapshot is called when a snapshot status changes
	ReportSnapshot(id uint64, status raft.SnapshotStatus)

	// ConfState returns current node confstate.
	ConfState() raftpb.ConfState

	// ConnectToNodes actively establishes connections to the specified node IDs
	ConnectToNodes(nodeIDs []uint64)

	// Stop stops the Raft node and releases resources
	Stop()
}

// Storage defines the interface for Raft storage
type Storage interface {
	// Implements the etcd/raft.Storage interface
	raft.Storage

	// Append appends the entries to storage
	Append([]raftpb.Entry) error

	// SetHardState sets the hard state
	SetHardState(st raftpb.HardState) error

	// CreateSnapshot creates a snapshot
	CreateSnapshot(snapIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)

	// ApplySnapshot applies a snapshot
	ApplySnapshot(raftpb.Snapshot) error

	// CleanupSnapshots cleans up snapshots keeping only N most recent ones
	CleanupSnapshots(retain int) error
}

// Commit represents a committed log entry
type Commit struct {
	Data       []string
	ApplyDoneC chan<- struct{}
}

// LeaderProcess handles actions when leadership changes
type LeaderProcess interface {
	// OnLeadershipChanged is called when leadership status changes
	OnLeadershipChanged(isLeader bool)
}

// Config contains configuration for a Raft node
type Config struct {
	// NodeID is the ID of this node
	NodeID uint64

	// RaftPort is the port for Raft communication
	RaftPort int

	// Address is the node's address (host:port)
	Address string

	// Storage is the storage backend
	Storage Storage

	// Discovery is the service for discovering other nodes
	Discovery discovery.Discovery

	// Join indicates whether to join an existing cluster
	Join bool

	// BootstrapAllowed indicates whether this node can bootstrap a new cluster
	BootstrapAllowed bool

	// LeaderProcess handles leadership changes
	LeaderProcess LeaderProcess

	// Logger is the logger instance
	Logger *zap.Logger

	// GetSnapshot is a function to get a snapshot of the state
	GetSnapshot func() ([]byte, error)
}

// New creates a new Raft node
func New(config Config, proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (Node, <-chan *Commit, <-chan error) {
	// Implementation in node.go
	return newNode(config, proposeC, confChangeC)
}

// Constants used throughout the package
const (
	// defaultSnapshotCount is the default number of entries between snapshots
	defaultSnapshotCount uint64 = 10000

	// snapshotCatchUpEntriesN is the number of entries to preserve when compacting
	snapshotCatchUpEntriesN = 10000
)
