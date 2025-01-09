package storage

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type Raft interface {
	raft.Storage

	Append([]raftpb.Entry) error
	SetHardState(st raftpb.HardState) error

	// Snapshots manipulations:
	CreateSnapshot(snapIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	ApplySnapshot(raftpb.Snapshot) error
	LoadNewestAvailable(walSnaps []walpb.Snapshot) (raftpb.Snapshot, error)
	CleanupSnapshots(retain int) error
}
