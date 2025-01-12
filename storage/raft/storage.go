package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/3vilhamster/kv-raft-cass/storage"
)

var _ storage.Raft = (*Storage)(nil)

// Namespace represents a group of shards and processors
type Namespace struct {
	ID           string
	ShardRange   ShardRange // Range of shard IDs belonging to this namespace
	Processors   map[uint64]Processor
	State        string // "active", "draining", "inactive"
	LastModified time.Time
}

type ShardRange struct {
	Start uint64
	End   uint64
}

// ShardAssignment represents the mapping of shards to processors within a namespace
type ShardAssignment struct {
	NamespaceID string
	ShardID     uint64
	ProcessorID uint64
	State       string // "assigned", "pending", "rebalancing"
}

// ClusterState represents the current state of the entire cluster
type ClusterState struct {
	Namespaces   map[string]*Namespace
	Version      uint64 // Incremented on each change
	LastModified time.Time
}

type Processor struct {
	ID       uint64
	Endpoint string
	Load     float64 // Metric for load balancing
	State    string  // "active", "leaving", "joining"
}

// Storage implements the raft.Storage interface
type Storage struct {
	sync.RWMutex

	session     *gocql.Session
	namespaceID string
	nodeID      uint64

	// Cache the first and last indices to avoid querying Cassandra
	firstIndex uint64
	lastIndex  uint64

	// Cache the latest snapshot
	snapshot *raftpb.Snapshot
}

// New creates a new storage instance
func New(session *gocql.Session, namespaceID string, nodeID uint64) (*Storage, error) {
	s := &Storage{
		session:     session,
		namespaceID: namespaceID,
		nodeID:      nodeID,
	}

	// Initialize indices
	if err := s.initIndices(); err != nil {
		return nil, fmt.Errorf("ini indices: %w", err)
	}

	return s, nil
}

// InitialState implements the raft.Storage interface
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.RLock()
	defer s.RUnlock()

	// Default state for fresh cluster
	hardState := raftpb.HardState{}

	// Get the latest term and commit index
	var term, commit uint64
	err := s.session.Query(`SELECT MAX(term) as term, MAX(snap_index) as commit
    FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ?`,
		s.namespaceID, s.nodeID,
	).Scan(&term, &commit)

	if err != nil && err != gocql.ErrNotFound {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("get max indices: %w", err)
	}

	// For joining nodes, use the latest known state
	if err == nil && commit > 0 {
		hardState.Term = term
		hardState.Commit = commit
		// Don't set Vote for joining nodes
		hardState.Vote = 0
	}

	// Get current membership
	iter := s.session.Query(`
        SELECT node_id 
        FROM raft_membership 
        WHERE namespace_id = ?`,
		s.namespaceID,
	).Iter()

	var nodeID uint64
	var voters []uint64
	for iter.Scan(&nodeID) {
		voters = append(voters, nodeID)
	}
	if err := iter.Close(); err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("get membership: %w", err)
	}

	confState := raftpb.ConfState{
		Voters: voters,
	}

	return hardState, confState, nil
}

func (s *Storage) SetHardState(st raftpb.HardState) error {
	return s.session.Query(`INSERT INTO raft_state (namespace_id, node_id, term, vote, commit, last_updated)
    VALUES (?, ?, ?, ?, ?, ?)`,
		s.namespaceID, s.nodeID, st.Term, st.Vote, st.Commit, time.Now(),
	).Exec()
}

// Entries implements the raft.Storage interface
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.RLock()
	defer s.RUnlock()

	if lo < s.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndex+1 {
		return nil, raft.ErrUnavailable
	}

	iter := s.session.Query(`SELECT snap_index, term, entry_type, data 
    FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ? AND snap_index >= ? AND snap_index < ?`,
		s.namespaceID, s.nodeID, lo, hi,
	).Iter()

	var entries []raftpb.Entry
	var size uint64
	var snapIndex, term uint64
	var entryType int
	var data []byte

	for iter.Scan(&snapIndex, &term, &entryType, &data) {
		entry := raftpb.Entry{
			Term:  term,
			Index: snapIndex,
			Type:  raftpb.EntryType(entryType),
			Data:  data,
		}

		size += uint64(entry.Size())
		if size > maxSize && len(entries) > 0 {
			break
		}
		entries = append(entries, entry)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iter close: %w", err)
	}

	return entries, nil
}

// Term implements the raft.Storage interface
func (s *Storage) Term(i uint64) (uint64, error) {
	s.RLock()
	defer s.RUnlock()

	// Log for debugging
	log.Printf("Term requested for index %d (firstIndex=%d, lastIndex=%d)", i, s.firstIndex, s.lastIndex)

	// Handle special cases for fresh cluster and initialization
	if i == 0 {
		return 0, nil
	}

	// For fresh cluster initialization
	if s.lastIndex == 0 {
		if i <= 1 {
			return 0, nil // Return term 0 for initial entries
		}
		return 0, raft.ErrUnavailable
	}

	if i < s.firstIndex {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}
	// If it's a snapshot entry
	if s.snapshot != nil && i == s.snapshot.Metadata.Index {
		return s.snapshot.Metadata.Term, nil
	}

	// For first entry in fresh state
	if i == 1 && s.firstIndex == 1 {
		return 0, nil
	}

	var term uint64
	err := s.session.Query(`SELECT term 
    FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ? AND snap_index = ?`,
		s.namespaceID, s.nodeID, i,
	).Scan(&term)

	if err == gocql.ErrNotFound {
		// For initial cluster setup, return term 0 for index 1
		if i == 1 {
			return 0, nil
		}
		return 0, raft.ErrUnavailable
	}
	if err != nil {
		return 0, fmt.Errorf("get term: %w", err)
	}

	return term, nil
}

// LastIndex implements the raft.Storage interface
func (s *Storage) LastIndex() (uint64, error) {
	s.RLock()
	defer s.RUnlock()
	return s.lastIndex, nil
}

// FirstIndex implements the raft.Storage interface
func (s *Storage) FirstIndex() (uint64, error) {
	s.RLock()
	defer s.RUnlock()
	return s.firstIndex, nil
}

// Snapshot implements the raft.Storage interface
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.RLock()
	defer s.RUnlock()

	// For fresh cluster or no snapshot yet
	if s.snapshot == nil {
		return raftpb.Snapshot{}, nil // Return empty snapshot for new clusters
	}

	return *s.snapshot, nil
}

// CreateSnapshot creates a new snapshot at the given index
func (s *Storage) CreateSnapshot(snapIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	if snapIndex <= s.firstIndex || snapIndex > s.lastIndex {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}

	// Get the term for the snapshot index
	term, err := s.Term(snapIndex)
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("get term: %w", err)
	}

	// Create snapshot metadata
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapIndex,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}

	// Marshal the snapshot as bytes
	snapData, err := snap.Marshal()
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("marshal snapshot: %w", err)
	}

	// Save to Cassandra
	err = s.session.Query(`INSERT INTO raft_snapshots (
        namespace_id,
        node_id,
        snap_index,
        term,
        data,
        created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`,
		s.namespaceID,
		s.nodeID,
		snapIndex,
		term,
		snapData,
		time.Now(),
	).Exec()

	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("save snapshot: %w", err)
	}

	// Update in-memory state
	s.snapshot = &snap
	// Update first index
	s.firstIndex = snapIndex + 1

	// Clean up old entries
	err = s.session.Query(`DELETE FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ? AND snap_index <= ?`,
		s.namespaceID, s.nodeID, snapIndex,
	).Exec()
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("cleanup entries: %w", err)
	}

	return *s.snapshot, nil
}

// ApplySnapshot applies a snapshot to the storage
func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.Lock()
	defer s.Unlock()

	snapIndex := snap.Metadata.Index
	if snapIndex <= s.firstIndex {
		return raft.ErrSnapOutOfDate
	}

	// Marshal the ConfState
	confStateData, err := json.Marshal(&snap.Metadata.ConfState)
	if err != nil {
		return fmt.Errorf("marshal confstate: %w", err)
	}

	// Save the snapshot
	err = s.session.Query(`INSERT INTO raft_snapshots (
        namespace_id, node_id, snap_index, term, data, conf_state, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.namespaceID, s.nodeID, snapIndex, snap.Metadata.Term, snap.Data, confStateData, time.Now(),
	).Exec()
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	s.snapshot = &snap
	s.firstIndex = snapIndex + 1
	s.lastIndex = snapIndex

	return nil
}

// DeleteSnapshot deletes a snapshot at the given index
func (s *Storage) DeleteSnapshot(index uint64) error {
	s.Lock()
	defer s.Unlock()

	return s.session.Query(`DELETE FROM raft_snapshots 
    WHERE namespace_id = ? AND node_id = ? AND snap_index = ?`,
		s.namespaceID, s.nodeID, index,
	).Exec()
}

// CleanupSnapshots keeps only the N most recent snapshots
func (s *Storage) CleanupSnapshots(retain int) error {
	s.Lock()
	defer s.Unlock()

	// Get all snapshot indices ordered by index descending
	iter := s.session.Query(`SELECT snap_index 
    FROM raft_snapshots 
    WHERE namespace_id = ? AND node_id = ?
    ORDER BY snap_index DESC`,
		s.namespaceID, s.nodeID,
	).Iter()

	var indices []uint64
	var index uint64
	for iter.Scan(&index) {
		indices = append(indices, index)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("cleanup snapshot: %w", err)
	}

	if len(indices) <= retain {
		return nil
	}

	// Delete old snapshots
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	for _, idx := range indices[retain:] {
		batch.Query(`DELETE FROM raft_snapshots 
    WHERE namespace_id = ? AND node_id = ? AND snap_index = ?`,
			s.namespaceID, s.nodeID, idx)
	}

	return s.session.ExecuteBatch(batch)
}

// Append appends the new entries to storage
func (s *Storage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	first := entries[0].Index
	last := entries[len(entries)-1].Index

	// For initial entries
	if s.lastIndex == 0 {
		s.firstIndex = first
		s.lastIndex = first - 1
	}

	// Ensure entries are in expected range
	if first <= s.firstIndex {
		entries = entries[s.firstIndex-first:]
		if len(entries) == 0 {
			return nil
		}
		first = s.firstIndex
	}

	// Create batch
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	now := time.Now()

	for _, entry := range entries {
		batch.Query(`INSERT INTO raft_entries (
        namespace_id, node_id, snap_index, term, entry_type, data, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			s.namespaceID, s.nodeID, entry.Index, entry.Term, int(entry.Type), entry.Data, now,
		)

		// Update lastIndex as we add entries
		if entry.Index > s.lastIndex {
			s.lastIndex = entry.Index
		}
	}

	// Execute batch
	if err := s.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("failed to append entries: %v", err)
	}

	log.Printf("Appended entries: first=%d last=%d firstIndex=%d lastIndex=%d",
		first, last, s.firstIndex, s.lastIndex)

	return nil
}

func (s *Storage) initIndices() error {
	// Load the latest snapshot first
	snapshot, err := s.loadLatestSnapshot()
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	s.snapshot = snapshot

	// Initialize indices
	if snapshot != nil {
		s.firstIndex = snapshot.Metadata.Index
		s.lastIndex = snapshot.Metadata.Index
	} else {
		// For fresh cluster, start at index 1
		s.firstIndex = 1
		s.lastIndex = 0 // Will be updated if entries exist
	}

	// Get last index from entries
	var maxIndex uint64
	err = s.session.Query(`SELECT MAX(snap_index) 
    FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ?`,
		s.namespaceID, s.nodeID,
	).Scan(&maxIndex)

	if err != nil && err != gocql.ErrNotFound {
		return fmt.Errorf("get entries: %w", err)
	}

	// Update lastIndex if we found entries
	if err != gocql.ErrNotFound && maxIndex > 0 {
		s.lastIndex = maxIndex
		// For joining nodes, set firstIndex
		if s.firstIndex == 1 {
			// Get the minimum index available
			var minIndex uint64
			err = s.session.Query(`SELECT MIN(snap_index) 
    FROM raft_entries 
    WHERE namespace_id = ? AND node_id = ?`,
				s.namespaceID, s.nodeID,
			).Scan(&minIndex)
			if err == nil && minIndex > 0 {
				s.firstIndex = minIndex
			}
		}
	}
	log.Printf("Storage initialized with firstIndex=%d, lastIndex=%d", s.firstIndex, s.lastIndex)
	return nil
}

func (s *Storage) loadLatestSnapshot() (*raftpb.Snapshot, error) {
	var snapIndex, term uint64
	var data, confStateData []byte
	var createdAt time.Time

	err := s.session.Query(`SELECT snap_index, term, data, conf_state, created_at 
    FROM raft_snapshots 
    WHERE namespace_id = ? AND node_id = ?
    ORDER BY snap_index DESC
    LIMIT 1`,
		s.namespaceID, s.nodeID,
	).Scan(&snapIndex, &term, &data, &confStateData, &createdAt)

	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest snapshot: %w", err)
	}

	var confState raftpb.ConfState
	if err := json.Unmarshal(confStateData, &confState); err != nil {
		return nil, fmt.Errorf("unmarshal constate: %w", err)
	}

	return &raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapIndex,
			Term:      term,
			ConfState: confState,
		},
	}, nil
}
