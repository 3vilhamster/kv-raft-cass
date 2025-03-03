package raft

import (
	"encoding/json"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// entriesToApply determines which entries need to be applied
func (n *node) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}

	firstIdx := ents[0].Index

	// For initial entries, allow the first index to be greater
	if n.appliedIndex == 0 {
		n.appliedIndex = firstIdx - 1
		return ents
	}

	if firstIdx > n.appliedIndex+1 {
		n.logger.Sugar().Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1",
			firstIdx, n.appliedIndex)
	}

	// Filter out entries we've already applied
	offset := n.appliedIndex - firstIdx + 1
	if offset < uint64(len(ents)) {
		return ents[offset:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel
func (n *node) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	var data []string
	var applyDoneC chan struct{}

	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			n.logger.Debug("Processing entry: type=EntryNormal",
				zap.Uint64("index", ents[i].Index),
				zap.String("data", string(ents[i].Data)))

			if len(ents[i].Data) == 0 {
				n.logger.Debug("Ignoring empty message")
				// Ignore empty messages
				n.appliedIndex = ents[i].Index
				continue
			}

			// Only accept JSON data
			if !json.Valid(ents[i].Data) {
				n.logger.Error("invalid JSON data",
					zap.String("data", string(ents[i].Data)))
				continue
			}

			n.logger.Debug("Append data to commit data",
				zap.String("data", string(ents[i].Data)))
			data = append(data, string(ents[i].Data))

		case raftpb.EntryConfChange:
			n.logger.Debug("Processing entry: type=EntryConfChange",
				zap.Uint64("index", ents[i].Index))

			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				n.logger.Error("failed to unmarshal conf change",
					zap.Error(err),
					zap.Binary("data", ents[i].Data))
				continue
			}

			n.mu.Lock()
			// Apply the conf change
			n.confState = *n.raftNode.ApplyConfChange(cc)
			n.mu.Unlock()

			// When a node is added, update the transport layer
			if cc.Type == raftpb.ConfChangeAddNode && cc.NodeID != n.id {
				nodeURL := string(cc.Context)
				if nodeURL != "" {
					n.logger.Info("adding peer to transport",
						zap.Uint64("node_id", cc.NodeID),
						zap.String("url", nodeURL))
					n.transport.AddPeer(types.ID(cc.NodeID), []string{nodeURL})
				}
			} else if cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID != n.id {
				// Remove the peer from transport
				n.logger.Info("removing peer from transport",
					zap.Uint64("node_id", cc.NodeID))
				n.transport.RemovePeer(types.ID(cc.NodeID))
			}

			n.appliedIndex = ents[i].Index
		}
	}

	if len(data) > 0 {
		applyDoneC = make(chan struct{})
		n.logger.Debug("Sending commit",
			zap.Int("data_count", len(data)))
		select {
		case n.commitC <- &Commit{Data: data, ApplyDoneC: applyDoneC}:
			n.logger.Debug("Commit sent successfully",
				zap.Int("data_count", len(data)))
			n.appliedIndex = ents[len(ents)-1].Index
		case <-n.stopc:
			return nil, false
		}
	} else if len(ents) > 0 {
		// Update appliedIndex even if there's no data to publish
		n.appliedIndex = ents[len(ents)-1].Index
	}

	n.logger.Sugar().Debugf("Published entries: first=%d last=%d appliedIndex=%d data_count=%d",
		ents[0].Index, ents[len(ents)-1].Index, n.appliedIndex, len(data))

	return applyDoneC, true
}

// publishSnapshot sends the loaded snapshot to the commit channel
func (n *node) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	n.logger.Sugar().Infof("publishing snapshot at index %d", n.snapshotIndex)
	defer n.logger.Sugar().Infof("finished publishing snapshot at index %d", n.snapshotIndex)

	if snapshotToSave.Metadata.Index <= n.appliedIndex {
		n.logger.Sugar().Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]",
			snapshotToSave.Metadata.Index, n.appliedIndex)
	}

	n.commitC <- nil // trigger kvstore to load snapshot

	n.mu.Lock()
	n.confState = snapshotToSave.Metadata.ConfState
	n.snapshotIndex = snapshotToSave.Metadata.Index
	n.appliedIndex = snapshotToSave.Metadata.Index
	n.mu.Unlock()
}

// maybeTriggerSnapshot checks if we should create a snapshot
func (n *node) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if n.appliedIndex-n.snapshotIndex <= n.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-n.stopc:
			return
		}
	}

	n.logger.Sugar().Infof("start snapshot [applied index: %d | last snapshot index: %d]",
		n.appliedIndex, n.snapshotIndex)

	data, err := n.getSnapshot()
	if err != nil {
		n.logger.Sugar().Panic(err)
	}

	snap, err := n.storage.CreateSnapshot(n.appliedIndex, &n.confState, data)
	if err != nil {
		panic(err)
	}

	if err = n.storage.ApplySnapshot(snap); err != nil {
		panic(err)
	}

	compactIndex := 1
	if int(n.appliedIndex) > snapshotCatchUpEntriesN {
		compactIndex = int(n.appliedIndex) - snapshotCatchUpEntriesN
	}

	if err := n.storage.CleanupSnapshots(compactIndex); err != nil {
		panic(err)
	}

	n.logger.Sugar().Infof("compacted log at index %d", compactIndex)
	n.snapshotIndex = n.appliedIndex
}

// serveChannels handles the main event loop
func (n *node) serveChannels() {
	snap, err := n.storage.Snapshot()
	if err != nil {
		n.logger.Fatal("get snapshot", zap.Error(err))
	}

	n.mu.Lock()
	n.confState = snap.Metadata.ConfState
	n.snapshotIndex = snap.Metadata.Index
	n.appliedIndex = snap.Metadata.Index
	n.mu.Unlock()

	// Start handling proposals in a separate goroutine
	go n.serveProposals()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			n.raftNode.Tick()

			// Check leadership status
			isLeader := n.IsLeader()
			if isLeader != n.lastLeader {
				n.lastLeader = isLeader
				if n.leaderProcess != nil {
					n.leaderProcess.OnLeadershipChanged(isLeader)
				}
			}

		// store raft entries to wal, then publish over commit channel
		case rd := <-n.raftNode.Ready():
			// Check leadership status
			isLeader := n.IsLeader()
			if isLeader != n.lastLeader {
				n.lastLeader = isLeader
				if n.leaderProcess != nil {
					n.leaderProcess.OnLeadershipChanged(isLeader)
				}
			}

			// Save entries and HardState before sending messages
			if err := n.storage.Append(rd.Entries); err != nil {
				n.logger.Fatal("failed to append entries", zap.Error(err))
			}

			if !raft.IsEmptyHardState(rd.HardState) {
				if err := n.storage.SetHardState(rd.HardState); err != nil {
					n.logger.Fatal("failed to set hard state", zap.Error(err))
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				n.logger.Info("applying snapshot",
					zap.Uint64("index", rd.Snapshot.Metadata.Index))
				if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
					n.logger.Fatal("failed to apply snapshot", zap.Error(err))
				}
				n.publishSnapshot(rd.Snapshot)
			}

			// Send messages only after entries are saved
			n.transport.Send(rd.Messages)

			if len(rd.CommittedEntries) > 0 {
				applyDoneC, ok := n.publishEntries(n.entriesToApply(rd.CommittedEntries))
				if !ok {
					n.Stop()
					return
				}

				n.maybeTriggerSnapshot(applyDoneC)
			}

			// Signal readiness for next batch
			n.raftNode.Advance()

		case err := <-n.transport.ErrorC:
			n.logger.Fatal("transport error", zap.Error(err))

		case <-n.stopc:
			n.Stop()
			return
		}
	}
}
