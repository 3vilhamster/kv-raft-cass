// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/storage"
)

const namespaceID = "test"

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// LeaderProcess is an interface for handling leadership changes.
type LeaderProcess interface {
	OnLeadershipChanged(isLeader bool)
}

// A key-value stream backed by raft
type raftNode struct {
	raftPort int

	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *commit           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node raft.Node

	raftStorage storage.Raft

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger

	leaderProcess LeaderProcess
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(id int, raftPort int, logger *zap.Logger, leaderProcess LeaderProcess, storage storage.Raft, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (*raftNode, <-chan *commit, <-chan error) {

	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		raftPort: raftPort,

		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger: logger,

		raftStorage: storage,
	}
	go rc.startRaft()
	return rc, commitC, errorC
}

func (rc *raftNode) isLeader() bool {
	if rc.node == nil {
		return false
	}
	return rc.node.Status().Lead == uint64(rc.id)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}

	firstIdx := ents[0].Index

	// For initial entries, allow the first index to be greater
	if rc.appliedIndex == 0 {
		rc.appliedIndex = firstIdx - 1
		return ents
	}

	if firstIdx > rc.appliedIndex+1 {
		rc.logger.Sugar().Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}

	// Filter out entries we've already applied
	offset := rc.appliedIndex - firstIdx + 1
	if offset < uint64(len(ents)) {
		return ents[offset:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	var data []string
	var applyDoneC chan struct{}

	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			rc.logger.Debug("Processing entry: type=EntryNormal",
				zap.Uint64("index", ents[i].Index), zap.String("data", string(ents[i].Data)))

			if len(ents[i].Data) == 0 {
				rc.logger.Debug("Ignoring empty message")
				// Ignore empty messages
				rc.appliedIndex = ents[i].Index
				continue
			}

			// Only accept JSON data
			if !json.Valid(ents[i].Data) {
				rc.logger.Error("invalid JSON data",
					zap.String("data", string(ents[i].Data)))
				continue
			}

			rc.logger.Debug("Append data to commit data", zap.String("data", string(ents[i].Data)))
			data = append(data, string(ents[i].Data))
		case raftpb.EntryConfChange:
			rc.logger.Debug("Processing entry: type=EntryConfChange",
				zap.Uint64("index", ents[i].Index))

			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				rc.logger.Error("failed to unmarshal conf change",
					zap.Error(err),
					zap.Binary("data", ents[i].Data))
				continue
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			rc.appliedIndex = ents[i].Index
		}
	}

	if len(data) > 0 {
		applyDoneC = make(chan struct{})
		rc.logger.Debug("Sending commit",
			zap.Int("data_count", len(data)))
		select {
		case rc.commitC <- &commit{data: data, applyDoneC: applyDoneC}:
			rc.logger.Debug("Commit sent successfully",
				zap.Int("data_count", len(data)))
			rc.appliedIndex = ents[len(ents)-1].Index
		case <-rc.stopc:
			return nil, false
		}
	} else if len(ents) > 0 {
		// Update appliedIndex even if there's no data to publish
		rc.appliedIndex = ents[len(ents)-1].Index
	}

	rc.logger.Sugar().Debugf("Published entries: first=%d last=%d appliedIndex=%d data_count=%d",
		ents[0].Index, ents[len(ents)-1].Index, rc.appliedIndex, len(data))

	return applyDoneC, true
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		PreVote:                   true,
		CheckQuorum:               true,
		DisableProposalForwarding: false,
	}

	// Initialize transport before creating node
	rc.initTransport()

	if rc.join {
		rc.logger.Info("joining existing cluster", zap.Int("id", rc.id))

		// For joining nodes, we start with empty state and let Raft catch up
		rc.node = raft.RestartNode(c)

		// Add self to the transport but don't initialize as voter
		rc.transport.AddPeer(types.ID(1), []string{rc.peers[0]})
	} else {
		peers := make([]raft.Peer, 0, len(rc.peers))
		for i := range rc.peers {
			peers = append(peers, raft.Peer{ID: uint64(i + 1)})
		}

		// For single-node cluster, ensure it becomes leader immediately
		if len(peers) == 1 {
			c.ElectionTick = 2 // Speed up election for single node
			c.HeartbeatTick = 1
		}

		rc.logger.Info("starting node",
			zap.Int("id", rc.id),
			zap.Int("peer_count", len(peers)))

		rc.node = raft.StartNode(c, peers)
	}

	// Start the transport services
	go rc.serveRaft()
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.logger.Debug("stopping node")
	rc.node.Stop()
	rc.logger.Debug("stopped")
}

func (rc *raftNode) stopHTTP() {
	rc.logger.Debug("stopping http")
	rc.transport.Stop()
	rc.logger.Debug("transport stopped, closing httpstopc")
	close(rc.httpstopc)
	rc.logger.Debug("waiting for ack on httpdonec")
	<-rc.httpdonec
	rc.logger.Debug("httpdonec closed")
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	rc.logger.Sugar().Infof("publishing snapshot at index %d", rc.snapshotIndex)
	defer rc.logger.Sugar().Infof("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		rc.logger.Sugar().Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	rc.logger.Sugar().Infof("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		rc.logger.Sugar().Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err = rc.raftStorage.ApplySnapshot(snap); err != nil {
		panic(err)
	}

	compactIndex := 1
	if int(rc.appliedIndex) > snapshotCatchUpEntriesN {
		compactIndex = int(rc.appliedIndex) - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.CleanupSnapshots(compactIndex); err != nil {
		panic(err)
	}

	rc.logger.Sugar().Infof("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		rc.logger.Fatal("get snapshot", zap.Error(err))
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					rc.logger.Debug("Received proposal",
						zap.String("data", prop))

					// blocks until accepted by raft state machine
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := rc.node.Propose(ctx, []byte(prop))
					cancel()
					if err != nil {
						rc.logger.Error("failed to propose", zap.Error(err))
						continue
					}
					rc.logger.Debug("Proposal submitted successfully")
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := rc.node.ProposeConfChange(ctx, cc)
					cancel()
					if err != nil {
						rc.logger.Error("failed to propose conf change", zap.Error(err))
					}
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// For joining nodes, periodically retry peer discovery
	//if rc.join {
	//	go func() {
	//		ticker := time.NewTicker(100 * time.Millisecond)
	//		defer ticker.Stop()
	//
	//		for {
	//			select {
	//			case <-ticker.C:
	//				if rc.transport.ActivePeers() == 0 {
	//					rc.logger.Debug("retrying peer connection",
	//						zap.String("leader_url", rc.peers[0]))
	//					rc.transport.AddPeer(types.ID(1), []string{rc.peers[0]})
	//				}
	//			case <-rc.stopc:
	//				return
	//			}
	//		}
	//	}()
	//}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastLeaderStatus bool

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

			// Check leadership status
			isLeader := rc.isLeader()
			if isLeader != lastLeaderStatus {
				lastLeaderStatus = isLeader
				if rc.leaderProcess != nil {
					rc.leaderProcess.OnLeadershipChanged(isLeader)
				}
			}

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			isLeader := rc.isLeader()
			if isLeader != lastLeaderStatus {
				lastLeaderStatus = isLeader
				if rc.leaderProcess != nil {
					rc.leaderProcess.OnLeadershipChanged(isLeader)
				}
			}

			// Save entries and HardState before sending messages
			if err := rc.raftStorage.Append(rd.Entries); err != nil {
				rc.logger.Fatal("failed to append entries", zap.Error(err))
			}

			if !raft.IsEmptyHardState(rd.HardState) {
				if err := rc.raftStorage.SetHardState(rd.HardState); err != nil {
					rc.logger.Fatal("failed to set hard state", zap.Error(err))
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.logger.Info("applying snapshot",
					zap.Uint64("index", rd.Snapshot.Metadata.Index))
				if err := rc.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
					rc.logger.Fatal("failed to apply snapshot", zap.Error(err))
				}
				rc.publishSnapshot(rd.Snapshot)
			}

			// Send messages only after entries are saved
			rc.transport.Send(rd.Messages)

			if len(rd.CommittedEntries) > 0 {
				applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
				if !ok {
					rc.stop()
					return
				}

				rc.maybeTriggerSnapshot(applyDoneC)
			}

			// Signal readiness for next batch
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse("http://127.0.0.1:" + strconv.Itoa(rc.raftPort))
	if err != nil {
		rc.logger.Sugar().Fatalf(" Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		rc.logger.Sugar().Fatalf(" Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		rc.logger.Sugar().Fatalf(" Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) initTransport() {
	// Initialize transport
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(rc.logger, strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	err := rc.transport.Start()
	if err != nil {
		rc.logger.Fatal("rafthttp.Transport.Start", zap.Error(err))
	}

	// For joining node, only add the leader initially
	if rc.join {
		rc.transport.AddPeer(types.ID(1), []string{rc.peers[0]})
		return
	}

	// For existing nodes, add all known peers
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
