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
	"errors"
	"fmt"
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

	"github.com/3vilhamster/kv-raft-cass/discovery"
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

	// Add these new fields
	address          string              // Local node address (hostname:port)
	discovery        discovery.Discovery // Discovery service
	bootstrapAllowed bool                // Whether this node can bootstrap if no other n
}

var defaultSnapshotCount uint64 = 10000

type raftNodeParams struct {
	ID               int
	RaftPort         int
	Address          string
	Logger           *zap.Logger
	LeaderProcess    LeaderProcess
	Storage          storage.Raft
	Discovery        discovery.Discovery
	Join             bool
	BootstrapAllowed bool
	GetSnapshot      func() ([]byte, error)
	ProposeC         <-chan string
	ConfChangeC      <-chan raftpb.ConfChange
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(p raftNodeParams) (*raftNode, <-chan *commit, <-chan error) {
	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		raftPort: p.RaftPort,
		address:  p.Address,

		proposeC:    p.ProposeC,
		confChangeC: p.ConfChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          p.ID,
		join:        p.Join,
		getSnapshot: p.GetSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger:           p.Logger,
		discovery:        p.Discovery,
		bootstrapAllowed: p.BootstrapAllowed,
		raftStorage:      p.Storage,
		leaderProcess:    p.LeaderProcess,
	}
	go rc.startRaft()
	return rc, commitC, errorC
}

func (rc *raftNode) getLocalURL() string {
	return fmt.Sprintf("http://%s", rc.address)
}

func (rc *raftNode) getLeaderURL() (string, error) {
	// Check if we're the leader
	if rc.isLeader() {
		return rc.getLocalURL(), nil
	}

	// Get leader ID from Raft state
	leaderID := rc.node.Status().Lead
	if leaderID == 0 {
		return "", errors.New("no leader elected yet")
	}

	// We need to find peer URLs manually since we don't have direct access
	// to the URLs through the transport API
	leaderTypeID := types.ID(leaderID)

	// Check if this peer is active
	activeSince := rc.transport.ActiveSince(leaderTypeID)
	if !activeSince.IsZero() {
		// This peer is active, but we need to get its URL
		// In the initialize transport code, URLs should have been mapped to this ID
		// We need to check our peers list
		for i, peer := range rc.peers {
			if i+1 == int(leaderID) {
				return peer, nil
			}
		}
	}

	// If not found in transport, try discovery
	if rc.discovery != nil {
		nodes, err := rc.discovery.GetClusterNodes()
		if err == nil && len(nodes) > 0 {
			// If we're using discovery and can't find the leader in our transport,
			// return an error so the client can retry or try a different node
			return "", errors.New("leader not found in transport, try another node")
		}
	}

	return "", errors.New("leader URL not found")
}

func (rc *raftNode) isLeader() bool {
	if rc.node == nil {
		return false
	}
	return rc.node.Status().Lead == uint64(rc.id)
}

func (rc *raftNode) joinCluster() error {
	if rc.discovery == nil {
		return errors.New("no discovery service configured")
	}

	// Use the discovery service to join the cluster
	return rc.discovery.JoinCluster(uint64(rc.id), rc.address, 500*time.Millisecond, 10)
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

			// Apply the conf change
			rc.confState = *rc.node.ApplyConfChange(cc)

			// When a node is added, update the transport layer
			if cc.Type == raftpb.ConfChangeAddNode && cc.NodeID != uint64(rc.id) {
				nodeURL := string(cc.Context)
				if nodeURL != "" {
					rc.logger.Info("adding peer to transport",
						zap.Uint64("node_id", cc.NodeID),
						zap.String("url", nodeURL))
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{nodeURL})
				}
			} else if cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID != uint64(rc.id) {
				// Remove the peer from transport
				rc.logger.Info("removing peer from transport",
					zap.Uint64("node_id", cc.NodeID))
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}

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

		// Try to join the cluster using discovery
		if err := rc.joinCluster(); err != nil {
			rc.logger.Error("failed to join cluster", zap.Error(err))
			// We'll continue anyway and retry later
		}
	} else {
		// Check if we should bootstrap or join
		shouldBootstrap := true

		if rc.discovery != nil {
			// Try to find existing nodes
			nodes, err := rc.discovery.GetClusterNodes()
			if err == nil && len(nodes) > 0 {
				// Other nodes exist, we should join instead
				rc.logger.Info("existing nodes found, switching to join mode",
					zap.Int("found_nodes", len(nodes)),
					zap.Strings("nodes", nodes))
				shouldBootstrap = false
				rc.join = true

				// Restart in joining mode
				rc.node = raft.RestartNode(c)

				// Try to join
				if err := rc.joinCluster(); err != nil {
					rc.logger.Error("failed to join existing cluster", zap.Error(err))
					// We'll continue anyway
				}
			}
		}

		if shouldBootstrap {
			if !rc.bootstrapAllowed {
				rc.logger.Fatal("attempted to bootstrap but not allowed")
			}

			// Bootstrap as single-node cluster
			peers := make([]raft.Peer, 1)
			peers[0] = raft.Peer{ID: uint64(rc.id)}

			rc.logger.Info("bootstrapping new cluster",
				zap.Int("id", rc.id),
				zap.Int("peer_count", len(peers)))

			rc.node = raft.StartNode(c, peers)
		}
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

	// For joining node, use discovery if available
	if rc.join {
		// If we're joining and have discovery, we don't need to add any peers yet
		// Discovery will handle finding peers during the join process
		if rc.discovery != nil {
			rc.logger.Info("joining via discovery, waiting for cluster nodes")
			return
		}

		// For backward compatibility with old tests that don't use discovery
		// Only attempt to add peer from rc.peers if the slice is not empty
		if len(rc.peers) > 0 {
			rc.logger.Info("joining using static peer list",
				zap.String("leader_url", rc.peers[0]))
			rc.transport.AddPeer(types.ID(1), []string{rc.peers[0]})
		} else {
			rc.logger.Warn("joining but no peers or discovery available")
		}
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
