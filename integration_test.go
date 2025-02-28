package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/3vilhamster/kv-raft-cass/discovery"

	raftstorage "github.com/3vilhamster/kv-raft-cass/storage/raft"
)

// TestDynamicNodeJoin tests adding a new node to the cluster using DNS discovery
func TestDynamicNodeJoin(t *testing.T) {
	// Setup Cassandra
	sess := setupCassandra(t)
	cleanupTestState(t, sess)
	defer sess.Close()

	logger := zaptest.NewLogger(t)

	// Create a mock discovery service
	mockDiscovery := discovery.NewMockDiscovery(logger, 20000)

	// Start a single-node cluster first
	cluster1 := newClusterWithDiscovery(t, 1, true, mockDiscovery)
	defer cluster1.closeNoErrors(t)

	// Test initial leader election
	if !waitForLeader(t, cluster1.nodes, 5*time.Second) {
		t.Fatal("No leader elected in initial cluster")
	}

	// Get the leader ID
	var leaderID int
	for i, node := range cluster1.nodes {
		if node.isLeader() {
			leaderID = i + 1
		}
	}
	t.Logf("Leader elected: Node %d", leaderID)

	// Put a test key to make sure the cluster is working
	srv := httptest.NewServer(&httpKVAPI{
		store:       cluster1.kvs[0],
		confChangeC: cluster1.confChangeC[0],
		raftNode:    cluster1.nodes[0],
		logger:      logger,
	})
	defer srv.Close()

	// Test PUT
	wantKey, wantValue := "test-key", "test-value"
	putURL := fmt.Sprintf("%s/%s", srv.URL, wantKey)
	resp, err := http.Post(putURL, "application/json", strings.NewReader(wantValue))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Start a second node using discovery to join
	node2ID := 2
	node2RaftPort := 20001
	//node2HTTPPort := 20002
	node2Address := fmt.Sprintf("127.0.0.1:%d", node2RaftPort)

	// Register the first node in the discovery service
	mockDiscovery.AddNode(1, "127.0.0.1:20000", 20000)

	// Create node 2's storage
	storage2, err := raftstorage.New(sess, namespaceID, uint64(node2ID))
	if err != nil {
		t.Fatal(err)
	}

	// Start node 2 with joining mode enabled
	proposeC2 := make(chan string)
	confChangeC2 := make(chan raftpb.ConfChange)
	node2Logger := logger.With(zap.String("node", fmt.Sprintf("%d", node2ID)))

	// Create node 2's KV store
	getSnapshot2 := func() ([]byte, error) { return nil, nil }

	// Start node 2 with discovery and joining
	node2, commitC2, errorC2 := newRaftNode(
		raftNodeParams{
			ID:               node2ID,
			RaftPort:         node2RaftPort,
			Address:          node2Address,
			Logger:           node2Logger,
			LeaderProcess:    testLeader(t),
			Storage:          storage2,
			Discovery:        mockDiscovery,
			Join:             true,
			BootstrapAllowed: false,
			GetSnapshot:      getSnapshot2,
			ProposeC:         proposeC2,
			ConfChangeC:      confChangeC2,
		},
	)

	// Create KV store for node 2
	kvs2 := newKVStore(node2Logger, storage2, proposeC2, commitC2, errorC2)
	getSnapshot2 = func() ([]byte, error) { return kvs2.getSnapshot() }

	// Wait for node 2 to join
	t.Log("Waiting for node 2 to join the cluster...")

	// Wait up to 10 seconds for node 2 to become part of the cluster
	success := false
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		confState := node2.confState
		// Check if node 2 is part of the voters
		for _, voterID := range confState.Voters {
			if voterID == uint64(node2ID) {
				t.Logf("Node %d successfully joined the cluster", node2ID)
				success = true
				break
			}
		}

		if success {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if !success {
		t.Fatal("Node 2 did not join the cluster within the timeout")
	}

	// Test PUT on node 2 - this should work if node 2 joined successfully
	// For simplicity, we'll just check that node 2 can look up the key we put earlier
	value, ok := kvs2.Lookup(wantKey)
	if !ok || value != wantValue {
		t.Errorf("Node 2 Lookup(%q) = (%q, %v), want (%q, true)", wantKey, value, ok, wantValue)
	} else {
		t.Logf("Node 2 successfully read key %q = %q", wantKey, value)
	}

	// Clean up
	defer func() {
		close(proposeC2)
		close(confChangeC2)
	}()
}

// newClusterWithDiscovery creates a cluster with a custom discovery service
func newClusterWithDiscovery(t *testing.T, n int, withKvs bool, discoveryService discovery.Discovery) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 20000+i)
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *commit, len(peers)),
		errorC:      make([]<-chan error, len(peers)),
		proposeC:    make([]chan string, len(peers)),
		confChangeC: make([]chan raftpb.ConfChange, len(peers)),
		address:     peers[0],
	}

	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	logger := zaptest.NewLogger(t)

	for i := range clus.peers {
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)

		storage, err := raftstorage.New(sess, namespaceID, uint64(i+1))
		if err != nil {
			panic(err)
		}

		var (
			kvs  *kvstore
			node *raftNode
		)

		getSnapshot := func() ([]byte, error) { return nil, nil }

		nodeLogger := logger.With(zap.String("node", fmt.Sprintf("%d", i+1)))
		nodeAddress := fmt.Sprintf("127.0.0.1:%d", 20000+i)

		node, clus.commitC[i], clus.errorC[i] = newRaftNode(
			raftNodeParams{
				ID:               i + 1,
				RaftPort:         20000 + i,
				Address:          nodeAddress,
				Logger:           nodeLogger,
				LeaderProcess:    testLeader(t),
				Storage:          storage,
				Discovery:        discoveryService,
				Join:             false,
				BootstrapAllowed: true,
				GetSnapshot:      getSnapshot,
				ProposeC:         clus.proposeC[i],
				ConfChangeC:      clus.confChangeC[i],
			},
		)

		if withKvs {
			kvs = newKVStore(nodeLogger, storage, clus.proposeC[i], clus.commitC[i], clus.errorC[i])
			getSnapshot = func() ([]byte, error) { return kvs.getSnapshot() }
		}

		clus.nodes = append(clus.nodes, node)
		clus.kvs = append(clus.kvs, kvs)
	}

	waitForLeader(t, clus.nodes, 5*time.Second)
	return clus
}

// waitForLeader waits for a leader to be elected in the cluster
func waitForLeader(t *testing.T, nodes []*raftNode, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	firstTry := true

	for time.Now().Before(deadline) {
		if firstTry {
			// Let HTTP server start up on first try
			time.Sleep(500 * time.Millisecond)
			firstTry = false
		}

		leaderCount := 0
		var leaderId int
		for i, node := range nodes {
			if node.isLeader() {
				leaderCount++
				leaderId = i + 1
			}
		}

		if leaderCount == 1 {
			t.Logf("Leader elected: Node %d", leaderId)
			return true
		}

		if leaderCount > 1 {
			t.Logf("Multiple leaders detected: %d", leaderCount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	return false
}
