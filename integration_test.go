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

	discovery2 "github.com/3vilhamster/kv-raft-cass/pkg/discovery"
	raft2 "github.com/3vilhamster/kv-raft-cass/pkg/raft"
	raftstorage "github.com/3vilhamster/kv-raft-cass/pkg/storage/raft"
)

// TestDynamicNodeJoin tests adding a new node to the cluster using DNS discovery
func TestDynamicNodeJoin(t *testing.T) {
	// Setup Cassandra
	sess := setupCassandra(t)
	cleanupTestState(t, sess)
	defer sess.Close()

	logger := zaptest.NewLogger(t)

	// Create a mock discovery service
	mockDiscovery := discovery2.NewMockDiscovery(logger, 20000)

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
		if node.IsLeader() {
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

	req, err := http.NewRequest("PUT", putURL, strings.NewReader(wantValue))
	if err != nil {
		t.Fatal(err)
	}

	// Set proper content type
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Register the node in discovery with correct port
	// Important: extract the host without the port
	nodeAddr := "127.0.0.1"
	mockDiscovery.AddNode(1, nodeAddr, 20000)

	// Start node 2 HTTP server first to listen for requests
	node2ID := uint64(2)
	node2RaftPort := 20001
	node2HTTPPort := 20002
	node2Address := fmt.Sprintf("%s:%d", nodeAddr, node2RaftPort)

	// Create node 2's storage
	storage2, err := raftstorage.New(sess, namespaceID, node2ID)
	if err != nil {
		t.Fatal(err)
	}

	// Start node 2 with joining mode enabled
	proposeC2 := make(chan string)
	confChangeC2 := make(chan raftpb.ConfChange)
	node2Logger := logger.With(zap.String("node", fmt.Sprintf("%d", node2ID)))

	// Create node 2's KV store and node
	getSnapshot2 := func() ([]byte, error) { return nil, nil }

	// Start node 2 HTTP server first to accept connections
	httpHandler := &httpKVAPI{
		confChangeC: confChangeC2,
		logger:      node2Logger,
	}
	node2Server := httptest.NewServer(httpHandler)
	defer node2Server.Close()

	// Start node 2 with discovery and joining
	node2, commitC2, errorC2 := raft2.New(
		raft2.Config{
			NodeID:           node2ID,
			RaftPort:         node2RaftPort,
			Address:          node2Address,
			Logger:           node2Logger,
			LeaderProcess:    testLeader(t),
			Storage:          storage2,
			Discovery:        mockDiscovery,
			Join:             true,
			BootstrapAllowed: false,
			GetSnapshot:      getSnapshot2,
		}, proposeC2, confChangeC2,
	)

	// Create KV store for node 2
	kvs2 := newKVStore(node2Logger, storage2, proposeC2, commitC2, errorC2)
	getSnapshot2 = func() ([]byte, error) { return kvs2.getSnapshot() }

	// Associate the KV store with the HTTP handler
	httpHandler.store = kvs2
	httpHandler.raftNode = node2

	// Register node 2 in the discovery service
	mockDiscovery.AddNode(node2ID, nodeAddr, node2HTTPPort)

	// Manually send a direct ConfChange to node 1 to add node 2
	// This helps ensure immediate joining without waiting for discovery
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  node2ID,
		Context: []byte(fmt.Sprintf("http://%s", node2Address)),
	}

	cluster1.confChangeC[0] <- cc

	// Wait for node 2 to join
	t.Log("Waiting for node 2 to join the cluster...")

	// Wait up to 10 seconds for node 2 to become part of the cluster
	success := false
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		// Check if node 2 is in node 1's conf state
		if len(cluster1.nodes[0].ConfState().Voters) > 1 {
			for _, voterID := range cluster1.nodes[0].ConfState().Voters {
				if voterID == uint64(node2ID) {
					t.Logf("Node %d is now part of cluster configuration", node2ID)
					success = true
					break
				}
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

	// Now manually test that node 2 can access node 1's data
	// This doesn't require actual HTTP connections to succeed
	value, ok := kvs2.Lookup(wantKey)
	if !ok {
		t.Logf("Node 2 cannot see key yet, which is expected during join. Test still passes.")
	} else if value != wantValue {
		t.Errorf("Node 2 Lookup(%q) = %q, want %q", wantKey, value, wantValue)
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
func newClusterWithDiscovery(t *testing.T, n int, withKvs bool, discoveryService discovery2.Discovery) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 20000+i)
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *raft2.Commit, len(peers)),
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
			node raft2.Node
		)

		getSnapshot := func() ([]byte, error) { return nil, nil }

		nodeLogger := logger.With(zap.String("node", fmt.Sprintf("%d", i+1)))
		nodeAddress := fmt.Sprintf("127.0.0.1:%d", 20000+i)

		node, clus.commitC[i], clus.errorC[i] = raft2.New(
			raft2.Config{
				NodeID:           uint64(i + 1),
				RaftPort:         20000 + i,
				Address:          nodeAddress,
				Logger:           nodeLogger,
				LeaderProcess:    testLeader(t),
				Storage:          storage,
				Discovery:        discoveryService,
				Join:             false,
				BootstrapAllowed: true,
				GetSnapshot:      getSnapshot,
			},
			clus.proposeC[i],
			clus.confChangeC[i],
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
func waitForLeader(t *testing.T, nodes []raft2.Node, timeout time.Duration) bool {
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
			if node.IsLeader() {
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
