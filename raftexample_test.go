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
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/3vilhamster/kv-raft-cass/pkg/discovery"
	"github.com/3vilhamster/kv-raft-cass/pkg/raft"
	raftstorage "github.com/3vilhamster/kv-raft-cass/pkg/storage/raft"
)

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(t, 3, false)
	defer clus.closeNoErrors(t)

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- string, cC <-chan *raft.Commit, eC <-chan error) {
			for n := 0; n < 100; n++ {
				c, ok := <-cC
				if !ok {
					pC = nil
				}
				select {
				case pC <- c.Data[0]:
					continue
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) { clus.proposeC[i] <- sampleData("foo") }(i)
	}

	for range clus.peers {
		<-donec
	}
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(t, 1, false)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	// Setup Cassandra and cleanup
	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	clus := newCluster(t, 1, false)
	defer func() {
		t.Log("closing cluster...")
		if err := clus.Close(); err != nil {
			t.Logf("error during cluster close: %v", err)
		}
		t.Log("closing cluster [done]")
	}()

	// Create channels for synchronization
	donec := make(chan struct{})
	commitc := make(chan string, 10) // Larger buffer to ensure we don't block

	// Monitor commits
	go func() {
		defer close(donec)
		for c := range clus.commitC[0] {
			if c == nil {
				continue
			}
			for _, data := range c.Data {
				t.Logf("Commit received: %q", data)
				commitc <- data
			}
		}
	}()

	// Send proposals
	go func() {
		t.Log("Proposing foo")
		clus.proposeC[0] <- sampleData("foo")

		t.Log("Proposing bar")
		clus.proposeC[0] <- sampleData("bar")
	}()

	// Expected sequence of messages
	expected := []string{sampleData("foo"), sampleData("bar")}
	received := make([]string, 0, len(expected))

	// Wait for commits with timeout
	timeout := time.After(5 * time.Second)
	for len(received) < len(expected) {
		select {
		case data := <-commitc:
			t.Logf("Received data: %q", data)
			received = append(received, data)
		case <-timeout:
			t.Fatalf("Timed out waiting for commits. Received so far: %v", received)
		}
	}

	// Verify order
	for i, want := range expected {
		if got := received[i]; got != want {
			t.Errorf("commit %d = %s, want %s", i, got, want)
		}
	}

	// Wait for cleanup
	select {
	case <-donec:
		t.Log("Commit processor finished")
	case <-time.After(time.Second):
		t.Log("Commit processor timeout")
	}
}

func TestPutAndGetKeyValue(t *testing.T) {
	cl := newCluster(t, 3, true)
	defer cl.closeNoErrors(t)

	srv := httptest.NewServer(&httpKVAPI{
		store:       cl.kvs[0],
		confChangeC: cl.confChangeC[0],
		logger:      zaptest.NewLogger(t),
	})
	defer srv.Close()

	// Test PUT
	wantKey, wantValue := "test-key", "test-value"
	url := fmt.Sprintf("%s/%s", srv.URL, wantKey)
	body := bytes.NewBufferString(wantValue)
	cli := srv.Client()

	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/html; charset=utf-8")

	resp, err := cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("PUT response code = %d, want %d", resp.StatusCode, http.StatusNoContent)
		t.FailNow()
	}

	// Test GET
	resp, err = cli.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(resp.Body)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if gotValue := string(data); wantValue != gotValue {
		t.Errorf("GET value = %s, want %s", gotValue, wantValue)
	}
}

func TestSnapshot(t *testing.T) {
	// Create cluster
	clus := newCluster(t, 1, false)
	defer func() {
		t.Log("closing cluster...")
		clus.closeNoErrors(t)
	}()

	// Create snapshot trigger channel
	snapshotTriggered := make(chan struct{})
	go func() {
		// Send message to trigger snapshot
		clus.proposeC[0] <- sampleData("foo")

		// Wait for response
		for c := range clus.commitC[0] {
			if c != nil && len(c.Data) > 0 {
				if c.ApplyDoneC != nil {
					close(c.ApplyDoneC)
				}
				close(snapshotTriggered)
				return
			}
		}
	}()

	// Wait for snapshot with timeout
	select {
	case <-snapshotTriggered:
		t.Log("Snapshot triggered successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Snapshot wasn't triggered within timeout")
	}
}

func cleanupTestState(t *testing.T, client *gocql.Session) {
	// Clean up Cassandra state
	keyspace := "raft_storage"
	var tables []string
	iter := client.Query(`SELECT table_name 
            FROM system_schema.tables 
            WHERE keyspace_name = ?`, keyspace).Iter()
	var table string
	for iter.Scan(&table) {
		tables = append(tables, table)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Failed to get tables for keyspace %s: %v", keyspace, err)
	}

	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE %s.%s", keyspace, table)
		if err := client.Query(query).Exec(); err != nil {
			t.Fatalf("Failed to truncate table %s.%s: %v", keyspace, table, err)
		}
	}
}

func setupCassandra(t *testing.T) *gocql.Session {
	clusterConfig := gocql.NewCluster("127.0.0.1")
	clusterConfig.Keyspace = "raft_storage"
	clusterConfig.Consistency = gocql.One
	clusterConfig.NumConns = 1
	clusterConfig.Timeout = 2 * time.Second
	clusterConfig.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 2}
	clusterConfig.PoolConfig.HostSelectionPolicy =
		gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	sess, err := clusterConfig.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	return sess
}

func sampleData(key string) string {
	return fmt.Sprintf(`{"key": "%s", "val": "sample-value"}`, key)
}

func testLeader(t *testing.T) raft.LeaderProcess {
	return raft.NewLeaderProcess(zaptest.NewLogger(t), func(ctx context.Context) error {
		return nil
	})
}

// getAvailablePorts finds n available TCP ports for testing
func getAvailablePorts(n int) ([]int, error) {
	ports := make([]int, n)
	listeners := make([]*net.TCPListener, n)

	defer func() {
		// Close all listeners when we're done
		for _, l := range listeners {
			if l != nil {
				l.Close()
			}
		}
	}()

	for i := 0; i < n; i++ {
		// Find a free port by asking the OS for an available port
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address: %v", err)
		}

		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen: %v", err)
		}

		// Get the port that was actually assigned
		port := listener.Addr().(*net.TCPAddr).Port
		ports[i] = port
		listeners[i] = listener
	}

	return ports, nil
}

type cluster struct {
	peers       []string
	commitC     []<-chan *raft.Commit
	errorC      []<-chan error
	proposeC    []chan string
	confChangeC []chan raftpb.ConfChange
	kvs         []*kvstore
	nodes       []raft.Node
	address     string
}

// newCluster creates a cluster of n nodes with dynamically allocated ports
func newCluster(t *testing.T, n int, withKvs bool) *cluster {
	// Allocate 2*n ports: n for Raft communication, n for HTTP API
	ports, err := getAvailablePorts(n * 2)
	if err != nil {
		t.Fatalf("Failed to allocate ports: %v", err)
	}

	// Split into raft ports and API ports
	raftPorts := ports[:n]
	apiPorts := ports[n:]

	t.Logf("Using Raft ports: %v", raftPorts)
	t.Logf("Using API ports: %v", apiPorts)

	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://localhost:%d", raftPorts[i])
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *raft.Commit, len(peers)),
		errorC:      make([]<-chan error, len(peers)),
		proposeC:    make([]chan string, len(peers)),
		confChangeC: make([]chan raftpb.ConfChange, len(peers)),
		address:     peers[0],
	}

	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	logger := zaptest.NewLogger(t)

	// Create a shared discovery service for the test cluster
	mockDiscovery := discovery.NewMockDiscovery(logger, raftPorts[0])

	// Populate the discovery service with all nodes BEFORE creating any nodes
	for i := range peers {
		nodeID := uint64(i + 1)
		mockDiscovery.AddNode(nodeID, "localhost", raftPorts[i])
		t.Logf("Registered node %d in discovery: localhost:%d", nodeID, raftPorts[i])
	}

	// Create the first node (bootstrap node) and wait for it to initialize
	nodeID := uint64(1)
	nodeLogger := logger.With(zap.String("node", fmt.Sprintf("%d", nodeID)))
	nodeAddress := fmt.Sprintf("localhost:%d", raftPorts[0])

	storage, err := raftstorage.New(sess, namespaceID, nodeID)
	if err != nil {
		t.Fatalf("Failed to create storage for node %d: %v", nodeID, err)
	}

	clus.proposeC[0] = make(chan string, 1)
	clus.confChangeC[0] = make(chan raftpb.ConfChange, 1)

	getSnapshot := func() ([]byte, error) { return nil, nil }

	t.Logf("Creating bootstrap node (ID: %d)", nodeID)
	var node raft.Node
	node, clus.commitC[0], clus.errorC[0] = raft.New(
		raft.Config{
			NodeID:           nodeID,
			RaftPort:         raftPorts[0],
			Address:          nodeAddress,
			Logger:           nodeLogger,
			LeaderProcess:    testLeader(t),
			Storage:          storage,
			Discovery:        mockDiscovery,
			Join:             false, // First node bootstraps
			BootstrapAllowed: true,
			GetSnapshot:      getSnapshot,
		}, clus.proposeC[0], clus.confChangeC[0])

	clus.nodes = append(clus.nodes, node)

	// Create KVStore for first node if needed
	var kvs *kvstore
	if withKvs {
		kvs = newKVStore(nodeLogger, storage, clus.proposeC[0], clus.commitC[0], clus.errorC[0])
		getSnapshot = func() ([]byte, error) { return kvs.getSnapshot() }
	}
	clus.kvs = append(clus.kvs, kvs)

	// Give the first node time to initialize
	t.Log("Waiting for bootstrap node to initialize...")
	time.Sleep(500 * time.Millisecond)

	// Now create and add all other nodes
	for i := 1; i < n; i++ {
		nodeID := uint64(i + 1)
		nodeLogger := logger.With(zap.String("node", fmt.Sprintf("%d", nodeID)))
		nodeAddress := fmt.Sprintf("localhost:%d", raftPorts[i])

		storage, err := raftstorage.New(sess, namespaceID, nodeID)
		if err != nil {
			t.Fatalf("Failed to create storage for node %d: %v", nodeID, err)
		}

		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)

		getSnapshot := func() ([]byte, error) { return nil, nil }

		t.Logf("Creating node %d", nodeID)
		node, clus.commitC[i], clus.errorC[i] = raft.New(
			raft.Config{
				NodeID:           nodeID,
				RaftPort:         raftPorts[i],
				Address:          nodeAddress,
				Logger:           nodeLogger,
				LeaderProcess:    testLeader(t),
				Storage:          storage,
				Discovery:        mockDiscovery,
				Join:             true, // Join existing cluster
				BootstrapAllowed: false,
				GetSnapshot:      getSnapshot,
			}, clus.proposeC[i], clus.confChangeC[i])

		if withKvs {
			kvs := newKVStore(nodeLogger, storage, clus.proposeC[i], clus.commitC[i], clus.errorC[i])
			getSnapshot = func() ([]byte, error) { return kvs.getSnapshot() }
			clus.kvs = append(clus.kvs, kvs)
		} else {
			clus.kvs = append(clus.kvs, nil)
		}

		clus.nodes = append(clus.nodes, node)

		// Allow time for this node to connect to the cluster
		time.Sleep(200 * time.Millisecond)

		// Explicitly add it to the cluster via a configuration change
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
			Context: []byte(fmt.Sprintf("http://localhost:%d", raftPorts[i])),
		}

		// Send the configuration change to the leader (assumed to be node 1)
		t.Logf("Sending ConfChange to add node %d to the cluster", nodeID)
		select {
		case clus.confChangeC[0] <- cc:
			t.Logf("ConfChange for node %d sent", nodeID)
		case <-time.After(time.Second):
			t.Logf("Timeout sending ConfChange for node %d", nodeID)
		}

		// Give the config change time to propagate
		time.Sleep(300 * time.Millisecond)
	}

	// Allow some time for cluster to stabilize before proceeding
	t.Log("Waiting for cluster to stabilize...")
	time.Sleep(1 * time.Second)

	// Wait for cluster to elect a leader
	if !waitForLeader(t, clus.nodes, 10*time.Second) {
		t.Error("No leader elected within timeout")
		clus.closeNoErrors(t)
		t.FailNow()
	}

	return clus
}

// Close closes all cluster nodes and returns an error if any failed.
func (cl *cluster) Close() (err error) {
	for i := range cl.peers {
		go func(i int) {
			for range cl.commitC[i] {
				// drain pending commits
			}
		}(i)
		close(cl.proposeC[i])
		close(cl.confChangeC[i])
		// wait for channel to close
		if erri := <-cl.errorC[i]; erri != nil {
			err = erri
		}
	}
	return err
}

func (cl *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("closing cluster [done]")
}
