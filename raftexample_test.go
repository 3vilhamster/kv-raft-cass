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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap/zaptest"

	raftstorage "github.com/3vilhamster/kv-raft-cass/storage/raft"
)

func getSnapshotFn() (func() ([]byte, error), <-chan struct{}) {
	snapshotTriggeredC := make(chan struct{})
	return func() ([]byte, error) {
		snapshotTriggeredC <- struct{}{}
		return nil, nil
	}, snapshotTriggeredC
}

type cluster struct {
	peers              []string
	commitC            []<-chan *commit
	errorC             []<-chan error
	proposeC           []chan string
	confChangeC        []chan raftpb.ConfChange
	snapshotTriggeredC []<-chan struct{}
}

// newCluster creates a cluster of n nodes
func newCluster(t *testing.T, n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 20000+i)
	}

	clus := &cluster{
		peers:              peers,
		commitC:            make([]<-chan *commit, len(peers)),
		errorC:             make([]<-chan error, len(peers)),
		proposeC:           make([]chan string, len(peers)),
		confChangeC:        make([]chan raftpb.ConfChange, len(peers)),
		snapshotTriggeredC: make([]<-chan struct{}, len(peers)),
	}

	sess := setupCassandra(t)

	cleanupTestState(t, sess)

	for i := range clus.peers {
		err := os.RemoveAll(fmt.Sprintf(".wal/raftexample-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
		fn, snapshotTriggeredC := getSnapshotFn()
		clus.snapshotTriggeredC[i] = snapshotTriggeredC

		storage, err := raftstorage.New(sess, namespaceID, uint64(i+1))
		if err != nil {
			panic(err)
		}

		clus.commitC[i], clus.errorC[i] = newRaftNode(i+1, zaptest.NewLogger(t), storage, clus.peers, false, fn, clus.proposeC[i], clus.confChangeC[i])
	}

	return clus
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		go func(i int) {
			for range clus.commitC[i] {
				// drain pending commits
			}
		}(i)
		close(clus.proposeC[i])
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
		// clean intermediates
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	if err := clus.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("closing cluster [done]")
}

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- string, cC <-chan *commit, eC <-chan error) {
			for n := 0; n < 100; n++ {
				c, ok := <-cC
				if !ok {
					pC = nil
				}
				select {
				case pC <- c.data[0]:
					continue
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
			for range cC {
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) { clus.proposeC[i] <- "foo" }(i)
	}

	for range clus.peers {
		<-donec
	}
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(t, 1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	// Setup Cassandra and cleanup
	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	clus := newCluster(t, 1)
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
			for _, data := range c.data {
				t.Logf("Commit received: %q", data)
				commitc <- data
			}
			if c.applyDoneC != nil {
				close(c.applyDoneC)
			}
		}
	}()

	// Send proposals
	go func() {
		t.Log("Proposing foo")
		clus.proposeC[0] <- "foo"
		// Ensure first proposal is processed
		time.Sleep(100 * time.Millisecond)
		t.Log("Proposing bar")
		clus.proposeC[0] <- "bar"
	}()

	// Expected sequence of messages
	expected := []string{"foo", "bar"}
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
	clusters := []string{"http://127.0.0.1:9021"}

	proposeC := make(chan string)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	sess := setupCassandra(t)

	// Clean up state before test
	cleanupTestState(t, sess)

	storage, err := raftstorage.New(sess, namespaceID, 1)
	if err != nil {
		t.Fatal(err)
	}

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC := newRaftNode(1, zaptest.NewLogger(t), storage, clusters, false, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(storage, proposeC, commitC, errorC)
	if kvs == nil {
		t.Fatal("Failed to create kvstore")
	}

	srv := httptest.NewServer(&httpKVAPI{
		store:       kvs,
		confChangeC: confChangeC,
	})
	defer srv.Close()

	// Wait for server to start and initialize
	time.Sleep(3 * time.Second)

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("PUT response code = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Wait for processing
	time.Sleep(time.Second)

	// Test GET
	resp, err = cli.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if gotValue := string(data); wantValue != gotValue {
		t.Errorf("GET value = %s, want %s", gotValue, wantValue)
	}
}

// TestAddNewNode tests adding new node to the existing cluster.
func TestAddNewNode(t *testing.T) {
	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	// Start with a single-node cluster for simplicity
	clus := newCluster(t, 1)
	defer func() {
		t.Log("closing cluster...")
		if err := clus.Close(); err != nil {
			t.Logf("error during cluster close: %v", err)
		}
	}()

	// Wait for the first node to become leader
	time.Sleep(1 * time.Second)

	// First confirm the cluster has stabilized
	t.Log("Waiting for cluster to stabilize...")
	stabilized := make(chan struct{})
	go func() {
		// Send a test message to confirm cluster is working
		clus.proposeC[0] <- "test"
		for commit := range clus.commitC[0] {
			if commit != nil && len(commit.data) > 0 && commit.data[0] == "test" {
				close(stabilized)
				return
			}
		}
	}()

	select {
	case <-stabilized:
		t.Log("Cluster stabilized")
	case <-time.After(5 * time.Second):
		t.Fatal("Cluster failed to stabilize")
	}

	// Prepare new node
	t.Log("Adding new node")
	newNodeURL := "http://127.0.0.1:20003"
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  2, // Start with node 2 since we have a single-node cluster
		Context: []byte(newNodeURL),
	}

	confChangeC := clus.confChangeC[0]
	confChangeC <- cc

	// Wait for conf change to be applied
	confChangeApplied := make(chan struct{})
	go func() {
		for commit := range clus.commitC[0] {
			if commit != nil && len(commit.data) == 0 {
				// Empty commit signifies conf change
				close(confChangeApplied)
				return
			}
		}
	}()

	select {
	case <-confChangeApplied:
		t.Log("Configuration change applied")
	case <-time.After(5 * time.Second):
		t.Fatal("Configuration change not applied")
	}

	t.Log("Starting new node")
	storage2, err := raftstorage.New(sess, namespaceID, 2)
	if err != nil {
		t.Fatal(err)
	}

	proposeC2 := make(chan string)
	confChangeC2 := make(chan raftpb.ConfChange)

	// Start the new node in join mode
	peers := []string{"http://127.0.0.1:20000", newNodeURL}
	commitC2, errorC2 := newRaftNode(2, zaptest.NewLogger(t), storage2, peers, true, nil, proposeC2, confChangeC2)

	defer func() {
		close(proposeC2)
		close(confChangeC2)
		for range commitC2 {
			// Drain commits
		}
		if err := <-errorC2; err != nil {
			t.Logf("Error from node 2: %v", err)
		}
	}()

	// Wait for the new node to catch up
	time.Sleep(2 * time.Second)

	// Test if the new node is receiving updates
	t.Log("Testing cluster with new node")
	testMsg := "after-join-test"
	msgReceived := make(chan struct{})

	go func() {
		for commit := range commitC2 {
			if commit != nil && len(commit.data) > 0 && commit.data[0] == testMsg {
				close(msgReceived)
				return
			}
		}
	}()

	clus.proposeC[0] <- testMsg

	select {
	case <-msgReceived:
		t.Log("New node successfully received message")
	case <-time.After(5 * time.Second):
		t.Fatal("New node failed to receive message")
	}
}

func TestSnapshot(t *testing.T) {
	prevDefaultSnapshotCount := defaultSnapshotCount
	prevSnapshotCatchUpEntriesN := snapshotCatchUpEntriesN
	defaultSnapshotCount = 4
	snapshotCatchUpEntriesN = 4
	defer func() {
		defaultSnapshotCount = prevDefaultSnapshotCount
		snapshotCatchUpEntriesN = prevSnapshotCatchUpEntriesN
	}()

	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)

	go func() {
		clus.proposeC[0] <- "foo"
	}()

	c := <-clus.commitC[0]

	select {
	case <-clus.snapshotTriggeredC[0]:
		t.Fatalf("snapshot triggered before applying done")
	default:
	}
	close(c.applyDoneC)
	<-clus.snapshotTriggeredC[0]
}

func cleanupTestState(t *testing.T, client *gocql.Session) {
	// Clean up WAL directory first
	// First remove contents
	walDir := ".wal"
	if entries, err := os.ReadDir(walDir); err == nil {
		for _, entry := range entries {
			path := filepath.Join(walDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				t.Logf("Warning: couldn't remove %s: %v", path, err)
			}
		}
	}
	// Then remove directory itself
	if err := os.RemoveAll(walDir); err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: couldn't remove directory %s: %v", walDir, err)
	}

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

func TestRecoveryFromWAL(t *testing.T) {
	// Setup initial conditions
	sess := setupCassandra(t)
	cleanupTestState(t, sess)

	// Step 1: Start a node and make some changes
	clusters := []string{"http://127.0.0.1:9021"}
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	storage, err := raftstorage.New(sess, namespaceID, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Create first kvstore instance
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC := newRaftNode(1, zaptest.NewLogger(t), storage, clusters, false, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(storage, proposeC, commitC, errorC)
	if kvs == nil {
		t.Fatal("Failed to create kvstore")
	}

	// Make some changes
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	// Write data
	for k, v := range testData {
		kvs.Propose(k, v)
	}

	// Wait for changes to be applied
	time.Sleep(2 * time.Second)

	// Verify data was written
	for k, want := range testData {
		if got, ok := kvs.Lookup(k); !ok || got != want {
			t.Errorf("Before restart: key %s = %s, want %s", k, got, want)
		}
	}

	// Close channels to shut down the first node
	close(proposeC)
	close(confChangeC)

	// Wait for shutdown
	select {
	case err := <-errorC:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Error channel did not close")
	}

	// Step 2: Start a new node with the same storage
	proposeC2 := make(chan string)
	confChangeC2 := make(chan raftpb.ConfChange)
	defer close(proposeC2)
	defer close(confChangeC2)

	storage2, err := raftstorage.New(sess, namespaceID, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Create second kvstore instance
	var kvs2 *kvstore
	getSnapshot2 := func() ([]byte, error) { return kvs2.getSnapshot() }
	commitC2, errorC2 := newRaftNode(1, zaptest.NewLogger(t), storage2, clusters, true, getSnapshot2, proposeC2, confChangeC2)

	kvs2 = newKVStore(storage2, proposeC2, commitC2, errorC2)
	if kvs2 == nil {
		t.Fatal("Failed to create second kvstore")
	}

	// Wait for recovery
	time.Sleep(2 * time.Second)

	// Verify all data was recovered
	for k, want := range testData {
		if got, ok := kvs2.Lookup(k); !ok || got != want {
			t.Errorf("After restart: key %s = %s, want %s", k, got, want)
		}
	}

	// Test we can write new data
	newData := map[string]string{
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range newData {
		kvs2.Propose(k, v)
	}

	// Wait for new changes to be applied
	time.Sleep(2 * time.Second)

	// Verify new data
	for k, want := range newData {
		if got, ok := kvs2.Lookup(k); !ok || got != want {
			t.Errorf("New data after restart: key %s = %s, want %s", k, got, want)
		}
	}

	// Also verify old data is still there
	for k, want := range testData {
		if got, ok := kvs2.Lookup(k); !ok || got != want {
			t.Errorf("Old data after restart: key %s = %s, want %s", k, got, want)
		}
	}
}
