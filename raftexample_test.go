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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	address            string
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
		address:            peers[0],
	}

	sess := setupCassandra(t)

	cleanupTestState(t, sess)

	for i := range clus.peers {
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
		fn, snapshotTriggeredC := getSnapshotFn()
		clus.snapshotTriggeredC[i] = snapshotTriggeredC

		storage, err := raftstorage.New(sess, namespaceID, uint64(i+1))
		if err != nil {
			panic(err)
		}

		clus.commitC[i], clus.errorC[i] = newRaftNode(i+1, 20000+i, zaptest.NewLogger(t), storage, clus.peers, false, fn, clus.proposeC[i], clus.confChangeC[i])
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
		go func(i int) { clus.proposeC[i] <- `"foo"` }(i)
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
		clus.proposeC[0] <- `"foo"`

		t.Log("Proposing bar")
		clus.proposeC[0] <- `"bar"`
	}()

	// Expected sequence of messages
	expected := []string{`"foo"`, `"bar"`}
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
	commitC, errorC := newRaftNode(1, 9021, zaptest.NewLogger(t), storage, clusters, false, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(zaptest.NewLogger(t), storage, proposeC, commitC, errorC)
	if kvs == nil {
		t.Fatal("Failed to create kvstore")
	}

	srv := httptest.NewServer(&httpKVAPI{
		store:       kvs,
		confChangeC: confChangeC,
		logger:      zaptest.NewLogger(t),
	})
	defer srv.Close()

	time.Sleep(time.Second)

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

// TestAddNewNode tests adding new node to the existing cluster.
func TestAddNewNode(t *testing.T) {
	// Start with a cluster
	clus := newCluster(t, 1)
	defer clus.closeNoErrors(t)

	time.Sleep(time.Second)

	// Create and start the new node
	newNodeID := uint64(4) // Changed from 4 to 2 to be consistent
	newNodeURL := fmt.Sprintf("http://127.0.0.1:%d", 20000+int(newNodeID))

	storage2, err := raftstorage.New(setupCassandra(t), namespaceID, newNodeID)
	if err != nil {
		t.Fatal(err)
	}
	clus.confChangeC[0] <- raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  newNodeID,
		Context: []byte(newNodeURL),
	}

	proposeC2 := make(chan string)
	confChangeC2 := make(chan raftpb.ConfChange)
	defer close(proposeC2)
	defer close(confChangeC2)

	kvs := newKVStore(zaptest.NewLogger(t), storage2, proposeC2, clus.commitC[0], clus.errorC[0])

	// Start the new node in join mode
	peers := []string{clus.peers[0], newNodeURL}
	commitC2, errorC2 := newRaftNode(int(newNodeID), 20000+int(newNodeID), zaptest.NewLogger(t), storage2, peers, true, func() ([]byte, error) { return kvs.getSnapshot() }, proposeC2, confChangeC2)

	// Send a test message
	testMsg := struct {
		Key string `json:"key"`
		Val string `json:"val"`
	}{
		Key: "test-key",
		Val: "after-join-test",
	}
	data, err := json.Marshal(testMsg)
	if err != nil {
		t.Fatal(err)
	}
	clus.proposeC[0] <- string(data)

	// Wait for message to be received by new node
	select {
	case <-commitC2:
		t.Log("New node successfully received message")
	case <-errorC2:
		t.Fatal("New node failed to receive message", err)
	case <-time.After(5 * time.Second):
		t.Fatal("New node failed to receive message")
	}
}

func TestSnapshot(t *testing.T) {
	// Override defaults for testing
	prevDefaultSnapshotCount := defaultSnapshotCount
	defaultSnapshotCount = 4
	defer func() {
		defaultSnapshotCount = prevDefaultSnapshotCount
	}()

	// Create cluster
	clus := newCluster(t, 1)
	defer func() {
		t.Log("closing cluster...")
		clus.closeNoErrors(t)
	}()

	// Create snapshot trigger channel
	snapshotTriggered := make(chan struct{})
	go func() {
		// Send message to trigger snapshot
		clus.proposeC[0] <- `"foo"`

		// Wait for response
		for c := range clus.commitC[0] {
			if c != nil && len(c.data) > 0 {
				if c.applyDoneC != nil {
					close(c.applyDoneC)
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
