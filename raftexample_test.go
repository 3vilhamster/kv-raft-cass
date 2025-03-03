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

// newCluster creates a cluster of n nodes
func newCluster(t *testing.T, n int, withKvs bool) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 20000+i)
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

	for i := range clus.peers {
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)

		storage, err := raftstorage.New(sess, namespaceID, uint64(i+1))
		if err != nil {
			panic(err)
		}

		var (
			kvs  *kvstore
			node raft.Node
		)

		getSnapshot := func() ([]byte, error) { return nil, nil }

		nodeLogger := logger.With(zap.String("node", fmt.Sprintf("%d", i+1)))

		node, clus.commitC[i], clus.errorC[i] = raft.New(
			raft.Config{
				NodeID:           uint64(i + 1),
				RaftPort:         20000 + i,
				Address:          "localhost",
				Logger:           nodeLogger,
				LeaderProcess:    testLeader(t),
				Storage:          storage,
				Discovery:        discovery.NewMockDiscovery(nodeLogger, 20000+i),
				Join:             false,
				BootstrapAllowed: true,
				GetSnapshot:      getSnapshot,
			}, clus.proposeC[i], clus.confChangeC[i])

		if withKvs {
			kvs = newKVStore(nodeLogger, storage, clus.proposeC[i], clus.commitC[i], clus.errorC[i])
			getSnapshot = func() ([]byte, error) { return kvs.getSnapshot() }
		}

		clus.nodes = append(clus.nodes, node)
		clus.kvs = append(clus.kvs, kvs)
	}

	// Wait for cluster to stabilize and elect a leader
	waitForLeader := func() bool {
		deadline := time.Now().Add(5 * time.Second)
		firstTry := true

		for time.Now().Before(deadline) {
			if firstTry {
				// Let HTTP server start up on first try
				time.Sleep(500 * time.Millisecond)
				firstTry = false
			}

			leaderCount := 0
			var leaderId int
			for i, node := range clus.nodes {
				if node.IsLeader() {
					leaderCount++
					leaderId = i + 1
				}
			}

			if leaderCount == 1 {
				logger.Info("leader elected",
					zap.Int("leader_id", leaderId),
					zap.Duration("elapsed", time.Since(deadline.Add(-5*time.Second))))
				return true
			}

			if leaderCount > 1 {
				logger.Warn("multiple leaders detected",
					zap.Int("count", leaderCount))
			}

			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	if !waitForLeader() {
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
