// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/pkg/discovery"
	"github.com/3vilhamster/kv-raft-cass/pkg/raft"
	raftstorage "github.com/3vilhamster/kv-raft-cass/pkg/storage/raft"
)

const namespaceID = "test"

func main() {
	// Original parameters
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	raftPort := flag.Int("raftport", 9021, "raft server port")
	join := flag.Bool("join", false, "join an existing cluster")

	// New parameters for DNS discovery
	dnsName := flag.String("dns", "", "DNS name for service discovery")
	localAddress := flag.String("address", "127.0.0.1", "Local address")
	bootstrapAllowed := flag.Bool("bootstrap", false, "Allow bootstrapping a new cluster if no nodes found")

	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }

	clusterConfig := gocql.NewCluster("127.0.0.1")
	clusterConfig.Keyspace = "raft_storage"
	// Optimize consistency levels for local datacenter
	clusterConfig.Consistency = gocql.One
	// Optimize connection settings for write-heavy workload
	clusterConfig.NumConns = 1
	clusterConfig.Timeout = 2 * time.Second
	clusterConfig.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 2}
	clusterConfig.PoolConfig.HostSelectionPolicy =
		gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	sess, err := clusterConfig.CreateSession()
	if err != nil {
		panic(err)
	}

	// Define node address
	nodeAddress := fmt.Sprintf("%s:%d", *localAddress, *raftPort)

	// Setup discovery service
	var discoveryService discovery.Discovery
	if *dnsName != "" {
		// Use DNS discovery
		discoveryService = discovery.NewDNSDiscovery(*dnsName, *raftPort, *kvport, logger)
		logger.Info("using DNS discovery",
			zap.String("dns_name", *dnsName),
			zap.Int("raft_port", *raftPort),
			zap.Int("kv_port", *kvport))
	} else {
		// Fallback to basic static peers if no DNS discovery
		logger.Warn("no DNS discovery configured, falling back to static peers")
	}

	// Create storage
	storage, err := raftstorage.New(sess, namespaceID, uint64(*id))
	if err != nil {
		panic(err)
	}

	// Setup leader process
	leaderProcess := raft.NewLeaderProcess(
		logger,
		func(ctx context.Context) error {
			return nil
		})

	// Initialize raft node with discovery
	node, commitC, errorC := raft.New(
		raft.Config{
			NodeID:           uint64(*id),
			RaftPort:         *raftPort,
			Address:          nodeAddress,
			Logger:           logger,
			LeaderProcess:    leaderProcess,
			Storage:          storage,
			Discovery:        discoveryService,
			Join:             *join,
			BootstrapAllowed: *bootstrapAllowed,
			GetSnapshot:      getSnapshot,
		}, proposeC, confChangeC)

	// Initialize key-value store
	kvs = newKVStore(logger, storage, proposeC, commitC, errorC)

	// Start HTTP server
	serveHttpKVAPI(logger, kvs, node, *kvport, confChangeC, errorC)
}
