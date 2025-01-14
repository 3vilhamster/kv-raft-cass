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
	"flag"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/leader"
	raftstorage "github.com/3vilhamster/kv-raft-cass/storage/raft"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	raftPort := flag.Int("raftport", 9021, "raft server port")
	join := flag.Bool("join", false, "join an existing cluster")
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
	storage, err := raftstorage.New(sess, namespaceID, uint64(*id))
	if err != nil {
		panic(err)
	}

	leaderProcess := leader.NewLeaderProcess(
		logger,
		func(ctx context.Context) error {
			return nil
		})

	_, commitC, errorC := newRaftNode(*id, *raftPort, logger, leaderProcess, storage, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(logger, storage, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(logger, kvs, *kvport, confChangeC, errorC)
}
