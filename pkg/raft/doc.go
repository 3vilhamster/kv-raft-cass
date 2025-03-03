/*
Package raft provides a high-level implementation of the Raft consensus algorithm.

This package is organized into several components:

1. Interface (interface.go) - Defines the public interfaces and types for the package.
2. Node (node.go) - Implements the core node functionality.
3. Integration (integration.go) - Handles Raft integration with etcd/raft and network transport.
4. Entries (entries.go) - Implements entry processing and log management.

To use this package, create a new Raft node with the New function, providing a Config
struct with all necessary configuration parameters. The function returns a Node interface,
a channel for receiving committed entries, and a channel for errors.

Example:

	// Create channels for Raft communication
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	// Configure the Raft node
	config := raft.Config{
	    NodeID:           1,
	    RaftPort:         9021,
	    Address:          "127.0.0.1:9021",
	    Storage:          myStorage,
	    Discovery:        myDiscovery,
	    Join:             false,
	    BootstrapAllowed: true,
	    LeaderProcess:    myLeaderProcess,
	    Logger:           myLogger,
	    GetSnapshot:      getSnapshotFunc,
	}

	// Create a new Raft node
	node, commitC, errorC := raft.New(config, proposeC, confChangeC)

	// Use the node...

Proposal and commit channels are used to propose new commands and receive committed entries.
The error channel should be monitored for any Raft errors.
*/
package raft
