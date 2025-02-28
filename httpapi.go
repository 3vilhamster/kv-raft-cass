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
	"strconv"
	"strings"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/discovery"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	raftNode    *raftNode // Added to access raftNode methods
	logger      *zap.Logger
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			h.logger.Error("close body", zap.Error(err))
		}
	}(r.Body)

	// Special handling for join endpoint
	if r.Method == "POST" && strings.HasSuffix(key, "/join") {
		h.handleJoin(w, r)
		return
	}

	switch {
	case r.Method == "PUT":
		v, err := io.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("read on PUT", zap.Error(err))
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		err = h.store.Propose(key[1:], string(v))
		if err != nil {
			h.logger.Error("failed to propose", zap.Error(err))
			http.Error(w, "Failed on PUT", http.StatusInternalServerError)
			return
		}

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, ok := h.store.Lookup(key[1:]); ok {
			_, err := w.Write([]byte(v))
			if err != nil {
				h.logger.Error("write on GET", zap.Error(err))
				return
			}
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		url, err := io.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("read on POST", zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			h.logger.Error("convert ID for conf change", zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			h.logger.Error("convert ID for conf change", zap.Error(err))
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleJoin processes requests to join the Raft cluster
func (h *httpKVAPI) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req discovery.NodeJoinRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error("failed to decode join request", zap.Error(err))
		http.Error(w, "Failed to decode request", http.StatusBadRequest)
		return
	}

	h.logger.Info("received join request",
		zap.Uint64("node_id", req.NodeID),
		zap.String("url", req.NodeURL))

	// Forward to leader if we're not the leader
	if !h.raftNode.isLeader() {
		h.logger.Info("forwarding join request to leader")
		leaderURL, err := h.getLeaderURL()
		if err != nil {
			h.logger.Error("failed to get leader URL", zap.Error(err))
			http.Error(w, "Cannot determine leader", http.StatusServiceUnavailable)
			return
		}

		// Forward the request
		forwardURL := fmt.Sprintf("%s/join", leaderURL)
		jsonData, _ := json.Marshal(req)
		resp, err := http.Post(forwardURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			h.logger.Error("failed to forward join request", zap.Error(err))
			http.Error(w, "Failed to forward request", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Return the leader's response
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// We're the leader, so process the join request
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  req.NodeID,
		Context: []byte(req.NodeURL),
	}

	// Send to confChange channel
	h.confChangeC <- cc

	// Create response with leader information
	response := discovery.NodeJoinResponse{
		Success: true,
		Message: "Join request accepted",
		Leader:  h.raftNode.getLocalURL(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Helper to get leader URL
func (h *httpKVAPI) getLeaderURL() (string, error) {
	return h.raftNode.getLeaderURL()
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(logger *zap.Logger, kv *kvstore, raftNode *raftNode, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
			raftNode:    raftNode, // Pass the raftNode
			logger:      logger,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Fatal("server stopped", zap.Error(err))
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		logger.Fatal("got error from error channel", zap.Error(err))
	}
}
