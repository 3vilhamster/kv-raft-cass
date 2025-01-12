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
	"io"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
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

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(logger *zap.Logger, kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
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
