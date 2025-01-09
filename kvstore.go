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
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/3vilhamster/kv-raft-cass/storage"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC chan<- string // channel for proposing updates
	mu       sync.RWMutex
	kvStore  map[string]string // current committed key-value pairs
	storage  storage.Raft
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter storage.Raft, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC: proposeC,
		kvStore:  make(map[string]string),
		storage:  snapshotter,
	}

	// Load snapshot if available
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Printf("Error loading snapshot: %v", err)
		return nil
	}

	if snapshot.Data != nil {
		log.Printf("Loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Printf("Error recovering from snapshot: %v", err)
			return nil
		}
	}

	// Start processing commits
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log.Printf("Looking up key: %s", key)
	v, ok := s.kvStore[key]
	log.Printf("Lookup result: key=%s found=%v value=%s", key, ok, v)
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	log.Printf("Proposing key=%s value=%s", k, v)
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot.Data != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (raftpb.Snapshot, error) {
	snapshot, err := s.storage.Snapshot()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
