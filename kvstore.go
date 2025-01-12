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
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/3vilhamster/kv-raft-cass/storage"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	storage     storage.Raft
	logger      *zap.Logger
	lastApplied uint64

	// Add proposal tracking
	proposals sync.Map // map[string]chan error
}

func newKVStore(logger *zap.Logger, snapshotter storage.Raft, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC: proposeC,
		kvStore:  make(map[string]string),
		storage:  snapshotter,
		logger:   logger,
	}

	// Load snapshot if available
	snapshot, err := s.loadSnapshot()
	if err != nil {
		logger.Info("loading snapshot", zap.Error(err))
		return nil
	}

	if snapshot.Data != nil {
		logger.Info("Loading snapshot", zap.Uint64("term", snapshot.Metadata.Term), zap.Uint64("index", snapshot.Metadata.Index))
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			logger.Error("recovering from snapshot", zap.Error(err))
			return nil
		}
		s.lastApplied = snapshot.Metadata.Index
	}

	// Start processing commits
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Strip leading slash
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	s.logger.Debug("Looking up key", zap.String("key", key))
	v, ok := s.kvStore[key]
	s.logger.Debug("Lookup result", zap.String("key", key), zap.Bool("found", ok), zap.String("value", v))
	return v, ok
}

func (s *kvstore) Propose(k string, v string) error {
	if len(k) > 0 && k[0] == '/' {
		k = k[1:]
	}

	s.logger.Debug("Proposing",
		zap.String("key", k),
		zap.String("value", v))

	proposalID := s.nextProposalID()
	waitC := make(chan error, 1) // Buffered channel to prevent blocking
	s.proposals.Store(proposalID, waitC)

	// Ensure cleanup of proposal on exit
	defer func() {
		if _, ok := s.proposals.Load(proposalID); ok {
			s.proposals.Delete(proposalID)
			s.logger.Debug("cleaned up proposal",
				zap.String("id", proposalID))
		}
	}()

	proposal := struct {
		ID  string `json:"id"`
		Key string `json:"key"`
		Val string `json:"val"`
	}{
		ID:  proposalID,
		Key: k,
		Val: v,
	}

	data, err := json.Marshal(proposal)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	// Submit proposal with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case s.proposeC <- string(data):
		s.logger.Debug("Proposal sent",
			zap.String("key", k),
			zap.String("id", proposalID))
	case <-ctx.Done():
		return fmt.Errorf("proposal send timed out")
	}

	// Wait for commit with context timeout
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case err := <-waitC:
		if err != nil {
			return fmt.Errorf("commit failed: %w", err)
		}
		s.logger.Debug("Proposal committed successfully",
			zap.String("key", k),
			zap.String("id", proposalID))
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for proposal commit timed out")
	}
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		s.logger.Debug("commit received")

		if commit == nil {
			s.logger.Debug("empty commit")
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				s.logger.Panic(err.Error())
			}
			if snapshot.Data != nil {
				s.logger.Debug("loading snapshot",
					zap.Uint64("term", snapshot.Metadata.Term),
					zap.Uint64("index", snapshot.Metadata.Index))
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		s.logger.Debug("commit with data",
			zap.Int("data_count", len(commit.data)))

		// Process all data in the commit before signaling completion
		processed := make([]string, 0, len(commit.data))
		for _, data := range commit.data {
			var proposal struct {
				ID  string `json:"id"`
				Key string `json:"key"`
				Val string `json:"val"`
			}

			if err := json.Unmarshal([]byte(data), &proposal); err != nil {
				s.logger.Error("failed to decode message",
					zap.Error(err),
					zap.String("data", data))
				continue
			}

			// Apply changes to store
			s.mu.Lock()
			s.kvStore[proposal.Key] = proposal.Val
			s.mu.Unlock()

			if proposal.ID != "" {
				processed = append(processed, proposal.ID)
			}
		}

		// Signal completions after processing all entries
		for _, proposalID := range processed {
			if waitC, ok := s.proposals.Load(proposalID); ok {
				s.logger.Debug("signaling proposal completion",
					zap.String("id", proposalID))

				if c, ok := waitC.(chan error); ok {
					// Ensure channel send doesn't block
					select {
					case c <- nil:
						s.logger.Debug("proposal completion signaled",
							zap.String("id", proposalID))
					default:
						s.logger.Warn("proposal completion channel blocked",
							zap.String("id", proposalID))
					}
				}
				s.proposals.Delete(proposalID)
			}
		}

		// Signal apply completion after all processing is done
		if commit.applyDoneC != nil {
			close(commit.applyDoneC)
		}
	}

	// Handle errors from raft
	if err, ok := <-errorC; ok {
		s.logger.Error("error from raft", zap.Error(err))
		s.proposals.Range(func(key, value interface{}) bool {
			if c, ok := value.(chan error); ok {
				select {
				case c <- err:
				default:
					s.logger.Warn("failed to signal error to proposal",
						zap.String("id", key.(string)))
				}
			}
			s.proposals.Delete(key)
			return true
		})
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

// Add a helper to generate unique proposal IDs
func (s *kvstore) nextProposalID() string {
	return fmt.Sprintf("proposal-%d", time.Now().UnixNano())
}
