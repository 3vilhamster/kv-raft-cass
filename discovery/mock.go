package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// MockDiscovery implements a discovery service for testing
type MockDiscovery struct {
	mu        sync.RWMutex
	nodes     map[string]uint64 // map[url]nodeID
	httpPorts map[uint64]int    // map[nodeID]httpPort
	raftPort  int
	logger    *zap.Logger
}

// NewMockDiscovery creates a new mock discovery service
func NewMockDiscovery(logger *zap.Logger, raftPort int) *MockDiscovery {
	return &MockDiscovery{
		nodes:     make(map[string]uint64),
		httpPorts: make(map[uint64]int),
		raftPort:  raftPort,
		logger:    logger,
	}
}

// AddNode adds a node to the mock discovery
func (m *MockDiscovery) AddNode(nodeID uint64, address string, httpPort int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes[address] = nodeID
	m.httpPorts[nodeID] = httpPort
	m.logger.Info("added node to discovery",
		zap.Uint64("node_id", nodeID),
		zap.String("address", address),
		zap.Int("http_port", httpPort))
}

// RemoveNode removes a node from the mock discovery
func (m *MockDiscovery) RemoveNode(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find and remove the node
	for addr, id := range m.nodes {
		if id == nodeID {
			delete(m.nodes, addr)
			delete(m.httpPorts, nodeID)
			m.logger.Info("removed node from discovery", zap.Uint64("node_id", nodeID))
			break
		}
	}
}

// GetClusterNodes returns all nodes in the mock cluster
func (m *MockDiscovery) GetClusterNodes() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]string, 0, len(m.nodes))
	for addr := range m.nodes {
		nodes = append(nodes, fmt.Sprintf("http://%s:%d", addr, m.raftPort))
	}

	return nodes, nil
}

// GetJoinEndpoints returns HTTP endpoints for join requests
func (m *MockDiscovery) GetJoinEndpoints() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	endpoints := make([]string, 0, len(m.nodes))
	for addr, nodeID := range m.nodes {
		httpPort, ok := m.httpPorts[nodeID]
		if !ok {
			httpPort = 9121 // Default HTTP port if not specified
		}

		// Fix: Use proper URL format with host:port
		endpoints = append(endpoints, fmt.Sprintf("http://%s:%d/join", addr, httpPort))
	}

	return endpoints, nil
}

// JoinCluster attempts to join the mock cluster
func (m *MockDiscovery) JoinCluster(nodeID uint64, raftAddress string, backoff time.Duration, maxRetries int) error {
	// Get join endpoints
	endpoints, err := m.GetJoinEndpoints()
	if err != nil {
		return err
	}

	if len(endpoints) == 0 {
		return fmt.Errorf("no endpoints found in mock discovery")
	}

	// Prepare the join request
	myURL := fmt.Sprintf("http://%s", raftAddress)
	joinReq := NodeJoinRequest{
		NodeID:  nodeID,
		NodeURL: myURL,
	}

	jsonData, err := json.Marshal(joinReq)
	if err != nil {
		m.logger.Error("failed to marshal join request", zap.Error(err))
		return err
	}

	// Randomize endpoints to simulate distributed environment
	rand.Shuffle(len(endpoints), func(i, j int) {
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	})

	// Try each endpoint with retries
	for retry := 0; retry < maxRetries; retry++ {
		// Try each endpoint
		for _, endpoint := range endpoints {
			m.logger.Info("attempting to join via endpoint in mock discovery",
				zap.String("endpoint", endpoint),
				zap.Int("retry", retry+1))

			resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				m.logger.Warn("join request failed",
					zap.String("endpoint", endpoint),
					zap.Error(err))
				continue
			}

			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// Success - add ourselves to the discovery
				m.mu.Lock()
				m.nodes[raftAddress] = nodeID
				m.mu.Unlock()

				m.logger.Info("join request accepted and added to mock discovery",
					zap.String("endpoint", endpoint))
				return nil
			}

			m.logger.Warn("join request rejected",
				zap.String("endpoint", endpoint),
				zap.Int("status", resp.StatusCode))
		}

		// All endpoints failed, wait before retrying
		if retry < maxRetries-1 {
			m.logger.Info("all join attempts failed, retrying after backoff",
				zap.Duration("backoff", backoff))
			time.Sleep(backoff)

			// Increase backoff
			backoff *= 2
		}
	}

	return fmt.Errorf("failed to join mock cluster after %d retries", maxRetries)
}
