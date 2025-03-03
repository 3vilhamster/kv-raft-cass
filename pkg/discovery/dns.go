package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// DNSDiscovery implements a DNS-based Raft node discovery service
type DNSDiscovery struct {
	mu          sync.RWMutex
	dnsName     string // e.g., "my-raft-service.domain"
	raftPort    int    // The port your Raft nodes communicate on
	httpPort    int    // For API requests like join
	logger      *zap.Logger
	cacheExpiry time.Duration
	lastLookup  time.Time
	cachedNodes []string

	// Add a map to store node ID to URL mappings
	nodeIDToURL map[uint64]string

	lookUpHostFn func(host string) (addrs []string, err error)
}

// NodeJoinRequest represents a request to join the Raft cluster
type NodeJoinRequest struct {
	NodeID  uint64 `json:"node_id"`
	NodeURL string `json:"node_url"`
}

// NodeJoinResponse represents a response to a join request
type NodeJoinResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Leader  string `json:"leader,omitempty"` // Leader URL, if known
}

// NewDNSDiscovery creates a new DNS-based discovery service
func NewDNSDiscovery(dnsName string, raftPort, httpPort int, logger *zap.Logger) *DNSDiscovery {
	return &DNSDiscovery{
		dnsName:      dnsName,
		raftPort:     raftPort,
		httpPort:     httpPort,
		logger:       logger,
		cacheExpiry:  30 * time.Second,
		lookUpHostFn: net.LookupHost,
		nodeIDToURL:  make(map[uint64]string),
	}
}

// GetClusterNodes returns all nodes registered in DNS
func (d *DNSDiscovery) GetClusterNodes() ([]string, error) {
	d.mu.RLock()
	if time.Since(d.lastLookup) < d.cacheExpiry && len(d.cachedNodes) > 0 {
		nodes := make([]string, len(d.cachedNodes))
		copy(nodes, d.cachedNodes)
		d.mu.RUnlock()
		return nodes, nil
	}
	d.mu.RUnlock()

	// Perform DNS lookup to get all IPs
	ips, err := d.lookUpHostFn(d.dnsName)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup failed: %w", err)
	}

	d.logger.Debug("DNS lookup succeeded",
		zap.String("dns_name", d.dnsName),
		zap.Int("ip_count", len(ips)))

	// Convert IPs to node URLs
	nodes := make([]string, len(ips))
	for i, ip := range ips {
		nodes[i] = fmt.Sprintf("http://%s:%d", ip, d.raftPort)
	}

	// Update cache
	d.mu.Lock()
	d.cachedNodes = nodes
	d.lastLookup = time.Now()
	d.mu.Unlock()

	return nodes, nil
}

// GetJoinEndpoints returns HTTP endpoints for join requests
func (d *DNSDiscovery) GetJoinEndpoints() ([]string, error) {
	// Get IPs same as above
	ips, err := d.lookUpHostFn(d.dnsName)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup failed: %w", err)
	}

	// Convert IPs to HTTP API endpoints
	endpoints := make([]string, len(ips))
	for i, ip := range ips {
		endpoints[i] = fmt.Sprintf("http://%s:%d/join", ip, d.httpPort)
	}

	return endpoints, nil
}

// JoinCluster attempts to join an existing cluster by contacting any node
func (d *DNSDiscovery) JoinCluster(nodeID uint64, raftAddress string, backoff time.Duration, maxRetries int) error {
	// Get join endpoints from DNS
	endpoints, err := d.GetJoinEndpoints()
	if err != nil {
		d.logger.Error("failed to get join endpoints", zap.Error(err))
		return err
	}

	if len(endpoints) == 0 {
		return fmt.Errorf("no endpoints found in DNS - nothing to join")
	}

	// Prepare the join request
	myURL := fmt.Sprintf("http://%s", raftAddress)
	joinReq := NodeJoinRequest{
		NodeID:  nodeID,
		NodeURL: myURL,
	}

	jsonData, err := json.Marshal(joinReq)
	if err != nil {
		d.logger.Error("failed to marshal join request", zap.Error(err))
		return err
	}

	// Try each endpoint with retries
	for retry := 0; retry < maxRetries; retry++ {
		// Try each endpoint
		for _, endpoint := range endpoints {
			d.logger.Info("attempting to join via endpoint",
				zap.String("endpoint", endpoint),
				zap.Int("retry", retry+1),
				zap.Int("max_retries", maxRetries))

			resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				d.logger.Warn("join request failed",
					zap.String("endpoint", endpoint),
					zap.Error(err))
				continue
			}

			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// Try to parse the response for leader information
				body, err := io.ReadAll(resp.Body)
				if err == nil {
					var joinResp NodeJoinResponse
					if err := json.Unmarshal(body, &joinResp); err == nil {
						if joinResp.Leader != "" {
							d.logger.Info("join request accepted, leader info received",
								zap.String("leader", joinResp.Leader))
						} else {
							d.logger.Info("join request accepted")
						}
					}
				}

				// After a successful join, update the nodeID to URL mapping
				d.mu.Lock()
				d.nodeIDToURL[nodeID] = fmt.Sprintf("http://%s", raftAddress)
				d.mu.Unlock()

				return nil
			}

			d.logger.Warn("join request rejected",
				zap.String("endpoint", endpoint),
				zap.Int("status", resp.StatusCode))
		}

		// All endpoints failed, wait before retrying
		if retry < maxRetries-1 {
			d.logger.Info("all join attempts failed, retrying after backoff",
				zap.Duration("backoff", backoff),
				zap.Int("retry", retry+1),
				zap.Int("max_retries", maxRetries))
			time.Sleep(backoff)

			// Increase backoff duration exponentially
			backoff *= 2

			// Refresh endpoints from DNS in case nodes have changed
			newEndpoints, err := d.GetJoinEndpoints()
			if err == nil && len(newEndpoints) > 0 {
				endpoints = newEndpoints
			}
		}
	}

	return fmt.Errorf("failed to join cluster after %d retries", maxRetries)
}

// GetNodeURL returns the URL for a specific node ID
func (d *DNSDiscovery) GetNodeURL(nodeID uint64) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Check if we have a cached mapping of node IDs to URLs
	if d.nodeIDToURL != nil {
		if url, ok := d.nodeIDToURL[nodeID]; ok {
			return url, nil
		}
	}

	// If we don't have a mapping, we can't determine the URL
	// This is a limitation that would need to be addressed by maintaining
	// a mapping of node IDs to URLs as nodes join and leave the cluster
	d.logger.Debug("no mapping found for node ID",
		zap.Uint64("node_id", nodeID))

	return "", fmt.Errorf("no URL mapping found for node ID %d", nodeID)
}
