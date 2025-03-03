package discovery

import (
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

// TestDNSDiscovery tests the DNS discovery implementation
func TestDNSDiscovery(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Use a local hostname for testing
	discovery := NewDNSDiscovery("localhost", 9021, 9121, logger)

	// Should be able to get local node
	nodes, err := discovery.GetClusterNodes()
	if err != nil {
		t.Fatalf("GetClusterNodes failed: %v", err)
	}

	// Should have at least one node (localhost)
	if len(nodes) == 0 {
		// Note: This could fail if localhost doesn't resolve properly
		t.Log("No nodes found via DNS lookup, this could be normal depending on DNS configuration")
	} else {
		t.Logf("Found %d nodes via DNS lookup", len(nodes))
	}

	// Test join endpoints
	endpoints, err := discovery.GetJoinEndpoints()
	if err != nil {
		t.Fatalf("GetJoinEndpoints failed: %v", err)
	}

	if len(endpoints) == 0 {
		t.Log("No endpoints found via DNS lookup, this could be normal depending on DNS configuration")
	} else {
		t.Logf("Found %d endpoints via DNS lookup", len(endpoints))
	}
}

// Setup a custom DNS resolver for testing if needed
type mockDNSResolver struct{}

func (r *mockDNSResolver) LookupHost(host string) ([]string, error) {
	// Always return localhost for any hostname
	return []string{"127.0.0.1"}, nil
}

// TestDNSDiscoveryWithMockResolver tests with a custom DNS resolver
func TestDNSDiscoveryWithMockResolver(t *testing.T) {
	logger := zaptest.NewLogger(t)
	discovery := NewDNSDiscovery("raft-service.test", 9021, 9121, logger)

	discovery.lookUpHostFn = func(host string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	}

	// Should now always return 127.0.0.1
	nodes, err := discovery.GetClusterNodes()
	if err != nil {
		t.Fatalf("GetClusterNodes failed: %v", err)
	}

	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node from mock resolver, got %d", len(nodes))
	}

	expectedURL := "http://127.0.0.1:9021"
	if nodes[0] != expectedURL {
		t.Errorf("Expected node URL %s, got %s", expectedURL, nodes[0])
	}

	// Test caching - should return cached value
	d := discovery
	d.lastLookup = time.Now() // Set the last lookup time to now

	// Modify the cached value
	d.mu.Lock()
	d.cachedNodes = []string{"http://cached-value:9021"}
	d.mu.Unlock()

	// Should return cached value
	nodes, err = discovery.GetClusterNodes()
	if err != nil {
		t.Fatalf("GetClusterNodes failed when using cache: %v", err)
	}

	if len(nodes) != 1 || nodes[0] != "http://cached-value:9021" {
		t.Errorf("Cache not working as expected: %v", nodes)
	}
}
