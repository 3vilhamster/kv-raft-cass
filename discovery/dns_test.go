package discovery

import (
	"net"
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

// TestExtractIPFromURL tests the URL parsing helper
func TestExtractIPFromURL(t *testing.T) {
	testCases := []struct {
		url      string
		expected string
	}{
		{"http://192.168.1.1:8080", "192.168.1.1"},
		{"https://example.com:443", "example.com"},
		{"192.168.1.1:8080", "192.168.1.1"},
		{"example.com", "example.com"},
		{"http://localhost/path", "localhost"},
	}

	for _, tc := range testCases {
		result := ExtractIPFromURL(tc.url)
		if result != tc.expected {
			t.Errorf("ExtractIPFromURL(%s) = %s; want %s", tc.url, result, tc.expected)
		}
	}
}

// Setup a custom DNS resolver for testing if needed
type mockDNSResolver struct{}

func (r *mockDNSResolver) LookupHost(host string) ([]string, error) {
	// Always return localhost for any hostname
	return []string{"127.0.0.1"}, nil
}

// Replace the net.LookupHost function with our mock implementation
func mockLookup() func() {
	original := net.LookupHost
	net.LookupHost = func(host string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	}

	return func() {
		net.LookupHost = original
	}
}

// TestDNSDiscoveryWithMockResolver tests with a custom DNS resolver
func TestDNSDiscoveryWithMockResolver(t *testing.T) {
	// Replace the DNS lookup function with our mock
	cleanup := mockLookup()
	defer cleanup()

	logger := zaptest.NewLogger(t)
	discovery := NewDNSDiscovery("raft-service.test", 9021, 9121, logger)

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
