package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
)

// stoppableListener sets TCP keep-alive timeouts on accepted connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

// newStoppableListener creates a new stoppable TCP listener
func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// Accept accepts a connection with TCP keep-alive enabled
func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)

	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()

	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

// initTransport initializes the Raft transport
func (n *node) initTransport() {
	n.transport = &rafthttp.Transport{
		Logger:      n.logger,
		ID:          types.ID(n.id),
		ClusterID:   0x1000,
		Raft:        n,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(n.logger, strconv.FormatUint(n.id, 10)),
		ErrorC:      make(chan error),
	}

	// Start the transport before adding peers
	err := n.transport.Start()
	if err != nil {
		n.logger.Fatal("rafthttp.Transport.Start", zap.Error(err))
	}

	// Add peers to transport if using discovery
	if n.discovery != nil {
		nodes, err := n.discovery.GetClusterNodes()
		if err == nil {
			for i, url := range nodes {
				peerID := uint64(i + 1)
				if peerID != n.id { // Don't add ourselves
					n.logger.Info("adding peer to transport during init",
						zap.Uint64("peer_id", peerID),
						zap.String("url", url))

					// Register peer in transport
					n.transport.AddPeer(types.ID(peerID), []string{url})
				}
			}
		}
	}
}

// serveRaft starts the HTTP server for Raft communication
func (n *node) serveRaft(w *sync.WaitGroup) {
	// Use "localhost" instead of "0.0.0.0" to ensure consistent behavior in tests
	host := "localhost"
	if strings.Contains(n.address, ":") {
		parts := strings.Split(n.address, ":")
		if len(parts) == 2 && parts[0] != "" {
			host = parts[0]
		}
	}

	listenAddr := fmt.Sprintf("%s:%d", host, n.raftPort)
	url, err := url.Parse("http://" + listenAddr)
	if err != nil {
		n.logger.Sugar().Fatalf("Failed parsing URL (%v)", err)
	}

	n.logger.Info("starting raft HTTP server", zap.String("listen_addr", listenAddr))

	ln, err := newStoppableListener(url.Host, n.httpstopc)
	if err != nil {
		n.logger.Sugar().Fatalf("Failed to listen rafthttp (%v)", err)
	}

	// Signal that this server is now listening
	w.Done()

	err = (&http.Server{Handler: n.transport.Handler()}).Serve(ln)
	select {
	case <-n.httpstopc:
	default:
		n.logger.Sugar().Fatalf("Failed to serve rafthttp (%v)", err)
	}

	close(n.httpdonec)
}

// serveProposals handles proposals from the proposal and confChange channels
func (n *node) serveProposals() {
	confChangeCount := uint64(0)

	for n.proposeC != nil && n.confChangeC != nil {
		select {
		case prop, ok := <-n.proposeC:
			if !ok {
				n.proposeC = nil
			} else {
				n.logger.Debug("Received proposal",
					zap.String("data", prop))

				// blocks until accepted by raft state machine
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := n.raftNode.Propose(ctx, []byte(prop))
				cancel()

				if err != nil {
					n.logger.Error("failed to propose", zap.Error(err))
					continue
				}

				n.logger.Debug("Proposal submitted successfully")
			}

		case cc, ok := <-n.confChangeC:
			if !ok {
				n.confChangeC = nil
			} else {
				confChangeCount++
				cc.ID = confChangeCount

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := n.raftNode.ProposeConfChange(ctx, cc)
				cancel()

				if err != nil {
					n.logger.Error("failed to propose conf change", zap.Error(err))
				}
			}
		}
	}

	// client closed channel; shutdown raft if not already
	close(n.stopc)
}
