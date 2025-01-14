package leader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type Process struct {
	logger    *zap.Logger
	isLeader  atomic.Bool
	stopC     chan struct{}
	processWg sync.WaitGroup

	// For custom process management
	startProcess func(ctx context.Context) error
}

// NewLeaderProcess creates a new leader process that starts a process when leadership is acquired and stops it when leadership is lost.
func NewLeaderProcess(logger *zap.Logger, startProcess func(ctx context.Context) error) *Process {
	return &Process{
		logger:       logger,
		stopC:        make(chan struct{}),
		startProcess: startProcess,
	}
}

func (lp *Process) OnLeadershipChanged(isLeader bool) {
	wasLeader := lp.isLeader.Swap(isLeader)

	if isLeader && !wasLeader {
		// Became leader
		lp.logger.Info("gained leadership, starting process")
		lp.processWg.Add(1)
		go lp.runProcess()
	} else if !isLeader && wasLeader {
		// Lost leadership
		lp.logger.Info("lost leadership, stopping process")
		close(lp.stopC)
		lp.processWg.Wait()
		// Create new stop channel for next leadership term
		lp.stopC = make(chan struct{})
	}
}

func (lp *Process) runProcess() {
	defer lp.processWg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor leadership loss
	go func() {
		select {
		case <-lp.stopC:
			cancel()
		case <-ctx.Done():
		}
	}()

	err := lp.startProcess(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		lp.logger.Error("process error", zap.Error(err))
	}
}
