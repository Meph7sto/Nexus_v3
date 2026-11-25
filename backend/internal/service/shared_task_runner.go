package service

import (
	"context"
	"sync"
	"time"
)

// sharedTaskRunner supports direct Start/Stop calls and waits for the old
// invocation to exit before allowing another one to start.
type sharedTaskRunner struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *sharedTaskRunner) start(parent context.Context, run func(context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil || parent.Err() != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	r.cancel, r.done = cancel, done
	go func() {
		defer close(done)
		run(ctx)
	}()
}

func (r *sharedTaskRunner) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
		<-r.done
		r.cancel, r.done = nil, nil
	}
}

func runSharedPeriodic(ctx context.Context, interval time.Duration, immediate bool, run func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if immediate && ctx.Err() == nil {
		run(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			run(ctx)
		}
	}
}
