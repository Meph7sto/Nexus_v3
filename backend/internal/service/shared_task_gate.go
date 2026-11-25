package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	sharedTaskLeaseTTL      = 30 * time.Second
	sharedTaskLeaseInterval = 10 * time.Second
	sharedTaskQueryTimeout  = 5 * time.Second
	sharedTaskStandbyDelay  = 2 * time.Second
)

type sharedTaskLeaseStore interface {
	claim(ctx context.Context, id, token string, ttl time.Duration) (bool, error)
	held(ctx context.Context) (bool, error)
	release(ctx context.Context, token string) error
}

type sharedTaskSQLLease struct{ db *sql.DB }

func (s sharedTaskSQLLease) claim(ctx context.Context, id, token string, ttl time.Duration) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO entry_shared_task_claim (entry_id, process_token, lease_expires_at, updated_at)
		VALUES ($1, $2, NOW() + $3 * INTERVAL '1 microsecond', NOW())
		ON CONFLICT (singleton) DO UPDATE SET
			entry_id = EXCLUDED.entry_id,
			process_token = EXCLUDED.process_token,
			claimed_at = CASE WHEN entry_shared_task_claim.process_token = EXCLUDED.process_token
				THEN entry_shared_task_claim.claimed_at ELSE NOW() END,
			lease_expires_at = EXCLUDED.lease_expires_at,
			updated_at = NOW()
		WHERE entry_shared_task_claim.lease_expires_at <= NOW()
			OR entry_shared_task_claim.process_token = EXCLUDED.process_token
	`, id, token, ttl.Microseconds())
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s sharedTaskSQLLease) held(ctx context.Context) (bool, error) {
	var held bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM entry_shared_task_claim WHERE lease_expires_at > NOW()
	)`).Scan(&held)
	return held, err
}

func (s sharedTaskSQLLease) release(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM entry_shared_task_claim WHERE process_token = $1`, token)
	return err
}

type sharedTaskLeaseTiming struct {
	ttl, interval, queryTimeout, standbyDelay time.Duration
}

// SharedTaskGate runs one cancellable group of jobs per confirmed lease.
// Registration is complete before Begin, so a partially wired app never owns a lease.
type SharedTaskGate struct {
	node  config.EntryNodeConfig
	store sharedTaskLeaseStore
	time  sharedTaskLeaseTiming
	token string

	mu     sync.Mutex
	jobs   []func(context.Context)
	cancel context.CancelFunc
	done   chan struct{}
	active *sharedTaskEpoch
}

type sharedTaskEpoch struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	deadline time.Time
}

func newSharedTaskGate(node config.EntryNodeConfig, store sharedTaskLeaseStore, timing sharedTaskLeaseTiming) (*SharedTaskGate, error) {
	g := &SharedTaskGate{node: node, store: store, time: timing, token: rand.Text()}
	if !node.ClusterEnabled {
		ctx, cancel := context.WithTimeout(context.Background(), timing.queryTimeout)
		defer cancel()
		held, err := store.held(ctx)
		if err != nil {
			return nil, fmt.Errorf("check prior Entry task lease: %w", err)
		}
		if held {
			return nil, fmt.Errorf("an unexpired prior clustered Entry task lease prevents standalone startup")
		}
	}
	return g, nil
}

func ProvideSharedTaskGate(cfg *config.Config, db *sql.DB) (*SharedTaskGate, error) {
	return newSharedTaskGate(cfg.EntryNode, sharedTaskSQLLease{db}, sharedTaskLeaseTiming{
		ttl: sharedTaskLeaseTTL, interval: sharedTaskLeaseInterval, queryTimeout: sharedTaskQueryTimeout,
		standbyDelay: sharedTaskStandbyDelay,
	})
}

func (g *SharedTaskGate) Running() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active != nil && g.active.ctx.Err() == nil &&
		(g.active.deadline.IsZero() || time.Now().Before(g.active.deadline))
}

// Register takes a job that exits only after its lease-scoped work has stopped.
// Nil gates are used in direct provider calls, outside the Entry lifecycle.
func (g *SharedTaskGate) Register(job func(context.Context)) {
	if g == nil {
		go job(context.Background())
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done != nil {
		panic("register shared Entry task after Begin")
	}
	g.jobs = append(g.jobs, job)
}

func (g *SharedTaskGate) RegisterLifecycle(start func(context.Context), stop func()) {
	if g == nil {
		start(context.Background())
		return
	}
	g.Register(func(ctx context.Context) {
		if ctx.Err() == nil {
			start(ctx)
			<-ctx.Done()
			stop()
		}
	})
}

func (g *SharedTaskGate) registerWorker(worker *sharedTaskRunner, job func(context.Context)) {
	if g == nil {
		worker.start(context.Background(), job)
		return
	}
	g.Register(job)
}

// Begin starts lease maintenance after the application has registered its jobs.
func (g *SharedTaskGate) Begin() {
	if g == nil || g.store == nil || (g.node.ClusterEnabled && !g.node.AllowsControl()) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel, g.done = cancel, make(chan struct{})
	if !g.node.ClusterEnabled {
		g.active = g.startEpoch(ctx, time.Time{})
	}
	go g.maintain(ctx)
}

func (g *SharedTaskGate) startEpoch(parent context.Context, deadline time.Time) *sharedTaskEpoch {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, job := range g.jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Err() == nil {
				job(ctx)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return &sharedTaskEpoch{ctx: ctx, cancel: cancel, done: done, deadline: deadline}
}

func (g *SharedTaskGate) maintain(ctx context.Context) {
	defer close(g.done)
	if !g.node.ClusterEnabled {
		<-ctx.Done()
		<-g.active.done
		return
	}

	var epoch *sharedTaskEpoch
	var deadlineTimer *time.Timer
	defer func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
		if epoch != nil {
			epoch.cancel()
			<-epoch.done
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), g.time.queryTimeout)
		defer cancel()
		if err := g.store.release(releaseCtx, g.token); err != nil {
			logger.L().Warn("entry.shared_task_lease_release_failed", zap.String("entry_id", g.node.ID), zap.Error(err))
		}
	}()

	initialDelay := time.Duration(0)
	if g.node.SharedTaskOwnerID != "" && !g.node.DesignatedTaskOwner() {
		// The preferred owner attempts immediately. The lease itself is
		// never preempted, regardless of an entry's configured priority.
		initialDelay = g.time.standbyDelay
	}
	next := time.NewTimer(initialDelay)
	defer next.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-next.C:
		}
		if epoch != nil && !time.Now().Before(epoch.deadline) {
			epoch.cancel()
		}
		if epoch != nil && epoch.ctx.Err() != nil {
			select {
			case <-epoch.done:
				epoch = nil
			default:
				next.Reset(g.time.interval)
				continue
			}
		}

		// An independent timer cancels jobs at the last confirmed deadline,
		// even if PostgreSQL is slow or workers are still starting/stopping.
		attempted := time.Now()
		queryDeadline := attempted.Add(g.time.queryTimeout)
		if epoch != nil && epoch.deadline.Before(queryDeadline) {
			queryDeadline = epoch.deadline
		}
		queryCtx, cancel := context.WithDeadline(ctx, queryDeadline)
		owned, err := g.store.claim(queryCtx, g.node.ID, g.token, g.time.ttl)
		cancel()
		if err != nil || !owned {
			if err == nil && epoch != nil {
				epoch.cancel()
			}
			if err != nil && ctx.Err() == nil {
				logger.L().Warn("entry.shared_task_lease_check_failed", zap.String("entry_id", g.node.ID), zap.Error(err))
			}
		} else if ctx.Err() == nil {
			deadline := attempted.Add(g.time.ttl)
			g.mu.Lock()
			if !time.Now().Before(deadline) || (epoch != nil &&
				(epoch.ctx.Err() != nil || !time.Now().Before(epoch.deadline))) {
				if epoch != nil {
					epoch.cancel()
				}
				g.mu.Unlock()
				next.Reset(g.time.interval)
				continue
			}
			if epoch == nil {
				epoch = g.startEpoch(ctx, deadline)
				g.active = epoch
			}
			epoch.deadline = deadline
			g.mu.Unlock()
			if deadlineTimer != nil {
				deadlineTimer.Stop()
			}
			deadlineTimer = time.AfterFunc(time.Until(deadline), func() {
				g.mu.Lock()
				defer g.mu.Unlock()
				if g.active != nil && g.active.deadline == deadline {
					g.active.cancel()
				}
			})
		}
		interval := g.time.interval
		if (epoch == nil || epoch.ctx.Err() != nil) && g.time.standbyDelay > 0 {
			interval = g.time.standbyDelay
		}
		next.Reset(time.Until(attempted.Add(interval)))
	}
}

// Stop cancels jobs, waits for in-flight work to unwind, and conditionally
// releases only this process's lease while the database is still open.
func (g *SharedTaskGate) Stop() {
	if g == nil {
		return
	}
	g.mu.Lock()
	cancel, done := g.cancel, g.done
	g.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}
