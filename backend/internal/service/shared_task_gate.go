package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// SharedTaskGate owns the process-wide decision about shared periodic jobs.
// A clustered claim never expires: an operator transfers it after the old
// process is stopped and unfinished jobs have been checked.
type SharedTaskGate struct {
	running atomic.Bool
}

func newSharedTaskGate(node config.EntryNodeConfig, claim func(string) (bool, error)) (*SharedTaskGate, error) {
	gate := &SharedTaskGate{}
	if !node.DesignatedTaskOwner() {
		return gate, nil
	}
	owned := true
	if node.ClusterEnabled {
		var err error
		owned, err = claim(node.ID)
		if err != nil {
			return nil, fmt.Errorf("claim shared Entry tasks: %w", err)
		}
		if !owned {
			logger.L().Warn("entry.shared_task_claim_already_held", zap.String("entry_id", node.ID))
		}
	}
	gate.running.Store(owned)
	return gate, nil
}

func ProvideSharedTaskGate(cfg *config.Config, db *sql.DB) (*SharedTaskGate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !cfg.EntryNode.ClusterEnabled {
		// Do not let a standalone rollback start jobs while a clustered
		// process still owns them on the same PostgreSQL instance.
		var held bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM entry_shared_task_claim)").Scan(&held); err != nil {
			return nil, fmt.Errorf("check prior Entry task claim: %w", err)
		}
		if held {
			return nil, fmt.Errorf("release the prior clustered Entry task claim before starting standalone")
		}
	}
	return newSharedTaskGate(cfg.EntryNode, func(id string) (bool, error) {
		result, err := db.ExecContext(ctx, `
			INSERT INTO entry_shared_task_claim (entry_id, process_token)
			VALUES ($1, $2) ON CONFLICT (singleton) DO NOTHING
		`, id, rand.Text())
		if err != nil {
			return false, err
		}
		count, err := result.RowsAffected()
		return count == 1, err
	})
}

func (g *SharedTaskGate) Running() bool {
	return g == nil || g.running.Load()
}

func (g *SharedTaskGate) Start(start func()) {
	if g.Running() {
		start()
	}
}

func (g *SharedTaskGate) Stop() {
	if g != nil {
		g.running.Store(false)
	}
}
