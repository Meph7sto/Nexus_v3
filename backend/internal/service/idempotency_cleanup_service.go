package service

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// IdempotencyCleanupService 定期清理已过期的幂等记录，避免表无限增长。
type IdempotencyCleanupService struct {
	repo     IdempotencyRepository
	interval time.Duration
	worker   sharedTaskRunner
	batch    int
}

func NewIdempotencyCleanupService(repo IdempotencyRepository, cfg *config.Config) *IdempotencyCleanupService {
	interval := 60 * time.Second
	batch := 500
	if cfg != nil {
		if cfg.Idempotency.CleanupIntervalSeconds > 0 {
			interval = time.Duration(cfg.Idempotency.CleanupIntervalSeconds) * time.Second
		}
		if cfg.Idempotency.CleanupBatchSize > 0 {
			batch = cfg.Idempotency.CleanupBatchSize
		}
	}
	return &IdempotencyCleanupService{
		repo:     repo,
		interval: interval,
		batch:    batch,
	}
}

func (s *IdempotencyCleanupService) Start() {
	if s != nil {
		s.worker.start(context.Background(), s.runShared)
	}
}

func (s *IdempotencyCleanupService) Stop() {
	if s != nil {
		s.worker.stop()
	}
}

func (s *IdempotencyCleanupService) runShared(ctx context.Context) {
	if s == nil || s.repo == nil {
		return
	}
	runSharedPeriodic(ctx, s.interval, true, s.cleanupOnceContext)
}

func (s *IdempotencyCleanupService) cleanupOnce() {
	s.cleanupOnceContext(context.Background())
}

func (s *IdempotencyCleanupService) cleanupOnceContext(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	deleted, err := s.repo.DeleteExpired(ctx, time.Now(), s.batch)
	if err != nil {
		logger.LegacyPrintf("service.idempotency_cleanup", "[IdempotencyCleanup] cleanup failed err=%v", err)
		return
	}
	if deleted > 0 {
		logger.LegacyPrintf("service.idempotency_cleanup", "[IdempotencyCleanup] cleaned expired records count=%d", deleted)
	}
}
