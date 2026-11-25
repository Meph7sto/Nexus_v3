package service

import (
	"context"
	"log"
	"time"
)

// AccountExpiryService periodically pauses expired accounts when auto-pause is enabled.
type AccountExpiryService struct {
	accountRepo AccountRepository
	interval    time.Duration
	worker      sharedTaskRunner
}

func NewAccountExpiryService(accountRepo AccountRepository, interval time.Duration) *AccountExpiryService {
	return &AccountExpiryService{
		accountRepo: accountRepo,
		interval:    interval,
	}
}

func (s *AccountExpiryService) Start() {
	if s != nil {
		s.worker.start(context.Background(), s.runShared)
	}
}

func (s *AccountExpiryService) Stop() {
	if s != nil {
		s.worker.stop()
	}
}

func (s *AccountExpiryService) runShared(ctx context.Context) {
	if s == nil || s.accountRepo == nil || s.interval <= 0 {
		return
	}
	runSharedPeriodic(ctx, s.interval, true, s.runOnceContext)
}

func (s *AccountExpiryService) runOnceContext(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	updated, err := s.accountRepo.AutoPauseExpiredAccounts(ctx, time.Now())
	if err != nil {
		log.Printf("[AccountExpiry] Auto pause expired accounts failed: %v", err)
		return
	}
	if updated > 0 {
		log.Printf("[AccountExpiry] Auto paused %d expired accounts", updated)
	}
}
