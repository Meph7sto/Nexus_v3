package service

import (
	"context"
	"log"
	"time"
)

// ProxyExpiryService 周期扫描到期代理并把绑定账号改投备用/直连。
type ProxyExpiryService struct {
	proxyRepo ProxyRepository
	interval  time.Duration
	worker    sharedTaskRunner
}

func NewProxyExpiryService(proxyRepo ProxyRepository, interval time.Duration) *ProxyExpiryService {
	return &ProxyExpiryService{proxyRepo: proxyRepo, interval: interval}
}

func (s *ProxyExpiryService) Start() {
	if s != nil {
		s.worker.start(context.Background(), s.runShared)
	}
}

func (s *ProxyExpiryService) Stop() {
	if s != nil {
		s.worker.stop()
	}
}

func (s *ProxyExpiryService) runShared(ctx context.Context) {
	if s == nil || s.proxyRepo == nil || s.interval <= 0 {
		return
	}
	runSharedPeriodic(ctx, s.interval, true, s.runOnceContext)
}

func (s *ProxyExpiryService) runOnceContext(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	changed, err := s.proxyRepo.SweepExpiredProxies(ctx, time.Now())
	if err != nil {
		log.Printf("[ProxyExpiry] sweep expired proxies failed: %v", err)
		return
	}
	if changed > 0 {
		log.Printf("[ProxyExpiry] re-routed %d accounts off expired proxies", changed)
	}
}
