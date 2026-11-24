package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPoolConfigAndEntryBudgetOverrides(t *testing.T) {
	resetViperWithJWTSecret(t)
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
gateway:
  request_body_memory_budget_bytes: 1073741824
  max_body_size: 268435456
redis:
  pool_size: 1024
database:
  max_open_conns: 100
  max_idle_conns: 16
worker_pool:
  enabled: false
  health_interval: 3s
  nodes:
    - id: worker-a
      url: https://worker-a.internal:9090
      shared_token_file: /run/nexus-worker-secrets/worker-a-token
      weight: 1
`), 0600))
	t.Setenv("CONFIG_FILE", configFile)
	t.Setenv("WORKER_POOL_ENABLED", "true")
	t.Setenv("WORKER_POOL_DEFAULT_PROXY_URL", "socks5://proxy.internal:1080")
	t.Setenv("GATEWAY_REQUEST_BODY_MEMORY_BUDGET_BYTES", "134217728")
	t.Setenv("GATEWAY_MAX_BODY_SIZE", "33554432")
	t.Setenv("GATEWAY_TEXT_MAX_BODY_SIZE", "33554432")
	t.Setenv("REDIS_POOL_SIZE", "48")
	t.Setenv("REDIS_MIN_IDLE_CONNS", "8")
	t.Setenv("DATABASE_MAX_OPEN_CONNS", "18")
	t.Setenv("DATABASE_MAX_IDLE_CONNS", "8")

	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.WorkerPool.Enabled)
	require.Equal(t, 3*time.Second, cfg.WorkerPool.HealthInterval)
	require.Equal(t, 2*time.Second, cfg.WorkerPool.HealthTimeout)
	require.Equal(t, "socks5://proxy.internal:1080", cfg.WorkerPool.DefaultProxyURL)
	require.Len(t, cfg.WorkerPool.Nodes, 1)
	require.Equal(t, "worker-a", cfg.WorkerPool.Nodes[0].ID)
	require.Equal(t, "/run/nexus-worker-secrets/worker-a-token", cfg.WorkerPool.Nodes[0].SharedTokenFile)
	require.Equal(t, int64(128<<20), cfg.Gateway.RequestBodyMemoryBudgetBytes)
	require.Equal(t, int64(32<<20), cfg.Gateway.MaxBodySize)
	require.Equal(t, int64(32<<20), cfg.Gateway.TextMaxBodySize)
	require.Equal(t, 48, cfg.Redis.PoolSize)
	require.Equal(t, 8, cfg.Redis.MinIdleConns)
	require.Equal(t, 18, cfg.Database.MaxOpenConns)
	require.Equal(t, 8, cfg.Database.MaxIdleConns)
}
