package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisSentinelEnvironment(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("REDIS_SENTINEL_ADDRESSES", "sentinel-a.internal:26379,sentinel-b.internal:26379")
	t.Setenv("REDIS_SENTINEL_ALLOWED_PRIMARIES", "redis-a.internal:6379,redis-b.internal:6379")
	t.Setenv("REDIS_SENTINEL_MASTER_NAME", "nexus")
	t.Setenv("REDIS_SENTINEL_USERNAME", "sentinel-client")
	t.Setenv("REDIS_SENTINEL_PASSWORD", "test-sentinel-auth")
	t.Setenv("REDIS_SENTINEL_ENABLE_TLS", "true")
	t.Setenv("REDIS_USERNAME", "redis-client")
	t.Setenv("REDIS_PASSWORD", "test-data-auth")
	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.Redis.Sentinel.Configured())
	require.Equal(t, []string{"sentinel-a.internal:26379", "sentinel-b.internal:26379"}, cfg.Redis.Sentinel.Addresses)
	require.Equal(t, []string{"redis-a.internal:6379", "redis-b.internal:6379"}, cfg.Redis.Sentinel.AllowedPrimaries)
	require.Equal(t, "nexus", cfg.Redis.Sentinel.MasterName)
	require.Equal(t, "sentinel-client", cfg.Redis.Sentinel.Username)
	require.Equal(t, "test-sentinel-auth", cfg.Redis.Sentinel.Password)
	require.True(t, cfg.Redis.Sentinel.EnableTLS)
	require.Equal(t, "redis-client", cfg.Redis.Username)
	require.Equal(t, "test-data-auth", cfg.Redis.Password)
}

func TestRedisSentinelPartialConfigFailsClosed(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("REDIS_SENTINEL_MASTER_NAME", "nexus")
	_, err := Load()
	require.ErrorContains(t, err, "redis.sentinel requires addresses")
}

func TestRedisSentinelEndpointValidation(t *testing.T) {
	c := RedisSentinelConfig{
		MasterName: "nexus", Addresses: []string{"sentinel.internal.:26379"},
		AllowedPrimaries: []string{"redis-a.internal:6379", "[fd00::12]:6379"},
	}
	require.NoError(t, c.Validate())
	c.AllowedPrimaries = []string{"SENTINEL.internal:26379"}
	require.ErrorContains(t, c.Validate(), "allowed_primaries[0] must be a unique")
	c.AllowedPrimaries = []string{"redis://credential@host:6379"}
	err := c.Validate()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "credential")
}
