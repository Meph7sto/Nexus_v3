//go:build cluster_data_integration

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestClusterDataProbeWithIsolatedPostgresRedis(t *testing.T) {
	pgPort, pgErr := strconv.Atoi(os.Getenv("NEXUS_TEST_CLUSTER_PG_PORT"))
	redisPort, redisErr := strconv.Atoi(os.Getenv("NEXUS_TEST_CLUSTER_REDIS_PORT"))
	password := os.Getenv("NEXUS_TEST_CLUSTER_PG_PASSWORD")
	if pgErr != nil || redisErr != nil || password == "" {
		t.Skip("set ports and fixture password for an isolated loopback PostgreSQL and Redis")
	}
	pg := config.DatabaseConfig{
		Host: "127.0.0.1", Port: pgPort, DBName: "nexus_phase2_test", User: "nexus_phase2",
		Password: password, SSLMode: "disable",
	}
	redisDB := config.RedisConfig{Host: "127.0.0.1", Port: redisPort, DB: 1}
	same := config.ClusterDataProfile{ID: "same", Postgres: pg, Redis: redisDB}
	separate := same
	separate.ID, separate.Redis.DB = "different-redis", 2
	cfg := &config.Config{Database: pg, Redis: redisDB, ClusterData: config.ClusterDataConfig{
		Profiles: []config.ClusterDataProfile{same, separate},
	}}
	pool, err := sql.Open("postgres", pg.DSN())
	require.NoError(t, err)
	defer pool.Close()
	s := service.NewClusterDataService(cfg, pool, ProvideClusterMigrationCheck(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	summary := s.View(ctx)
	require.Equal(t, "primary", summary.Postgres.Status)
	require.Equal(t, "primary", summary.Redis.Status)
	require.Equal(t, "standalone", summary.Redis.Mode)
	require.Equal(t, "unknown", summary.Backup.Status)

	state, err := s.Probe(ctx, service.ClusterDataRequest{ProfileID: "same", Intent: service.DataIntentSameCluster})
	require.NoError(t, err)
	require.True(t, state.Ready, "%+v", state.Checks)
	require.Greater(t, state.MigrationCount, 315)
	blocked, err := s.Probe(ctx, service.ClusterDataRequest{ProfileID: "different-redis", Intent: service.DataIntentSameCluster})
	require.NoError(t, err)
	require.False(t, blocked.Ready, "changing the Redis logical database is not a same-cluster address change")
	_, err = s.Export(ctx, service.ClusterDataRequest{ProfileID: "different-redis", Intent: service.DataIntentSameCluster})
	require.Error(t, err)
	exported, err := s.Export(ctx, service.ClusterDataRequest{ProfileID: "different-redis", Intent: service.DataIntentMigration})
	require.NoError(t, err)
	require.True(t, exported.Preflight.Ready)
	b, err := json.Marshal(exported)
	require.NoError(t, err)
	require.NotContains(t, string(b), password)
	require.False(t, strings.Contains(strings.ToLower(string(b)), `"password":`))
	require.Contains(t, exported.RequiredEnv, "DATABASE_PASSWORD")
}
