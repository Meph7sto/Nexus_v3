package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// An opt-in drill; the operator controls only the disposable Redis containers.
func TestRedisSentinelIsolatedDrill(t *testing.T) {
	if os.Getenv("NEXUS_REDIS_SENTINEL_DRILL") != "1" {
		t.Skip("start the disposable Redis Sentinel drill")
	}
	secret := func(path string) string {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		return strings.TrimSpace(string(b))
	}
	dataPassword := secret("/run/secrets/redis_data_password")
	sentinelPassword := secret("/run/secrets/redis_sentinel_password")
	cfg := &config.Config{Redis: config.RedisConfig{
		Password: dataPassword, PoolSize: 1,
		DialTimeoutSeconds: 1, ReadTimeoutSeconds: 2, WriteTimeoutSeconds: 2,
		Sentinel: config.RedisSentinelConfig{
			MasterName: "nexus", Password: sentinelPassword,
			Addresses:        []string{"redis-sentinel-a:26379", "redis-sentinel-b:26379", "redis-sentinel-c:26379"},
			AllowedPrimaries: []string{"redis-a:6379", "redis-b:6379"},
		},
	}}
	require.NoError(t, cfg.Redis.Sentinel.Validate())
	rdb := InitRedis(cfg)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	sentinel := redis.NewSentinelClient(&redis.Options{Addr: "redis-sentinel-b:26379", Password: sentinelPassword})
	defer sentinel.Close()
	require.Eventually(t, func() bool {
		probe, done := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer done()
		ck := sentinel.CkQuorum(probe, "nexus")
		return ck.Err() == nil && rdb.Set(probe, "nexus:drill:probe", "old", time.Minute).Err() == nil
	}, 25*time.Second, 250*time.Millisecond, "Sentinel quorum and an initial writable replicated primary")

	concurrency := NewConcurrencyCache(rdb, 5, 30)
	auth := NewAPIKeyCache(rdb)
	limit := NewRPMCache(rdb)
	locks := NewLeaderLockCache(rdb)
	billing := NewBillingCache(rdb)
	const id = 90117
	slot, err := concurrency.AcquireUserSlot(ctx, id, 1, "initial-request")
	require.NoError(t, err)
	require.True(t, slot)
	require.NoError(t, auth.SetAuthCache(ctx, "drill-auth", &service.APIKeyAuthCacheEntry{NotFound: true}, 5*time.Minute))
	_, err = limit.IncrementRPM(ctx, id)
	require.NoError(t, err)
	owned, err := locks.TryAcquireLeaderLock(ctx, "nexus:drill:leader", "old-owner", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, owned)
	window := time.Now().UTC().Truncate(time.Minute)
	entry := &service.UserPlatformQuotaCacheEntry{SchemaVersion: 1,
		DailyWindowStart: &window, WeeklyWindowStart: &window, MonthlyWindowStart: &window,
	}
	require.NoError(t, billing.SetUserPlatformQuotaCache(ctx, id, service.PlatformOpenAI, entry, 5*time.Minute))
	require.NoError(t, billing.IncrUserPlatformQuotaUsageCache(ctx, id, service.PlatformOpenAI, 1.25, 5*time.Minute, true))
	acked, err := rdb.Wait(ctx, 1, 5*time.Second).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, acked)

	invalidated := make(chan string, 10)
	go func() {
		_ = auth.SubscribeAuthCacheInvalidation(ctx, func(key string) {
			select {
			case invalidated <- key:
			default:
			}
		})
	}()
	require.Eventually(t, func() bool {
		_ = auth.PublishAuthCacheInvalidation(ctx, "before")
		select {
		case <-invalidated:
			return true
		case <-time.After(250 * time.Millisecond):
			return false
		}
	}, 4*time.Second, 200*time.Millisecond)
	fmt.Printf("REDIS_DRILL_READY replica_acks=%d\n", acked)
	start := time.Now()
	require.Eventually(t, func() bool {
		lookup, done := context.WithTimeout(ctx, time.Second)
		defer done()
		primary, err := sentinel.GetMasterAddrByName(lookup, "nexus").Result()
		if err != nil || len(primary) != 2 || primary[0] != "redis-b" {
			return false
		}
		value, err := rdb.Get(lookup, "nexus:drill:probe").Result()
		return err == nil && value == "old"
	}, 42*time.Second, 250*time.Millisecond, "same data client can read acknowledged state after master switch")
	fmt.Printf("REDIS_DRILL_PROMOTED read_delay_ms=%d\n", time.Since(start).Milliseconds())
	err = rdb.Set(ctx, "nexus:drill:unsafe", "must be blocked", time.Minute).Err()
	require.ErrorContains(t, err, "NOREPLICAS")
	// The concurrency read prunes expired entries in Lua, so it is a
	// write-capable operation and also fails closed without a replica.
	_, err = concurrency.GetUserConcurrency(ctx, id)
	require.ErrorContains(t, err, "NOREPLICAS")
	cached, err := auth.GetAuthCache(ctx, "drill-auth")
	require.NoError(t, err)
	require.True(t, cached.NotFound)
	fmt.Println("REDIS_DRILL_WAIT_FOR_REJOIN lone_primary_writes_blocked=true slot_admission_blocked=true confirmed_auth=true")

	require.Eventually(t, func() bool {
		probe, done := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer done()
		return rdb.Set(probe, "nexus:drill:probe", "new", time.Minute).Err() == nil
	}, 36*time.Second, 250*time.Millisecond, "the same data client is writable after the standby returns")
	fmt.Printf("REDIS_DRILL_WRITABLE total_delay_ms=%d\n", time.Since(start).Milliseconds())
	n, err := concurrency.GetUserConcurrency(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the replicated active slot survived failover")
	other, err := locks.TryAcquireLeaderLock(ctx, "nexus:drill:leader", "another-owner", time.Minute)
	require.NoError(t, err)
	require.False(t, other, "replicated leader lock was not overwritten")
	require.NoError(t, locks.ReleaseLeaderLock(ctx, "nexus:drill:leader", "old-owner"))
	other, err = locks.TryAcquireLeaderLock(ctx, "nexus:drill:leader", "another-owner", time.Minute)
	require.NoError(t, err)
	require.True(t, other)

	blocked, err := concurrency.AcquireUserSlot(ctx, id, 1, "second-request")
	require.NoError(t, err)
	require.False(t, blocked)
	policy, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	require.NoError(t, err)
	require.Equal(t, "noeviction", policy["maxmemory-policy"])
	require.NoError(t, rdb.ConfigSet(ctx, "maxmemory", "1").Err())
	require.ErrorContains(t, rdb.Set(ctx, "nexus:drill:pressure", "must be blocked", time.Minute).Err(), "OOM")
	require.NoError(t, rdb.ConfigSet(ctx, "maxmemory", "201326592").Err())
	n, err = concurrency.GetUserConcurrency(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 1, n, "memory pressure did not evict the active slot")
	require.NoError(t, concurrency.ReleaseUserSlot(ctx, id, "initial-request"))
	slot, err = concurrency.AcquireUserSlot(ctx, id, 1, "second-request")
	require.NoError(t, err)
	require.True(t, slot)
	rpm, err := limit.IncrementRPM(ctx, id)
	require.NoError(t, err)
	require.Positive(t, rpm)
	require.NoError(t, auth.DeleteAuthCache(ctx, "drill-auth"))
	require.Eventually(t, func() bool {
		_ = auth.PublishAuthCacheInvalidation(ctx, "after")
		select {
		case key := <-invalidated:
			return key == "after"
		case <-time.After(250 * time.Millisecond):
			return false
		}
	}, 5*time.Second, 200*time.Millisecond, "original auth invalidation subscriber recovers")

	tw, err := service.NewTimingWheelService()
	require.NoError(t, err)
	defer tw.Stop()
	cfg.Database.UserPlatformQuotaFlusherEnabled = true
	cfg.Database.UserPlatformQuotaFlushIntervalMs = 1000
	quota := &redisDrillQuotaRepo{written: make(chan []service.UserPlatformQuotaSnapshot, 1)}
	flusher := service.NewUserPlatformQuotaUsageFlusher(cfg, billing, quota, tw)
	flusher.Start()
	defer flusher.Stop()
	select {
	case snapshots := <-quota.written:
		require.Len(t, snapshots, 1)
		require.EqualValues(t, id, snapshots[0].UserID)
		require.InDelta(t, 1.25, snapshots[0].DailyUsageUSD, 0.001)
	case <-time.After(6 * time.Second):
		t.Fatal("a replicated dirty quota entry was not written to the repository fixture")
	}
	fmt.Println("REDIS_DRILL_VERIFIED slots=ok auth_cache_and_pubsub=ok rpm=ok leader_lock=ok quota_flush=ok noeviction=ok")
}

type redisDrillQuotaRepo struct {
	service.UserPlatformQuotaRepository
	written chan []service.UserPlatformQuotaSnapshot
}

func (r *redisDrillQuotaRepo) BatchSnapshotUsage(_ context.Context, records []service.UserPlatformQuotaSnapshot, _ time.Time) error {
	r.written <- records
	return nil
}
