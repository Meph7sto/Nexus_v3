package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/worker"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type testRedisSignalFailure struct {
	service.WorkerClusterCoordination
	fail           atomic.Bool
	loseBHeartbeat atomic.Bool
}

func (c *testRedisSignalFailure) PutEntryHeartbeat(ctx context.Context, entry service.EntryHeartbeat, ttl time.Duration) error {
	if c.loseBHeartbeat.Load() && entry.NodeID == "b" {
		return errors.New("synthetic lost Redis heartbeat")
	}
	return c.WorkerClusterCoordination.PutEntryHeartbeat(ctx, entry, ttl)
}

func (c *testRedisSignalFailure) PublishWorkerChange(ctx context.Context, version int64) error {
	if c.fail.Load() {
		return errors.New("synthetic redis publication failure")
	}
	return c.WorkerClusterCoordination.PublishWorkerChange(ctx, version)
}

type testClusterRuntime struct {
	*DistributedHTTPUpstream
	reject  atomic.Bool
	reloads atomic.Int64
}

func (r *testClusterRuntime) Reload(cfg config.WorkerPoolConfig) error {
	r.reloads.Add(1)
	if r.reject.Load() {
		return errors.New("synthetic rejected runtime configuration")
	}
	return r.DistributedHTTPUpstream.Reload(cfg)
}

// The supplied endpoints must be disposable loopback-only PostgreSQL and Redis
// instances. This test intentionally applies migrations and creates fixtures.
func TestWorkerClusterLocalPostgresRedisConvergence(t *testing.T) {
	dsn := os.Getenv("NEXUS_TEST_PHASE2_PG")
	redisAddr := os.Getenv("NEXUS_TEST_PHASE2_REDIS")
	if dsn == "" || redisAddr == "" {
		t.Skip("isolated loopback PostgreSQL and Redis fixtures are required")
	}
	dbURL, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "postgres", dbURL.Scheme)
	require.Equal(t, "127.0.0.1", dbURL.Hostname())
	require.Contains(t, []string{"/nexus_phase2_test", "/nexus_phase2_upgrade"}, dbURL.Path)
	require.NotNil(t, dbURL.User)
	require.Equal(t, "nexus_phase2", dbURL.User.Username())
	require.Empty(t, dbURL.Query().Get("host"))
	require.Empty(t, dbURL.Query().Get("hostaddr"))
	redisHost, _, err := net.SplitHostPort(redisAddr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", redisHost)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, db.PingContext(ctx))
	require.NoError(t, client.Ping(ctx).Err())

	legacyFS := fstest.MapFS{}
	files, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	for _, name := range files {
		if name < "253_" {
			content, err := migrations.FS.ReadFile(name)
			require.NoError(t, err)
			legacyFS[name] = &fstest.MapFile{Data: content}
		}
	}
	require.NoError(t, applyMigrationsFS(ctx, db, legacyFS))
	_, err = db.ExecContext(ctx, `
		INSERT INTO users (email,password_hash,balance) VALUES ('phase2-v2-user@example.test','synthetic',12.34);
		INSERT INTO accounts (name,platform,type) VALUES ('phase2-v2-account','openai','apikey');
		INSERT INTO api_keys (user_id,key,name)
			SELECT id,'phase2-synthetic-api-key','phase2-v2-key' FROM users WHERE email='phase2-v2-user@example.test';
		INSERT INTO groups (name) VALUES ('phase2-v2-group');
		INSERT INTO user_subscriptions (user_id,group_id,starts_at,expires_at,notes)
			SELECT u.id,g.id,NOW(),NOW() + interval '30 days','phase2-v2-subscription'
			FROM users u, groups g WHERE u.email='phase2-v2-user@example.test' AND g.name='phase2-v2-group';
		INSERT INTO channels (name) VALUES ('phase2-v2-channel');
		INSERT INTO channel_groups (channel_id,group_id)
			SELECT c.id,g.id FROM channels c,groups g WHERE c.name='phase2-v2-channel' AND g.name='phase2-v2-group';
		INSERT INTO account_groups (account_id,group_id)
			SELECT a.id,g.id FROM accounts a,groups g WHERE a.name='phase2-v2-account' AND g.name='phase2-v2-group';
		INSERT INTO usage_logs (user_id,api_key_id,account_id,request_id,model,input_tokens,output_tokens,actual_cost)
			SELECT k.user_id,k.id,a.id,'phase2-v2-usage','fixture-model',17,8,0.0123
			FROM api_keys k,accounts a WHERE k.name='phase2-v2-key' AND a.name='phase2-v2-account';
		INSERT INTO settings (key,value) VALUES ('phase2-v2-setting','preserved');`)
	require.NoError(t, err)
	readLegacy := func() string {
		t.Helper()
		var result string
		err := db.QueryRowContext(ctx, `
			SELECT jsonb_build_array(
				(SELECT to_jsonb(u) FROM users u WHERE email='phase2-v2-user@example.test'),
				(SELECT to_jsonb(a) FROM accounts a WHERE name='phase2-v2-account'),
				(SELECT to_jsonb(k) FROM api_keys k WHERE name='phase2-v2-key'),
				(SELECT to_jsonb(s) FROM user_subscriptions s WHERE notes='phase2-v2-subscription'),
				(SELECT to_jsonb(c) FROM channels c WHERE name='phase2-v2-channel'),
				(SELECT to_jsonb(l) FROM usage_logs l WHERE request_id='phase2-v2-usage'),
				(SELECT to_jsonb(s) FROM settings s WHERE key='phase2-v2-setting')
			)::text`).Scan(&result)
		require.NoError(t, err)
		return result
	}
	before := readLegacy()
	require.NoError(t, ApplyMigrations(ctx, db))
	var recorded bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename='253_worker_nodes.sql')`).Scan(&recorded))
	require.True(t, recorded)
	require.NoError(t, applyMigrationsFS(ctx, db, legacyFS), "the prior migration manifest can still start after expansion")
	require.Equal(t, before, readLegacy())

	store := NewWorkerNodeStore(db)
	coord := &testRedisSignalFailure{WorkerClusterCoordination: NewWorkerClusterCoordination(client)}
	releaseStream := make(chan struct{})
	var released sync.Once
	release := func() { released.Do(func() { close(releaseStream) }) }
	defer release()
	actualWorker, err := worker.New(worker.Config{
		NodeID: "worker", SharedToken: testWorkerToken,
		AllowedTargets: []string{"api.openai.com"}, AllowPrivateH2C: true,
	}, testWorkerUpstream(func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
		_, _ = io.Copy(io.Discard, req.Body)
		body, writer := io.Pipe()
		go func() {
			defer writer.Close()
			_, _ = io.WriteString(writer, "first")
			select {
			case <-releaseStream:
				_, _ = io.WriteString(writer, "tail")
			case <-req.Context().Done():
			}
		}()
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	}))
	require.NoError(t, err)
	node := testWorkerOrigin(t, "worker", actualWorker)
	poolConfig := testPoolConfig(node)
	poolConfig.HealthInterval = 2 * time.Second
	poolConfig.FallbackLocal = false
	local := testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		return nil, errors.New("unexpected local request")
	})
	startEntry := func(id, role string, running bool) (*service.WorkerClusterService, *testClusterRuntime) {
		t.Helper()
		pool, err := NewDistributedHTTPUpstream(local, poolConfig)
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Close() })
		runtime := &testClusterRuntime{DistributedHTTPUpstream: pool}
		cfg := &config.Config{WorkerPool: poolConfig, EntryNode: config.EntryNodeConfig{
			ClusterEnabled: true, ID: id, Name: id, Role: role, SharedTaskOwnerID: "a",
		}}
		entry, err := service.NewWorkerClusterService(store, coord, runtime, cfg, func() service.EntryProcessStatus {
			return service.EntryProcessStatus{Version: "fixture", ActiveConnections: 2, SharedTasksRunning: running}
		}, nil)
		require.NoError(t, err)
		require.NoError(t, entry.Start(ctx))
		t.Cleanup(entry.Close)
		return entry, runtime
	}
	a, aRuntime := startEntry("a", "all", true)
	b, bRuntime := startEntry("b", "gateway_only", false)
	require.Eventually(t, func() bool {
		count, err := client.PubSubNumSub(ctx, workerClusterReloadChannel).Result()
		return err == nil && count[workerClusterReloadChannel] == 2
	}, 3*time.Second, 20*time.Millisecond)
	readEntry := func(id string) service.EntryHeartbeat {
		entries, _ := coord.ListEntryHeartbeats(ctx)
		for _, entry := range entries {
			if entry.NodeID == id {
				return entry
			}
		}
		return service.EntryHeartbeat{}
	}
	initial, err := a.View(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), initial.Desired.Version)
	require.Len(t, initial.Entries, 2)
	require.NotEqual(t, initial.Entries[0].InstanceID, initial.Entries[1].InstanceID)
	require.True(t, readEntry("a").SharedTasksRunning)
	require.False(t, readEntry("b").SharedTasksRunning)
	ttl, err := client.TTL(ctx, entryHeartbeatPrefix+readEntry("b").InstanceID).Result()
	require.NoError(t, err)
	require.Positive(t, ttl)
	require.LessOrEqual(t, ttl, 30*time.Second)
	data, err := json.Marshal(initial)
	require.NoError(t, err)
	require.NotContains(t, string(data), testWorkerToken)
	join, err := a.JoinTemplate(ctx, "worker")
	require.NoError(t, err)
	require.Contains(t, join, "WORKER_SECRETS_DIR")
	require.NotContains(t, join, testWorkerToken)
	wrongBinding := initial.Desired.Workers[0]
	wrongBinding.URL = "http://127.0.0.1:1/"
	_, err = a.UpsertWorker(ctx, wrongBinding, 1, "")
	require.ErrorIs(t, err, service.ErrWorkerClusterInvalid)

	policy := initial.Desired.Policy
	policy.SelectionPolicy = "least_inflight"
	updated, err := a.UpdatePolicy(ctx, policy, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), updated.Version)
	require.Eventually(t, func() bool { return readEntry("b").AppliedVersion == 2 }, 3*time.Second, 20*time.Millisecond)

	bRuntime.reject.Store(true)
	policy.SelectionPolicy = "round_robin"
	_, err = a.UpdatePolicy(ctx, policy, 2)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		entry := readEntry("b")
		return entry.AppliedVersion == 2 && entry.ApplyError != ""
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, "least_inflight", bRuntime.current.Load().selection)

	coord.fail.Store(true)
	policy.MaxAttempts = 3
	updated, err = a.UpdatePolicy(ctx, policy, 3)
	require.NoError(t, err)
	require.Equal(t, int64(4), updated.Version, "PostgreSQL commit survives a publication failure")
	bRuntime.reject.Store(false)
	require.Eventually(t, func() bool {
		entry := readEntry("b")
		return entry.AppliedVersion == 4 && entry.ApplyError == ""
	}, 35*time.Second, 100*time.Millisecond, "fallback poll converges without a Redis notification")
	require.Equal(t, "round_robin", bRuntime.current.Load().selection)
	coord.fail.Store(false)
	reloads := bRuntime.reloads.Load()
	require.NoError(t, coord.PublishWorkerChange(ctx, 2))
	require.NoError(t, b.Reconcile(ctx))
	require.Equal(t, int64(4), readEntry("b").AppliedVersion)
	require.Equal(t, reloads, bRuntime.reloads.Load(), "old notifications never reset the pool")
	_, err = a.UpdatePolicy(ctx, policy, 3)
	require.ErrorIs(t, err, service.ErrWorkerClusterConflict)

	awaitWorkerHealth(t, aRuntime.current.Load())
	resp, err := aRuntime.Do(testRelayRequest(t, ctx, strings.NewReader("live stream")), testWorkerProxy, 41, 1)
	require.NoError(t, err)
	defer resp.Body.Close()
	first := make([]byte, len("first"))
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, "first", string(first))

	bRuntime.reject.Store(true)
	missingInstanceID := readEntry("b").InstanceID
	coord.loseBHeartbeat.Store(true)
	require.NoError(t, client.Del(ctx, entryHeartbeatPrefix+missingInstanceID).Err())
	require.Empty(t, readEntry("b").InstanceID)
	drain, err := a.DrainWorker(ctx, "worker", 4)
	require.NoError(t, err)
	require.Len(t, drain.Workers[0].DrainEntries, 2)
	require.False(t, drain.Workers[0].Enabled)
	require.Never(t, func() bool {
		health, err := a.ProbeWorker(ctx, "worker")
		return err == nil && health.Status == "draining"
	}, 6*time.Second, 100*time.Millisecond, "a registered Entry without a Redis heartbeat blocks the drain command")
	pending, err := a.DrainStatus(ctx, "worker")
	require.NoError(t, err)
	require.Equal(t, []string{missingInstanceID}, pending.PendingEntries)
	require.Equal(t, []string{missingInstanceID}, pending.UncertainEntries)
	require.False(t, pending.Complete)
	coord.loseBHeartbeat.Store(false)
	bRuntime.reject.Store(false)
	require.NoError(t, b.Reconcile(ctx))
	require.Eventually(t, func() bool {
		health, err := a.DrainStatus(ctx, "worker")
		return err == nil && health.Health != nil && health.Health.Status == "draining" &&
			health.Health.InFlight == 1 && !health.Complete && len(health.PendingEntries) == 0
	}, 7*time.Second, 50*time.Millisecond)

	release()
	tail, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "tail", string(tail))
	require.NoError(t, resp.Body.Close())
	require.Eventually(t, func() bool {
		status, err := a.DrainStatus(ctx, "worker")
		return err == nil && status.Complete
	}, 3*time.Second, 50*time.Millisecond)

	broken := drain
	broken.Workers = append([]service.WorkerNode(nil), drain.Workers...)
	broken.Version++
	broken.Workers[0].Weight = 0
	require.Error(t, store.Save(ctx, drain.Version, broken))
	preserved, err := a.View(ctx)
	require.NoError(t, err)
	require.Equal(t, drain, preserved.Desired, "a bad row rolls back both version and node mutation")
	renamed := preserved.Desired.Workers[0]
	renamed.Name, renamed.Tags, renamed.Note = "maintained worker", []string{"fixture"}, "maintenance verified"
	updated, err = a.UpsertWorker(ctx, renamed, drain.Version, "")
	require.NoError(t, err)
	require.Equal(t, renamed, updated.Workers[0])
	deleted, err := a.DeleteWorker(ctx, "worker", updated.Version)
	require.NoError(t, err)
	require.Empty(t, deleted.Workers)
	require.Equal(t, before, readLegacy())
	instanceID := readEntry("b").InstanceID
	b.Close()
	require.ErrorIs(t, client.Get(ctx, entryHeartbeatPrefix+instanceID).Err(), redis.Nil)
}
