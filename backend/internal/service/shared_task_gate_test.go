package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func sharedTaskPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("NEXUS_ENTRY_TASK_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXUS_ENTRY_TASK_TEST_POSTGRES_DSN to a throwaway PostgreSQL database")
	}
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	name := "shared_task_" + strings.ToLower(rand.Text())
	schema := pq.QuoteIdentifier(name)
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	if strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "postgres://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", name)
		u.RawQuery = query.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + name
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	apply := func(name string) {
		raw, err := migrations.FS.ReadFile(name)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(raw))
		require.NoError(t, err)
	}
	apply("254_entry_shared_task_claim.sql")
	_, err = db.ExecContext(ctx, "INSERT INTO entry_shared_task_claim (entry_id, process_token) VALUES ('legacy', 'legacy')")
	require.NoError(t, err)
	apply("256_entry_shared_task_lease.sql")
	return db
}

type sharedTaskTestStore struct {
	sharedTaskLeaseStore
	unavailable atomic.Bool
	attempts    atomic.Int32
}

func (s *sharedTaskTestStore) claim(ctx context.Context, id, token string, ttl time.Duration) (bool, error) {
	s.attempts.Add(1)
	if s.unavailable.Load() {
		return false, errors.New("database unavailable")
	}
	return s.sharedTaskLeaseStore.claim(ctx, id, token, ttl)
}

type sharedTaskExpiryTestRepo struct {
	AccountRepository
	calls    atomic.Int32
	canceled atomic.Int32
}

type sharedTaskAuditTestRepo struct {
	AuditLogRepository
	written atomic.Int32
}

func (r *sharedTaskAuditTestRepo) BatchInsert(_ context.Context, entries []*AuditLog) (int64, error) {
	r.written.Add(int32(len(entries)))
	return int64(len(entries)), nil
}

func (r *sharedTaskExpiryTestRepo) AutoPauseExpiredAccounts(ctx context.Context, _ time.Time) (int64, error) {
	r.calls.Add(1)
	<-ctx.Done()
	r.canceled.Add(1)
	return 0, ctx.Err()
}

type sharedTaskRenewTestStore struct {
	sharedTaskLeaseStore
	attempts atomic.Int32
	released atomic.Bool
}

func (s *sharedTaskRenewTestStore) claim(context.Context, string, string, time.Duration) (bool, error) {
	s.attempts.Add(1)
	return true, nil
}

func (s *sharedTaskRenewTestStore) release(context.Context, string) error {
	s.released.Store(true)
	return nil
}

func TestSharedTaskLongRunningJobDoesNotDelayRenewal(t *testing.T) {
	store := &sharedTaskRenewTestStore{}
	gate, err := newSharedTaskGate(config.EntryNodeConfig{
		ClusterEnabled: true, ID: "test", Role: config.EntryRoleAll,
	}, store, sharedTaskLeaseTiming{
		ttl: 500 * time.Millisecond, interval: 25 * time.Millisecond, queryTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	jobStarted := make(chan struct{})
	jobCanceled := make(chan struct{})
	allowDrain := make(chan struct{})
	gate.Register(func(ctx context.Context) {
		close(jobStarted)
		<-ctx.Done()
		close(jobCanceled)
		<-allowDrain
	})
	gate.Begin()
	t.Cleanup(gate.Stop)
	var drainAllowed bool
	defer func() {
		if !drainAllowed {
			close(allowDrain)
		}
	}()
	select {
	case <-jobStarted:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
	require.Eventually(t, func() bool { return store.attempts.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.True(t, gate.Running())
	stopped := make(chan struct{})
	go func() {
		gate.Stop()
		close(stopped)
	}()
	select {
	case <-jobCanceled:
	case <-time.After(time.Second):
		t.Fatal("job was not canceled")
	}
	require.False(t, gate.Running())
	require.False(t, store.released.Load(), "release must wait for local work to drain")
	close(allowDrain)
	drainAllowed = true
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("gate did not stop")
	}
	require.True(t, store.released.Load())
}

func TestSharedTaskPostgresLeaseHandoff(t *testing.T) {
	db := sharedTaskPostgres(t)
	store := sharedTaskSQLLease{db}
	ctx := t.Context()
	timing := sharedTaskLeaseTiming{
		ttl: 600 * time.Millisecond, interval: 100 * time.Millisecond,
		queryTimeout: 200 * time.Millisecond, standbyDelay: 30 * time.Millisecond,
	}
	node := func(id, preferred, role string) config.EntryNodeConfig {
		return config.EntryNodeConfig{ClusterEnabled: true, ID: id, Role: role, SharedTaskOwnerID: preferred}
	}
	newGate := func(cfg config.EntryNodeConfig, store sharedTaskLeaseStore) *SharedTaskGate {
		gate, err := newSharedTaskGate(cfg, store, timing)
		require.NoError(t, err)
		t.Cleanup(gate.Stop)
		return gate
	}
	eventually := func(f func() bool) { require.Eventually(t, f, 3*time.Second, 5*time.Millisecond) }
	token := func() string {
		var token string
		err := db.QueryRowContext(ctx, "SELECT process_token FROM entry_shared_task_claim").Scan(&token)
		require.NoError(t, err)
		return token
	}

	// Existing rows expire with the additive migration rather than fencing out
	// a standalone rollback indefinitely.
	standalone := newGate(config.EntryNodeConfig{}, store)
	standalone.Begin()
	require.True(t, standalone.Running())
	standalone.Stop()
	require.Equal(t, "legacy", token())

	primaryStore := &sharedTaskTestStore{sharedTaskLeaseStore: store}
	primary := newGate(node("primary", "primary", config.EntryRoleControlOnly), primaryStore)
	repo := &sharedTaskExpiryTestRepo{}
	ProvideAccountExpiryService(repo, primary)
	primary.Begin()
	eventually(func() bool { return primary.Running() && repo.calls.Load() == 1 })
	require.Equal(t, primary.token, token())

	_, err := newSharedTaskGate(config.EntryNodeConfig{}, store, timing)
	require.ErrorContains(t, err, "unexpired prior clustered Entry task lease")

	sameID := newGate(node("primary", "primary", config.EntryRoleControlOnly), store)
	sameID.Begin()
	standby := newGate(node("standby", "primary", config.EntryRoleAll), store)
	standbyRepo := &sharedTaskExpiryTestRepo{}
	ProvideAccountExpiryService(standbyRepo, standby)
	standby.Begin()
	gateway := newGate(node("gateway", "", config.EntryRoleGatewayOnly), store)
	gateway.Register(func(context.Context) { t.Error("gateway-only started a shared task") })
	gateway.Begin()
	eventually(func() bool { return primaryStore.attempts.Load() >= 2 })
	require.False(t, sameID.Running())
	require.False(t, standby.Running())
	require.False(t, gateway.Running())
	require.Equal(t, primary.token, token(), "renewal and same-ID contenders cannot change the holder's token")
	sameID.Stop()
	require.Equal(t, primary.token, token(), "a contender's Stop cannot delete another process's lease")

	primary.Stop()
	require.EqualValues(t, 1, repo.canceled.Load(), "Stop joins the old process's jobs")
	eventually(func() bool { return standby.Running() && standbyRepo.calls.Load() == 1 })
	require.Equal(t, standby.token, token())

	restart := newGate(node("primary", "primary", config.EntryRoleControlOnly), store)
	restart.Begin()
	require.Never(t, restart.Running, 180*time.Millisecond, 5*time.Millisecond, "the designated owner cannot preempt a healthy standby")
	standby.Stop()
	eventually(restart.Running)
	require.Equal(t, restart.token, token())
	restart.Stop()
	require.ErrorIs(t, db.QueryRowContext(ctx, "SELECT process_token FROM entry_shared_task_claim").Scan(new(string)), sql.ErrNoRows)
}

func TestSharedTaskPostgresLeaseExpiryAndLoss(t *testing.T) {
	db := sharedTaskPostgres(t)
	store := sharedTaskSQLLease{db}
	timing := sharedTaskLeaseTiming{
		ttl: 700 * time.Millisecond, interval: 110 * time.Millisecond,
		queryTimeout: 200 * time.Millisecond, standbyDelay: 30 * time.Millisecond,
	}
	eventually := func(f func() bool) { require.Eventually(t, f, 3*time.Second, 5*time.Millisecond) }
	gate := func(id string, store sharedTaskLeaseStore, repo *sharedTaskExpiryTestRepo) *SharedTaskGate {
		g, err := newSharedTaskGate(config.EntryNodeConfig{ClusterEnabled: true, ID: id, Role: config.EntryRoleAll}, store, timing)
		require.NoError(t, err)
		ProvideAccountExpiryService(repo, g)
		g.Begin()
		t.Cleanup(g.Stop)
		return g
	}
	lostStore := &sharedTaskTestStore{sharedTaskLeaseStore: store}
	oldRepo := &sharedTaskExpiryTestRepo{}
	old, err := newSharedTaskGate(config.EntryNodeConfig{
		ClusterEnabled: true, ID: "old", Role: config.EntryRoleAll,
	}, lostStore, timing)
	require.NoError(t, err)
	auditRepo := &sharedTaskAuditTestRepo{}
	audit := ProvideAuditLogService(auditRepo, nil, old)
	t.Cleanup(audit.Stop)
	ProvideAccountExpiryService(oldRepo, old)
	old.Begin()
	t.Cleanup(old.Stop)
	eventually(func() bool { return old.Running() && oldRepo.calls.Load() == 1 })
	lostStore.unavailable.Store(true)
	newRepo := &sharedTaskExpiryTestRepo{}
	next := gate("next", store, newRepo)
	eventually(func() bool { return lostStore.attempts.Load() >= 2 })
	require.True(t, old.Running(), "an unavailable database does not revoke the last confirmed lease early")
	require.False(t, next.Running())

	// No Stop or row deletion for the old process: expiry alone permits takeover.
	eventually(func() bool { return next.Running() && newRepo.calls.Load() == 1 && oldRepo.canceled.Load() == 1 })
	require.False(t, old.Running())

	audit.Record(&AuditLog{})
	eventually(func() bool { return auditRepo.written.Load() == 1 })
	require.False(t, old.Running(), "lease loss must not stop the local request audit writer")

	// Replace the row's token while its former owner is running. A zero-row
	// renewal must cancel that owner's work, even before its local TTL is up.
	ctx := t.Context()
	_, err = db.ExecContext(ctx, "UPDATE entry_shared_task_claim SET process_token = 'replacement', lease_expires_at = clock_timestamp() + INTERVAL '2 seconds'")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !next.Running() && newRepo.canceled.Load() == 1
	}, 500*time.Millisecond, 5*time.Millisecond)
	next.Stop()
	var token string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT process_token FROM entry_shared_task_claim").Scan(&token))
	require.Equal(t, "replacement", token)

	_, err = db.ExecContext(ctx, "UPDATE entry_shared_task_claim SET lease_expires_at = clock_timestamp()")
	require.NoError(t, err)
	lostStore.unavailable.Store(false)
	eventually(func() bool { return old.Running() && oldRepo.calls.Load() == 2 })
	require.EqualValues(t, 1, oldRepo.canceled.Load(), "the original process restarted its shared job without restarting the Entry")
}
