package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestEntryTaskClaimPostgresHandoff(t *testing.T) {
	dsn := os.Getenv("NEXUS_ENTRY_TASK_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXUS_ENTRY_TASK_TEST_POSTGRES_DSN to a throwaway PostgreSQL database")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	schema := pq.QuoteIdentifier("entry_task_test_" + strings.ToLower(rand.Text()))
	_, err = db.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		dropCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, err := db.ExecContext(dropCtx, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	_, err = db.ExecContext(ctx, "SET search_path TO "+schema)
	require.NoError(t, err)
	claimMigration, err := migrations.FS.ReadFile("254_entry_shared_task_claim.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(claimMigration))
	require.NoError(t, err)
	leaseMigration, err := migrations.FS.ReadFile("256_entry_shared_task_lease.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(leaseMigration))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE scheduled_test_plans (
		id BIGINT PRIMARY KEY, enabled BOOLEAN, last_run_at TIMESTAMPTZ,
		next_run_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
	)`)
	require.NoError(t, err)

	ctrl := func(id string) *config.Config {
		return &config.Config{EntryNode: config.EntryNodeConfig{
			ClusterEnabled: true, ID: id, Role: config.EntryRoleControlOnly, SharedTaskOwnerID: id,
		}}
	}
	first, err := service.ProvideSharedTaskGate(ctrl("control-a"), db)
	require.NoError(t, err)
	t.Cleanup(first.Stop)
	first.Begin()
	require.Eventually(t, first.Running, 3*time.Second, 5*time.Millisecond)
	second, err := service.ProvideSharedTaskGate(ctrl("control-b"), db)
	require.NoError(t, err)
	t.Cleanup(second.Stop)
	second.Begin()
	require.Never(t, second.Running, 100*time.Millisecond, 5*time.Millisecond)
	_, err = service.ProvideSharedTaskGate(&config.Config{}, db)
	require.ErrorContains(t, err, "prior clustered Entry task lease")

	now := time.Now().UTC().Truncate(time.Second)
	due := now.Add(-time.Minute)
	next := now.Add(time.Hour)
	_, err = db.ExecContext(ctx, "INSERT INTO scheduled_test_plans (id, enabled, next_run_at) VALUES (1, TRUE, $1)", due)
	require.NoError(t, err)
	plans := NewScheduledTestPlanRepository(db).(service.ScheduledTestPlanClaimRepository)
	claimed, err := plans.ClaimDue(ctx, 1, due, now, next)
	require.NoError(t, err)
	require.True(t, claimed)

	first.Stop()
	require.Eventually(t, second.Running, 4*time.Second, 10*time.Millisecond)
	claimed, err = plans.ClaimDue(ctx, 1, due, now.Add(time.Minute), next)
	require.NoError(t, err)
	require.False(t, claimed, "the replacement cannot consume a stale occurrence")
}
