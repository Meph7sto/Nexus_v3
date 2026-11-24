//go:build integration

package repository

import (
	"database/sql"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestWorkerRecoveryRetainsOnlyNeededTombstones(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("postgres", integrationDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, `CREATE TEMP TABLE settings (LIKE public.settings INCLUDING ALL);
		CREATE TEMP TABLE worker_nodes (LIKE public.worker_nodes INCLUDING ALL);
		INSERT INTO settings(key,value) VALUES ('not_a_membership','not JSON')`)
	require.NoError(t, err)
	store := NewWorkerNodeStore(db)
	for _, id := range []string{"live", "old", "needed"} {
		require.NoError(t, store.SetEntryMembership(ctx, service.EntryMembership{InstanceID: id}))
		if id != "live" {
			require.NoError(t, store.SetEntryMembership(ctx, service.EntryMembership{InstanceID: id, Retired: true}))
		}
	}
	_, err = db.ExecContext(ctx, `UPDATE settings SET updated_at=NOW()-INTERVAL '8 days'`)
	require.NoError(t, err)
	next := service.WorkerClusterSnapshot{Version: 1, Workers: []service.WorkerNode{{NodeID: "worker", Name: "worker", URL: "https://worker.test", Weight: 1, SecretRef: "worker", DrainVersion: 1, DrainEntries: []string{"needed"}}}}
	require.NoError(t, store.Save(ctx, 0, next))
	members, err := store.ListEntryMemberships(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []service.EntryMembership{{InstanceID: "live"}, {InstanceID: "needed", Retired: true}}, members)
	next.Version, next.Workers = 2, nil
	require.NoError(t, store.Save(ctx, 1, next))
	members, err = store.ListEntryMemberships(ctx)
	require.NoError(t, err)
	require.Equal(t, []service.EntryMembership{{InstanceID: "live"}}, members)
}
