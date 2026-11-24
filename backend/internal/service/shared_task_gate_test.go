package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type sharedTaskExpiryRepo struct {
	AccountRepository
	calls atomic.Int32
}

func (r *sharedTaskExpiryRepo) AutoPauseExpiredAccounts(context.Context, time.Time) (int64, error) {
	r.calls.Add(1)
	return 0, nil
}

func TestSharedTasksOneOwnerAcrossEntryHandoff(t *testing.T) {
	var ownership struct {
		sync.Mutex
		id string
	}
	claim := func(id string) (bool, error) {
		ownership.Lock()
		defer ownership.Unlock()
		if ownership.id != "" {
			return false, nil
		}
		ownership.id = id
		return true, nil
	}
	oldControl := config.EntryNodeConfig{
		ClusterEnabled: true, ID: "control-a", Role: config.EntryRoleControlOnly, SharedTaskOwnerID: "control-a",
	}
	newControl := config.EntryNodeConfig{
		ClusterEnabled: true, ID: "control-b", Role: config.EntryRoleControlOnly, SharedTaskOwnerID: "control-b",
	}
	gateway := config.EntryNodeConfig{
		ClusterEnabled: true, ID: "gateway", Role: config.EntryRoleGatewayOnly, SharedTaskOwnerID: "control-a",
	}
	oldGate, err := newSharedTaskGate(oldControl, claim)
	require.NoError(t, err)
	peerGate, err := newSharedTaskGate(gateway, claim)
	require.NoError(t, err)
	replacementGate, err := newSharedTaskGate(newControl, claim)
	require.NoError(t, err)
	require.True(t, oldGate.Running())
	require.False(t, peerGate.Running())
	require.False(t, replacementGate.Running())

	expiry := &sharedTaskExpiryRepo{}
	peer := ProvideAccountExpiryService(expiry, peerGate)
	replacement := ProvideAccountExpiryService(expiry, replacementGate)
	peer.Stop()
	replacement.Stop()
	require.Zero(t, expiry.calls.Load())
	old := ProvideAccountExpiryService(expiry, oldGate)
	old.Stop()
	require.EqualValues(t, 1, expiry.calls.Load())
	oldGate.Stop()

	// Stop alone never clears the database claim.
	blocked, err := newSharedTaskGate(newControl, claim)
	require.NoError(t, err)
	require.False(t, blocked.Running())

	ownership.id = "" // confirmed process exit and explicit operator handoff
	next, err := newSharedTaskGate(newControl, claim)
	require.NoError(t, err)
	require.True(t, next.Running())
	successor := ProvideAccountExpiryService(expiry, next)
	successor.Stop()
	require.EqualValues(t, 2, expiry.calls.Load())
}

func TestSharedTasksStandalonePreservesV2(t *testing.T) {
	gate, err := newSharedTaskGate(config.EntryNodeConfig{}, func(string) (bool, error) {
		t.Fatal("a standalone Entry must not claim clustered ownership")
		return false, nil
	})
	require.NoError(t, err)
	require.True(t, gate.Running())
}
