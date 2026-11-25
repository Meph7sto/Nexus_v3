package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type clusterStartupStore struct {
	unavailable atomic.Bool
	registered  atomic.Bool
	retired     atomic.Bool
}

func (s *clusterStartupStore) Load(context.Context) (WorkerClusterSnapshot, error) {
	if s.unavailable.Load() {
		return WorkerClusterSnapshot{}, errors.New("fixture unavailable")
	}
	return snapshotFromBootstrap(config.WorkerPoolConfig{Enabled: true}), nil
}

func (*clusterStartupStore) Save(context.Context, int64, WorkerClusterSnapshot) error {
	return errors.New("unexpected bootstrap overwrite")
}

func (s *clusterStartupStore) SetEntryMembership(_ context.Context, member EntryMembership) error {
	if !member.Retired {
		s.registered.Store(true)
	} else {
		s.retired.Store(true)
	}
	return nil
}

func (*clusterStartupStore) ListEntryMemberships(context.Context) ([]EntryMembership, error) {
	return nil, nil
}

type clusterStartupCoordination struct{}

func (clusterStartupCoordination) SubscribeWorkerChanges(context.Context) <-chan int64 {
	return nil
}
func (clusterStartupCoordination) PublishWorkerChange(context.Context, int64) error { return nil }
func (clusterStartupCoordination) PutEntryHeartbeat(context.Context, EntryHeartbeat, time.Duration) error {
	return nil
}
func (clusterStartupCoordination) ListEntryHeartbeats(context.Context) ([]EntryHeartbeat, error) {
	return nil, nil
}
func (clusterStartupCoordination) DeleteEntryHeartbeat(context.Context, string) error { return nil }

type clusterStartupRuntime struct {
	store   *clusterStartupStore
	enabled atomic.Bool
}

func (*clusterStartupRuntime) Validate(config.WorkerPoolConfig) error { return nil }

func (r *clusterStartupRuntime) Reload(cfg config.WorkerPoolConfig) error {
	if cfg.Enabled && !r.store.registered.Load() {
		return errors.New("Worker snapshot would serve before registration")
	}
	r.enabled.Store(cfg.Enabled)
	return nil
}

func (*clusterStartupRuntime) ProbeWorker(context.Context, config.WorkerPoolConfig, config.WorkerNodeConfig) (WorkerProbe, error) {
	return WorkerProbe{}, nil
}

func (*clusterStartupRuntime) DrainWorker(context.Context, config.WorkerPoolConfig, config.WorkerNodeConfig) error {
	return nil
}

func TestWorkerClusterStartupRecoversByPollAfterPGUnavailable(t *testing.T) {
	store := &clusterStartupStore{}
	store.unavailable.Store(true)
	runtime := &clusterStartupRuntime{store: store}
	cfg := &config.Config{EntryNode: config.EntryNodeConfig{ClusterEnabled: true, ID: "entry-a", Role: config.EntryRoleAll}}
	control, err := NewWorkerClusterService(store, clusterStartupCoordination{}, runtime, cfg, nil, nil)
	require.NoError(t, err)
	control.pollEvery = 5 * time.Millisecond
	require.Error(t, control.Start(t.Context()))
	t.Cleanup(control.Close)
	require.False(t, runtime.enabled.Load())
	require.False(t, store.registered.Load())

	store.unavailable.Store(false)
	require.Eventually(t, func() bool {
		return control.applied() == 1 && runtime.enabled.Load()
	}, time.Second, 5*time.Millisecond)
	require.True(t, store.registered.Load())
	control.Close()
	require.False(t, runtime.enabled.Load(), "retirement stops new Worker dispatch")
	require.True(t, store.retired.Load())
}
