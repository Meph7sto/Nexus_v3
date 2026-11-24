package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type recoveryStore struct {
	members  []EntryMembership
	snapshot WorkerClusterSnapshot
}

func TestSharedTaskGateDoesNotStartGatewayOpsCollector(t *testing.T) {
	collector := ProvideOpsMetricsCollector(nil, nil, nil, nil, nil, nil, nil, &SharedTaskGate{})
	require.Nil(t, collector.stopCh)
	collector.Stop()
}

func (s *recoveryStore) Load(context.Context) (WorkerClusterSnapshot, error) {
	shallow := s.snapshot
	shallow.Workers = append([]WorkerNode(nil), shallow.Workers...)
	return shallow, nil
}
func (s *recoveryStore) Save(_ context.Context, expected int64, next WorkerClusterSnapshot) error {
	if s.snapshot.Version != expected {
		return ErrWorkerClusterConflict
	}
	s.snapshot = next
	return nil
}
func (s *recoveryStore) ListEntryMemberships(context.Context) ([]EntryMembership, error) {
	return s.members, nil
}
func (s *recoveryStore) SetEntryMembership(_ context.Context, member EntryMembership) error {
	for i, m := range s.members {
		if m.InstanceID == member.InstanceID {
			s.members[i] = member
			return nil
		}
	}
	return ErrWorkerClusterInvalid
}

type recoveryCoord struct {
	clusterStartupCoordination
	entries []EntryHeartbeat
	err     error
}

func (c *recoveryCoord) ListEntryHeartbeats(context.Context) ([]EntryHeartbeat, error) {
	return c.entries, c.err
}

type recoveryRuntime struct {
	clusterStartupRuntime
	online bool
}

func (r *recoveryRuntime) ProbeWorker(context.Context, config.WorkerPoolConfig, config.WorkerNodeConfig) (WorkerProbe, error) {
	if !r.online {
		return WorkerProbe{}, ErrWorkerClusterUnavailable
	}
	return WorkerProbe{Status: "draining", InFlight: 1}, nil
}

func TestWorkerClusterFencedRecovery(t *testing.T) {
	ctx := t.Context()
	cfg := config.WorkerPoolConfig{AllowPrivateH2C: true, Nodes: []config.WorkerNodeConfig{{ID: "worker", URL: "http://127.0.0.1:9999", SharedToken: "fixture"}}}
	store := &recoveryStore{snapshot: snapshotFromBootstrap(cfg), members: []EntryMembership{{InstanceID: "dead"}, {InstanceID: "live"}}}
	coord := &recoveryCoord{entries: []EntryHeartbeat{{InstanceID: "live", Role: "all", AppliedVersion: 1}}}
	transport := &recoveryRuntime{}
	s, err := NewWorkerClusterService(store, coord, transport, &config.Config{WorkerPool: cfg, EntryNode: config.EntryNodeConfig{ClusterEnabled: true, ID: "control", Role: "control_only"}}, nil)
	require.NoError(t, err)
	_, err = s.ForgetOfflineWorker(ctx, "worker", 1)
	require.ErrorIs(t, err, ErrWorkerClusterConflict)
	node := &store.snapshot.Workers[0]
	node.DrainVersion, node.Enabled, node.DrainEntries = 1, false, []string{"dead", "live"}
	_, err = s.ForgetOfflineWorker(ctx, "worker", 1)
	require.ErrorIs(t, err, ErrWorkerClusterConflict)
	require.ErrorIs(t, s.RetireOfflineEntry(ctx, "live"), ErrWorkerClusterConflict)
	require.ErrorIs(t, s.RetireOfflineEntry(ctx, "unknown"), ErrWorkerClusterConflict)
	coord.err = context.DeadlineExceeded
	require.ErrorIs(t, s.RetireOfflineEntry(ctx, "dead"), ErrWorkerClusterUnavailable)
	coord.err = nil
	require.NoError(t, s.RetireOfflineEntry(ctx, "dead"))
	_, err = s.DeleteWorker(ctx, "worker", 1)
	require.ErrorIs(t, err, ErrWorkerClusterConflict)
	transport.online = true
	_, err = s.ForgetOfflineWorker(ctx, "worker", 1)
	require.ErrorIs(t, err, ErrWorkerClusterConflict)
	transport.online = false
	_, err = s.ForgetOfflineWorker(ctx, "worker", 0)
	require.ErrorIs(t, err, ErrWorkerClusterConflict)
	got, err := s.ForgetOfflineWorker(ctx, "worker", 1)
	require.NoError(t, err)
	require.Empty(t, got.Workers)
	require.Equal(t, int64(2), got.Version)
}
