//go:build unit

package service

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type broadcastTestSettings struct{ settingRepoStub }

func (r *broadcastTestSettings) GetAll(context.Context) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.values), nil
}

func (r *broadcastTestSettings) SetMultiple(_ context.Context, updates map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	maps.Copy(r.values, updates)
	return nil
}

func TestSettingsBroadcastRefreshesOtherEntry(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	repo := &broadcastTestSettings{settingRepoStub{values: map[string]string{SettingKeyClaudeCodeClientVersion: claudeCodeTestVersion(1)}}}
	writer := NewSettingService(repo, &config.Config{})
	peer := NewSettingService(repo, &config.Config{})
	resetGatewayForwardingSettingsCacheForTest(t)
	defer writer.refreshCachedSettings(&SystemSettings{})
	writer.StartSettingsBroadcast(client)
	defer writer.StopSettingsBroadcast()
	peer.StartSettingsBroadcast(client)
	defer peer.StopSettingsBroadcast()
	require.Eventually(t, func() bool {
		count, err := client.PubSubNumSub(ctx, settingsChangeChannel).Result()
		return err == nil && count[settingsChangeChannel] == 2
	}, time.Second, 5*time.Millisecond)
	// This cache is per service, not one of the package-level caches shared by this test process.
	require.Equal(t, claudeCodeTestVersion(1), peer.GetClaudeCodeClientVersion(ctx))
	require.NoError(t, writer.UpdateSettings(ctx, &SystemSettings{ClaudeCodeClientVersion: claudeCodeTestVersion(2)}))
	require.Eventually(t, func() bool {
		return peer.GetClaudeCodeClientVersion(ctx) == claudeCodeTestVersion(2)
	}, time.Second, 5*time.Millisecond)

	server.Close()
	require.NoError(t, writer.UpdateSettings(ctx, &SystemSettings{ClaudeCodeClientVersion: claudeCodeTestVersion(3)}))
	stored, err := repo.GetAll(ctx)
	require.NoError(t, err)
	require.Equal(t, claudeCodeTestVersion(3), stored[SettingKeyClaudeCodeClientVersion])
}
