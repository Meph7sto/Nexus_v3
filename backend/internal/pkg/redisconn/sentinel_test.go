package redisconn

import (
	"context"
	"net"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestSentinelDataDialRolesStaySeparateBeforeAuth(t *testing.T) {
	opts, _ := failoverOptions(config.RedisConfig{Sentinel: config.RedisSentinelConfig{
		Addresses: []string{"sentinel.example:26379"}, AllowedPrimaries: []string{"primary.example:6379"},
		MasterName: "nexus",
	}})
	ctx := context.Background()
	data, err := opts.Dialer(ctx, "tcp", "primary.example:6379")
	require.NoError(t, err)
	_, err = data.Write([]byte("AUTH sentinel-test-secret"))
	require.ErrorIs(t, err, errDataCannotBeSentinel)

	opened := false
	hook := dataDialHook{open: func(context.Context, string, string) (net.Conn, error) {
		opened = true
		return data, nil
	}}
	controlSocket, peer := net.Pipe()
	defer peer.Close()
	conn, err := hook.DialHook(func(context.Context, string, string) (net.Conn, error) {
		return controlSocket, nil
	})(ctx, "tcp", "FailoverClient")
	require.ErrorContains(t, err, "non-data endpoint")
	require.Nil(t, conn)
	buf := make([]byte, 1)
	n, err := peer.Read(buf)
	require.Zero(t, n)
	require.Error(t, err)
	require.False(t, opened)
	_, err = hook.DialHook(func(context.Context, string, string) (net.Conn, error) {
		return data, nil
	})(ctx, "tcp", "FailoverClient")
	require.NoError(t, err)
	require.True(t, opened)
}
