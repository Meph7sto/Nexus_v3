package setup

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/alicebob/miniredis/v2"
	redisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSetupRedisSentinelPreflightAndPersistedTopology(t *testing.T) {
	t.Setenv("SKIP_SETUP", "false")
	t.Setenv("DATA_DIR", t.TempDir())
	data := miniredis.RunT(t)
	data.RequireAuth("test-data-password")
	sentinel := miniredis.RunT(t)
	sentinel.RequireAuth("test-sentinel-password")
	host, port, err := net.SplitHostPort(data.Addr())
	require.NoError(t, err)
	require.NoError(t, sentinel.Server().Register("SENTINEL", func(peer *redisserver.Peer, _ string, args []string) {
		if len(args) < 2 || args[1] != "nexus" {
			peer.WriteError("ERR unexpected Sentinel master")
			return
		}
		switch strings.ToLower(args[0]) {
		case "get-master-addr-by-name":
			peer.WriteLen(2)
			peer.WriteBulk(host)
			peer.WriteBulk(port)
		case "sentinels":
			peer.WriteLen(0)
		default:
			peer.WriteError("ERR unexpected Sentinel command")
		}
	}))
	req := RedisConfig{Password: "test-data-password", Sentinel: config.RedisSentinelConfig{
		Addresses: []string{sentinel.Addr()}, MasterName: "nexus", Password: "test-sentinel-password",
		AllowedPrimaries: []string{data.Addr()},
	}}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	router := gin.New()
	RegisterRoutes(router)
	request := httptest.NewRequest(http.MethodPost, "/setup/test-redis", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	reply := httptest.NewRecorder()
	router.ServeHTTP(reply, request)
	require.Equal(t, http.StatusOK, reply.Code, reply.Body.String())
	require.NotContains(t, reply.Body.String(), req.Password)
	require.NotContains(t, reply.Body.String(), req.Sentinel.Password)

	require.NoError(t, writeConfigFile(&SetupConfig{Redis: req, Timezone: "UTC"}))
	generated, err := os.ReadFile(GetConfigFilePath())
	require.NoError(t, err)
	var saved struct {
		Redis RedisConfig `yaml:"redis"`
	}
	require.NoError(t, yaml.Unmarshal(generated, &saved))
	require.Empty(t, saved.Redis.Host, "do not pin the observed primary in place of Sentinel")
	require.Zero(t, saved.Redis.Port)
	require.Equal(t, req.Sentinel, saved.Redis.Sentinel)
}
