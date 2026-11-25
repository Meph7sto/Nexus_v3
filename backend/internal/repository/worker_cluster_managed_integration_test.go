package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/worker"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Requires a disposable loopback PostgreSQL database; test rows use temporary
// tables. NEXUS_TEST_PHASE5_REDIS optionally exercises a real Redis instance.
func TestManagedWorkerAdminPostgresCrossEntryForwarding(t *testing.T) {
	dsn := os.Getenv("NEXUS_TEST_PHASE5_PG")
	if dsn == "" {
		t.Skip("isolated loopback PostgreSQL fixture is required")
	}
	pg, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "postgres", pg.Scheme)
	require.Equal(t, "127.0.0.1", pg.Hostname())
	require.Equal(t, "/nexus_phase5_test", pg.Path)
	require.Empty(t, pg.Query().Get("host"))
	require.Empty(t, pg.Query().Get("hostaddr"))
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, ApplyMigrations(ctx, db))
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, `CREATE TEMP TABLE settings (LIKE public.settings INCLUDING ALL);
		CREATE TEMP TABLE worker_nodes (LIKE public.worker_nodes INCLUDING ALL);
		CREATE TEMP TABLE audit_logs (LIKE public.audit_logs INCLUDING ALL);
		ALTER TABLE pg_temp.worker_nodes DROP COLUMN encrypted_token;
		INSERT INTO pg_temp.worker_nodes (node_id,name,url,use_h2c,weight,max_inflight,enabled,secret_ref)
		VALUES ('legacy','Old Worker','http://127.0.0.1:8080',true,1,0,false,'legacy-ref')`)
	require.NoError(t, err)
	migration, err := migrations.FS.ReadFile("255_worker_nodes_managed_token.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(migration))
	require.NoError(t, err)
	var legacyRef, legacyCiphertext string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT secret_ref, encrypted_token FROM pg_temp.worker_nodes WHERE node_id='legacy'`).Scan(&legacyRef, &legacyCiphertext))
	require.Equal(t, "legacy-ref", legacyRef)
	require.Empty(t, legacyCiphertext)
	_, err = db.ExecContext(ctx, `DELETE FROM pg_temp.worker_nodes WHERE node_id='legacy'`)
	require.NoError(t, err)

	redisAddr := os.Getenv("NEXUS_TEST_PHASE5_REDIS")
	if redisAddr == "" {
		redisAddr = miniredis.RunT(t).Addr()
	}
	host, _, err := net.SplitHostPort(redisAddr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host)
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = redisClient.Close() })
	coord := NewWorkerClusterCoordination(redisClient)
	const oldToken = "managed-worker-integration-original-token"
	const newToken = "managed-worker-integration-replacement-token"
	var relayedAuth atomic.Value
	actualWorker, err := worker.New(worker.Config{NodeID: "managed", SharedToken: newToken,
		PreviousToken: oldToken, AllowPrivateH2C: true, AllowedTargets: []string{"api.openai.com"},
		Version: "must-not-echo-" + oldToken + ":" + newToken,
	}, testWorkerUpstream(func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: req.Body}, nil
	}))
	require.NoError(t, err)
	workerEndpoint := strings.Replace(testWorkerOrigin(t, "managed", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == workerproto.RelayPath {
			relayedAuth.Store(r.Header.Get("Authorization"))
		}
		actualWorker.ServeHTTP(w, r)
	})).URL, "127.0.0.1", "localhost", 1)
	startEntry := func(id, role string) (*service.WorkerClusterService, *DistributedHTTPUpstream, service.SecretEncryptor) {
		t.Helper()
		cfg := &config.Config{
			EntryNode:  config.EntryNodeConfig{ClusterEnabled: true, ID: id, Role: role},
			WorkerPool: config.WorkerPoolConfig{AllowPrivateH2C: true},
			Totp:       config.TotpConfig{EncryptionKey: strings.Repeat("cd", 32), EncryptionKeyConfigured: true},
		}
		transport, err := ProvideHTTPUpstream(cfg)
		require.NoError(t, err)
		pool, ok := transport.(*DistributedHTTPUpstream)
		require.True(t, ok, "a cluster without a local Worker pool still needs a reloadable transport")
		t.Cleanup(func() { _ = pool.Close() })
		encryptor, err := NewAESEncryptor(cfg)
		require.NoError(t, err)
		cluster, err := service.NewWorkerClusterService(NewWorkerNodeStore(db), coord, pool, cfg, nil, encryptor)
		require.NoError(t, err)
		require.NoError(t, cluster.Start(ctx))
		t.Cleanup(cluster.Close)
		return cluster, pool, encryptor
	}
	entryA, poolA, cipher := startEntry("managed-a", config.EntryRoleAll)
	_, poolB, _ := startEntry("managed-b", config.EntryRoleGatewayOnly)
	require.Eventually(t, func() bool {
		count, err := redisClient.PubSubNumSub(ctx, workerClusterReloadChannel).Result()
		return err == nil && count[workerClusterReloadChannel] == 2
	}, 5*time.Second, 20*time.Millisecond)
	awaitEntryB := func(version int64) {
		t.Helper()
		require.Eventually(t, func() bool {
			entries, err := coord.ListEntryHeartbeats(ctx)
			if err != nil {
				return false
			}
			for _, entry := range entries {
				if entry.NodeID == "managed-b" && entry.AppliedVersion == version && entry.ApplyError == "" {
					return true
				}
			}
			return false
		}, 5*time.Second, 20*time.Millisecond)
	}
	audit := service.NewAuditLogService(NewAuditLogRepository(db), nil)
	audit.Start()
	t.Cleanup(audit.Stop)
	router := gin.New()
	group := router.Group("/api/v1/admin/cluster", gin.HandlerFunc(middleware.NewAuditLogMiddleware(audit)))
	handler := admin.NewClusterHandler(entryA, nil)
	group.GET("", handler.View)
	group.POST("/workers", handler.SaveWorker)
	group.PUT("/workers/:id", handler.SaveWorker)
	group.DELETE("/workers/:id", handler.DeleteWorker)
	group.POST("/workers/:id/probe", handler.ProbeWorker)
	group.POST("/workers/:id/join", handler.JoinTemplate)
	group.PUT("/policy", handler.UpdatePolicy)
	var ciphertexts []string
	assertNoSecrets := func(raw string) {
		t.Helper()
		for _, secret := range append([]string{oldToken, newToken}, ciphertexts...) {
			require.NotContains(t, raw, secret)
		}
		require.NotContains(t, raw, `"shared_token"`)
		require.NotContains(t, raw, "encrypted_token")
	}
	call := func(method, path string, input any, status int) string {
		t.Helper()
		raw, err := json.Marshal(input)
		require.NoError(t, err)
		req := httptest.NewRequestWithContext(ctx, method, "/api/v1/admin/cluster"+path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		require.Equal(t, status, res.Code, res.Body.String())
		assertNoSecrets(res.Body.String())
		return res.Body.String()
	}
	readCiphertext := func() string {
		t.Helper()
		var stored, secretRef string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT encrypted_token, secret_ref FROM pg_temp.worker_nodes WHERE node_id='managed'`).Scan(&stored, &secretRef))
		require.Empty(t, secretRef, "no binding is required")
		require.NotEmpty(t, stored)
		require.NotContains(t, stored, oldToken)
		require.NotContains(t, stored, newToken)
		return stored
	}
	assertRouted := func(pool *DistributedHTTPUpstream, expectedToken string) {
		t.Helper()
		res, err := pool.Do(testRelayRequest(t, ctx, strings.NewReader("worker-relay")), testWorkerProxy, 9, 1)
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, "worker-relay", string(body))
		require.Equal(t, "Bearer "+expectedToken, relayedAuth.Load())
	}
	view, err := entryA.View(ctx)
	require.NoError(t, err)
	require.Empty(t, view.Bindings)
	require.Empty(t, view.Desired.Workers)
	policy := view.Desired.Policy
	policy.Enabled = true
	call(http.MethodPut, "/policy", gin.H{"expected_version": 1, "policy": policy}, http.StatusOK)
	node := gin.H{"node_id": "managed", "name": "Managed Worker", "url": workerEndpoint, "use_h2c": true,
		"weight": 1, "max_inflight": 2, "enabled": true, "shared_token": oldToken}
	write := func(method, path string, version int64, status int) string {
		return call(method, path, gin.H{"expected_version": version, "node": node}, status)
	}
	created := write(http.MethodPost, "/workers", 2, http.StatusOK)
	firstCiphertext := readCiphertext()
	ciphertexts = append(ciphertexts, firstCiphertext)
	assertNoSecrets(created)
	require.Contains(t, created, `"credential_source":"managed"`)
	require.Contains(t, created, `"has_token":true`)
	opened, err := cipher.Decrypt(firstCiphertext)
	require.NoError(t, err)
	require.Equal(t, oldToken, opened)
	awaitEntryB(3)
	assertRouted(poolA, oldToken)
	assertRouted(poolB, oldToken)
	listed := call(http.MethodGet, "", nil, http.StatusOK)
	require.Contains(t, listed, `"credential_source":"managed"`)
	join := call(http.MethodPost, "/workers/managed/join", nil, http.StatusOK)
	require.Contains(t, join, "WORKER_SECRETS_DIR/shared-token")
	call(http.MethodPost, "/workers/managed/probe", nil, http.StatusOK)

	node["name"], node["shared_token"] = "Renamed Worker", ""
	write(http.MethodPut, "/workers/managed", 3, http.StatusOK)
	require.Equal(t, firstCiphertext, readCiphertext())
	node["shared_token"] = newToken + ","
	write(http.MethodPut, "/workers/managed", 4, http.StatusBadRequest)
	node["shared_token"], node["url"] = newToken, "http://8.8.8.8:8000"
	write(http.MethodPut, "/workers/managed", 4, http.StatusBadRequest)
	node["url"] = workerEndpoint
	rotated := write(http.MethodPut, "/workers/managed", 4, http.StatusOK)
	replacementCiphertext := readCiphertext()
	ciphertexts = append(ciphertexts, replacementCiphertext)
	assertNoSecrets(rotated)
	require.NotEqual(t, firstCiphertext, replacementCiphertext)
	opened, err = cipher.Decrypt(replacementCiphertext)
	require.NoError(t, err)
	require.Equal(t, newToken, opened)
	awaitEntryB(5)
	assertRouted(poolA, newToken)
	assertRouted(poolB, newToken)
	call(http.MethodPost, "/workers/managed/probe", nil, http.StatusOK)
	node["enabled"], node["shared_token"] = false, ""
	write(http.MethodPut, "/workers/managed", 5, http.StatusOK)
	call(http.MethodDelete, "/workers/managed?expected_version=6", nil, http.StatusOK)
	awaitEntryB(7)
	require.Empty(t, poolB.current.Load().nodes)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pg_temp.worker_nodes`).Scan(&count))
	require.Zero(t, count)

	audit.Stop()
	rows, err := db.QueryContext(ctx, `SELECT request_body, to_jsonb(a)::text FROM pg_temp.audit_logs a WHERE method IN ('POST','PUT') AND path LIKE '%/workers%'`)
	require.NoError(t, err)
	defer rows.Close()
	audited := 0
	for rows.Next() {
		var requestBody, entry string
		require.NoError(t, rows.Scan(&requestBody, &entry))
		require.Equal(t, "<credential-bearing body omitted>", requestBody)
		assertNoSecrets(entry)
		audited++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 9, audited)
}
