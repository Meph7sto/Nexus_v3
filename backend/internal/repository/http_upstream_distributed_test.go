package repository

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/worker"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
	"github.com/stretchr/testify/require"
)

const (
	testWorkerToken = "test-worker-token-authentication-0001"
	testWorkerProxy = "socks5h://proxy.test:1080"
)

type testLocalUpstream func(*http.Request, string) (*http.Response, error)

func (f testLocalUpstream) Do(req *http.Request, proxy string, _ int64, _ int) (*http.Response, error) {
	return f(req, proxy)
}

func (f testLocalUpstream) DoWithTLS(req *http.Request, proxy string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return f(req, proxy)
}

type testWorkerUpstream func(*http.Request, string, int64, int, upstream.RequestOptions) (*http.Response, error)

func (f testWorkerUpstream) Do(req *http.Request, proxy string, id int64, concurrency int, opts upstream.RequestOptions) (*http.Response, error) {
	return f(req, proxy, id, concurrency, opts)
}

func testWorkerOrigin(t *testing.T, id string, handler http.Handler) config.WorkerNodeConfig {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.Protocols = &http.Protocols{}
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	return config.WorkerNodeConfig{ID: id, URL: server.URL, SharedToken: testWorkerToken, Weight: 1}
}

func testMockWorker(t *testing.T, id string, handler http.HandlerFunc) config.WorkerNodeConfig {
	t.Helper()
	return testWorkerOrigin(t, id, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testWorkerToken || r.ProtoMajor != 2 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == workerproto.HealthPath && r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(workerproto.Health{Status: workerproto.HealthOK, NodeID: id, MaxInFlight: 10})
			return
		}
		handler(w, r)
	}))
}

func testPoolConfig(nodes ...config.WorkerNodeConfig) config.WorkerPoolConfig {
	return config.WorkerPoolConfig{
		Enabled: true, Nodes: nodes, DefaultProxyURL: testWorkerProxy,
		FallbackLocal: true, AllowPrivateH2C: true, HealthInterval: time.Hour,
	}
}

func testDistributedPool(t *testing.T, local service.HTTPUpstream, cfg config.WorkerPoolConfig) *DistributedHTTPUpstream {
	t.Helper()
	pool, err := NewDistributedHTTPUpstream(local, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	awaitWorkerHealth(t, pool.current.Load())
	return pool
}

func awaitWorkerHealth(t *testing.T, snapshot *workerSnapshot) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, node := range snapshot.nodes {
			if _, ok := node.load(); !ok {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
}

func testRelayRequest(t *testing.T, ctx context.Context, body io.Reader) *http.Request {
	t.Helper()
	ctx = service.WithDistributedHTTPUpstream(service.WithHTTPUpstreamProfile(ctx, service.HTTPUpstreamProfileOpenAI))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", body)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer test-upstream-key")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestDistributedHTTPUpstreamReloadWarmsBeforePublishing(t *testing.T) {
	old := testMockWorker(t, "old", nil)
	pool := testDistributedPool(t, testLocalUpstream(nil), testPoolConfig(old))
	release := make(chan struct{})
	probing := make(chan struct{})
	next := testWorkerOrigin(t, "next", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(probing)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(workerproto.Health{Status: workerproto.HealthOK, NodeID: "next", MaxInFlight: 10})
	}))
	done := make(chan error, 1)
	go func() { done <- pool.Reload(testPoolConfig(next)) }()
	select {
	case <-probing:
	case <-time.After(3 * time.Second):
		t.Fatal("new Worker not probed")
	}
	before := pool.current.Load().selectWorker(1, nil)
	close(release)
	require.NoError(t, <-done)
	require.NotNil(t, before)
	require.Equal(t, "old", before.id)
	after := pool.current.Load().selectWorker(1, nil)
	require.NotNil(t, after)
	require.Equal(t, "next", after.id)
}

func TestDistributedHTTPUpstreamTwoRealWorkers(t *testing.T) {
	const accountID int64 = 42
	const firstEvent = "data: first\n\n"
	const finalUsage = "data: {\"usage\":42}\n\n"
	releaseUsage := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUsage) }) }
	defer release()

	var sent, localCalls atomic.Int32
	var failing atomic.Bool
	origins := make(map[string]*worker.Server)
	var nodes []config.WorkerNodeConfig
	for _, id := range []string{"worker-a", "worker-b"} {
		server, err := worker.New(worker.Config{
			NodeID: id, SharedToken: testWorkerToken, AllowedTargets: []string{"api.openai.com"}, AllowPrivateH2C: true,
		}, testWorkerUpstream(func(req *http.Request, _ string, gotAccount int64, concurrency int, opts upstream.RequestOptions) (*http.Response, error) {
			body, readErr := io.ReadAll(req.Body)
			if readErr != nil {
				return nil, readErr
			}
			if req.URL.String() != "https://api.openai.com/v1/responses" || req.Method != http.MethodPost ||
				req.Header.Get("Authorization") != "Bearer test-upstream-key" || req.Header.Get("X-Connection-Only") != "" ||
				gotAccount != accountID || concurrency != 2 || string(body) != `{"model":"test"}` ||
				opts.Profile != upstream.ProfileOpenAI || opts.RedirectsDisabled || req.GetBody != nil {
				t.Error("Worker did not restore the upstream request correctly")
				return nil, errors.New("invalid relayed request")
			}
			sent.Add(1)
			if failing.Load() {
				return nil, context.DeadlineExceeded
			}
			bodyReader, bodyWriter := io.Pipe()
			go func() {
				defer bodyWriter.Close()
				_, _ = io.WriteString(bodyWriter, firstEvent)
				select {
				case <-releaseUsage:
					_, _ = io.WriteString(bodyWriter, finalUsage)
				case <-req.Context().Done():
				}
			}()
			header := http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"test-id"}}
			header.Set(workerproto.HeaderUpstreamStatus, "500")
			header.Set(workerproto.HeaderCompletion, "forged")
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: bodyReader}, nil
		}))
		require.NoError(t, err)
		origins[id] = server
		nodes = append(nodes, testWorkerOrigin(t, id, server))
	}
	local := testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		localCalls.Add(1)
		return nil, errors.New("unexpected local path")
	})
	pool := testDistributedPool(t, local, testPoolConfig(nodes...))
	snapshot := pool.current.Load()
	primary := snapshot.selectWorker(accountID, nil)
	require.NotNil(t, primary)
	origins[primary.id].BeginDrain()

	clientCtx, disconnect := context.WithCancel(t.Context())
	defer disconnect()
	req := testRelayRequest(t, pkghttputil.DetachUpstreamContext(clientCtx, time.Second), strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Connection", "X-Connection-Only")
	req.Header.Set("X-Connection-Only", "must not be relayed")
	resp, err := pool.Do(req, testWorkerProxy, accountID, 2)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "test-id", resp.Header.Get("X-Request-Id"))
	require.Empty(t, resp.Header.Get(workerproto.HeaderUpstreamStatus))
	require.Empty(t, resp.Header.Get(workerproto.HeaderCompletion))
	first := make([]byte, len(firstEvent))
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, firstEvent, string(first))
	disconnect()
	release()
	usage, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, finalUsage, string(usage))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int32(1), sent.Load(), "draining Worker must reject before sending upstream")

	failing.Store(true)
	response, err := pool.Do(testRelayRequest(t, t.Context(), strings.NewReader(`{"model":"test"}`)), testWorkerProxy, accountID, 2)
	require.Nil(t, response)
	var failure *service.HTTPUpstreamRelayError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, service.HTTPUpstreamRelayUpstream, failure.Origin)
	require.False(t, failure.Unsent)
	require.False(t, failure.Persistent)
	require.Equal(t, int32(2), sent.Load(), "upstream header timeout must not replay")
	require.Zero(t, localCalls.Load())
	for _, node := range snapshot.nodes {
		require.Zero(t, node.localLoad.Load())
	}
}

func TestDistributedHTTPUpstreamPreservesCompleteUpstreamErrors(t *testing.T) {
	var forwarded atomic.Int32
	var localCalls atomic.Int32
	nodes := make([]config.WorkerNodeConfig, 0, 2)
	for _, id := range []string{"error-a", "error-b"} {
		server, err := worker.New(worker.Config{
			NodeID: id, SharedToken: testWorkerToken, AllowedTargets: []string{"api.openai.com"}, AllowPrivateH2C: true,
		}, testWorkerUpstream(func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
			forwarded.Add(1)
			requestBody, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			status, err := strconv.Atoi(string(requestBody))
			if err != nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: status,
				Header: http.Header{
					"X-Request-Id": {id + "-" + string(requestBody)},
					"Retry-After":  {"3"},
					"Content-Type": {"application/json"},
				},
				Body: io.NopCloser(strings.NewReader(`{"status":` + string(requestBody) + "}")),
			}, nil
		}))
		require.NoError(t, err)
		nodes = append(nodes, testWorkerOrigin(t, id, server))
	}
	pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		localCalls.Add(1)
		return nil, errors.New("unexpected fallback for an upstream response")
	}), testPoolConfig(nodes...))
	const account int64 = 17
	snapshot := pool.current.Load()
	preferred := snapshot.selectWorker(account, nil)
	require.NotNil(t, preferred)

	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError, 529} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			before := forwarded.Load()
			decimal := strconv.Itoa(status)
			resp, err := pool.Do(testRelayRequest(t, t.Context(), strings.NewReader(decimal)), testWorkerProxy, account, 1)
			require.NoError(t, err)
			require.Equal(t, status, resp.StatusCode)
			require.Equal(t, preferred.id+"-"+decimal, resp.Header.Get("X-Request-Id"))
			require.Equal(t, "3", resp.Header.Get("Retry-After"))
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "the complete error response must carry its relay completion marker")
			require.NoError(t, resp.Body.Close())
			require.JSONEq(t, `{"status":`+decimal+"}", string(body))
			require.Equal(t, before+1, forwarded.Load())
			require.Zero(t, localCalls.Load())
			preferred.mu.Lock()
			failures := preferred.failures
			preferred.mu.Unlock()
			require.Zero(t, failures)
			require.Same(t, preferred, snapshot.selectWorker(account, nil))
		})
	}
}

type testLocalTLSUpstream struct {
	testLocalUpstream
	profile *tlsfingerprint.Profile
	proxy   string
}

func (l *testLocalTLSUpstream) DoWithTLS(_ *http.Request, proxy string, _ int64, _ int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	l.profile, l.proxy = profile, proxy
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
}

func TestDistributedHTTPUpstreamLocalOptOutPreservesTLS(t *testing.T) {
	var remoteCalls atomic.Int32
	node := testMockWorker(t, "worker", func(w http.ResponseWriter, _ *http.Request) {
		remoteCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	local := &testLocalTLSUpstream{testLocalUpstream: func(*http.Request, string) (*http.Response, error) {
		return nil, errors.New("TLS fallback dropped the profile")
	}}
	pool := testDistributedPool(t, local, testPoolConfig(node))
	profile := &tlsfingerprint.Profile{Name: "existing-local-profile"}
	resp, err := pool.DoWithTLS(testRelayRequest(t, service.WithLocalHTTPUpstreamOnly(t.Context()), strings.NewReader("{}")), testWorkerProxy, 12, 1, profile)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Same(t, profile, local.profile)
	require.Equal(t, testWorkerProxy, local.proxy)
	require.Zero(t, remoteCalls.Load())
}

func TestDistributedHTTPUpstreamRejectsUnprovenOutcomes(t *testing.T) {
	for _, name := range []string{"missing trailer", "duplicate status", "contradictory error headers", "duplicate error code", "redirect", "link drops before headers", "persistent upstream error"} {
		t.Run(name, func(t *testing.T) {
			var workerCalls, localCalls atomic.Int32
			node := testMockWorker(t, "worker", func(w http.ResponseWriter, _ *http.Request) {
				workerCalls.Add(1)
				switch name {
				case "missing trailer":
					w.Header().Set(workerproto.HeaderUpstreamStatus, "200")
					_, _ = io.WriteString(w, "truncated")
				case "duplicate status":
					w.Header().Add(workerproto.HeaderUpstreamStatus, "200")
					w.Header().Add(workerproto.HeaderUpstreamStatus, "503")
					w.WriteHeader(http.StatusOK)
				case "contradictory error headers":
					w.Header().Set(workerproto.HeaderUpstreamStatus, "200")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"code":"worker_draining","outcome":"unsent"}`)
				case "duplicate error code":
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"code":"upstream_error","code":"worker_draining","outcome":"unsent"}`)
				case "redirect":
					w.Header().Set("Location", "http://127.0.0.1:1/")
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "link drops before headers":
					panic(http.ErrAbortHandler)
				case "persistent upstream error":
					w.WriteHeader(http.StatusBadGateway)
					_ = json.NewEncoder(w).Encode(workerproto.RelayError{Code: workerproto.CodeUpstreamPersistent, Outcome: workerproto.OutcomeUnknown})
				}
			})
			pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
				localCalls.Add(1)
				return nil, errors.New("unexpected fallback")
			}), testPoolConfig(node))
			resp, err := pool.Do(testRelayRequest(t, t.Context(), http.NoBody), testWorkerProxy, 1, 1)
			if name == "missing trailer" {
				require.NoError(t, err)
				_, err = io.ReadAll(resp.Body)
				require.NoError(t, resp.Body.Close())
				require.ErrorIs(t, err, errIncompleteRelay)
			} else {
				require.Nil(t, resp)
			}
			var failure *service.HTTPUpstreamRelayError
			require.ErrorAs(t, err, &failure)
			require.False(t, failure.Unsent)
			if name == "persistent upstream error" {
				require.Equal(t, service.HTTPUpstreamRelayUpstream, failure.Origin)
				require.True(t, failure.Persistent)
			} else {
				require.Equal(t, service.HTTPUpstreamRelayLink, failure.Origin)
			}
			require.Equal(t, int32(1), workerCalls.Load())
			require.Zero(t, localCalls.Load())
		})
	}
}

func TestDistributedHTTPUpstreamFallbackAfterVerifiedDialFailure(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != workerproto.HealthPath || r.Header.Get("Authorization") != "Bearer "+testWorkerToken {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(workerproto.Health{Status: workerproto.HealthOK, NodeID: "worker", MaxInFlight: 1})
	}))
	server.Config.Protocols = &http.Protocols{}
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	node := config.WorkerNodeConfig{ID: "worker", URL: server.URL, SharedToken: testWorkerToken}
	var localCalls atomic.Int32
	pool := testDistributedPool(t, testLocalUpstream(func(_ *http.Request, proxy string) (*http.Response, error) {
		require.Equal(t, testWorkerProxy, proxy)
		localCalls.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}), testPoolConfig(node))
	pool.current.Load().nodes[0].client.CloseIdleConnections()
	server.Close()

	resp, err := pool.Do(testRelayRequest(t, t.Context(), strings.NewReader("payload")), testWorkerProxy, 1, 1)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int32(1), localCalls.Load(), "only the same SOCKS route may be attempted after the Worker could not be reached")
}

func TestDistributedHTTPUpstreamFallbackUsesSameProxyOnly(t *testing.T) {
	var attempted, localCalls atomic.Int32
	node := testMockWorker(t, "worker", func(w http.ResponseWriter, _ *http.Request) {
		attempted.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(workerproto.RelayError{Code: workerproto.CodeWorkerOverloaded, Outcome: workerproto.OutcomeUnsent})
	})
	localErr := errors.New("local outgoing attempt failed")
	pool := testDistributedPool(t, testLocalUpstream(func(req *http.Request, proxy string) (*http.Response, error) {
		localCalls.Add(1)
		require.Equal(t, "socks5://proxy.test", proxy)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Equal(t, "payload", string(body))
		return nil, localErr
	}), testPoolConfig(node))
	resp, err := pool.Do(testRelayRequest(t, t.Context(), strings.NewReader("payload")), "socks5://proxy.test", 1, 1)
	require.Nil(t, resp)
	require.ErrorIs(t, err, localErr)
	var failure *service.HTTPUpstreamRelayError
	require.ErrorAs(t, err, &failure)
	require.True(t, failure.LocalFallback)
	require.False(t, failure.Unsent)
	require.Equal(t, int32(1), localCalls.Load())

	nonReplayable := io.NopCloser(strings.NewReader("payload"))
	_, err = pool.Do(testRelayRequest(t, t.Context(), nonReplayable), testWorkerProxy, 1, 1)
	require.ErrorAs(t, err, &failure)
	require.True(t, failure.Unsent)
	require.Equal(t, int32(2), attempted.Load())
	require.Equal(t, int32(1), localCalls.Load())
}

func TestDistributedHTTPUpstreamNoRetryAfterDetachedClientCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	node := testMockWorker(t, "worker", func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(workerproto.RelayError{Code: workerproto.CodeWorkerOverloaded, Outcome: workerproto.OutcomeUnsent})
	})
	var localCalls atomic.Int32
	pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		localCalls.Add(1)
		return nil, errors.New("unexpected fallback")
	}), testPoolConfig(node))
	req := testRelayRequest(t, pkghttputil.DetachUpstreamContext(ctx, time.Second), strings.NewReader("payload"))
	resp, err := pool.Do(req, testWorkerProxy, 1, 1)
	require.Nil(t, resp)
	var failure *service.HTTPUpstreamRelayError
	require.ErrorAs(t, err, &failure)
	require.True(t, failure.Unsent)
	require.Zero(t, localCalls.Load())
}

func TestDistributedHTTPUpstreamReloadPreservesActiveStream(t *testing.T) {
	releaseTail := make(chan struct{})
	var released sync.Once
	release := func() { released.Do(func() { close(releaseTail) }) }
	defer release()
	var oldCalls atomic.Int32
	old := testMockWorker(t, "old", func(w http.ResponseWriter, r *http.Request) {
		oldCalls.Add(1)
		w.Header().Set(workerproto.HeaderUpstreamStatus, "200")
		w.Header().Set("Trailer", workerproto.HeaderCompletion)
		_, _ = io.WriteString(w, "old")
		_ = http.NewResponseController(w).Flush()
		select {
		case <-releaseTail:
			_, _ = io.WriteString(w, "-tail")
			w.Header().Set(workerproto.HeaderCompletion, workerproto.CompletionComplete)
		case <-r.Context().Done():
		}
	})
	next := testMockWorker(t, "next", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(workerproto.HeaderUpstreamStatus, "200")
		w.Header().Set("Trailer", workerproto.HeaderCompletion)
		_, _ = io.WriteString(w, "new")
		w.Header().Set(workerproto.HeaderCompletion, workerproto.CompletionComplete)
	})
	pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		return nil, errors.New("unexpected fallback")
	}), testPoolConfig(old))
	oldSnapshot := pool.current.Load()
	resp, err := pool.Do(testRelayRequest(t, t.Context(), http.NoBody), testWorkerProxy, 1, 1)
	require.NoError(t, err)
	defer resp.Body.Close()
	first := make([]byte, len("old"))
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, "old", string(first))
	require.Equal(t, int64(1), oldSnapshot.nodes[0].localLoad.Load())

	invalid := testPoolConfig(next)
	invalid.Nodes[0].URL = "http://8.8.8.8/"
	require.Error(t, pool.Reload(invalid))
	require.Same(t, oldSnapshot, pool.current.Load())
	require.NoError(t, pool.Reload(testPoolConfig(next)))
	awaitWorkerHealth(t, pool.current.Load())
	require.True(t, oldSnapshot.retired.Load())

	fresh, err := pool.Do(testRelayRequest(t, t.Context(), http.NoBody), testWorkerProxy, 1, 1)
	require.NoError(t, err)
	body, err := io.ReadAll(fresh.Body)
	require.NoError(t, err)
	require.Equal(t, "new", string(body))
	require.NoError(t, fresh.Body.Close())

	stale, staleErr, retry := oldSnapshot.relay(oldSnapshot.nodes[0], testRelayRequest(t, t.Context(), http.NoBody), 1, 1, "", "", "", "")
	require.Nil(t, stale)
	require.True(t, staleErr.Unsent)
	require.False(t, retry)
	require.Equal(t, int32(1), oldCalls.Load(), "the retired snapshot must not start another request")

	release()
	tail, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "-tail", string(tail))
	require.NoError(t, resp.Body.Close())
	require.Zero(t, oldSnapshot.nodes[0].localLoad.Load())
}

func TestDistributedHTTPUpstreamCircuitOnlyRecoversAfterProbe(t *testing.T) {
	var calls atomic.Int32
	node := testMockWorker(t, "worker", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	cfg := testPoolConfig(node)
	cfg.FallbackLocal = false
	pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		return nil, errors.New("unexpected fallback")
	}), cfg)
	worker := pool.current.Load().nodes[0]
	for range workerFailureThreshold {
		_, err := pool.Do(testRelayRequest(t, t.Context(), http.NoBody), testWorkerProxy, 1, 1)
		require.ErrorIs(t, err, errInvalidRelayReply)
	}
	_, eligible := worker.load()
	require.False(t, eligible)
	worker.probe(t.Context(), time.Second)
	_, eligible = worker.load()
	require.False(t, eligible)
	_, err := pool.Do(testRelayRequest(t, t.Context(), http.NoBody), testWorkerProxy, 1, 1)
	require.ErrorIs(t, err, errNoHealthyWorker)
	require.Equal(t, int32(workerFailureThreshold), calls.Load())

	worker.mu.Lock()
	worker.openUntil = time.Now().Add(-time.Millisecond)
	worker.mu.Unlock()
	_, eligible = worker.load()
	require.False(t, eligible)
	worker.probe(t.Context(), time.Second)
	_, eligible = worker.load()
	require.True(t, eligible)
}

func TestWorkerPolicySelectionUsesWeightsAndCapacity(t *testing.T) {
	a := &relayWorker{id: "a", weight: 1, healthy: true, remoteLoad: 3}
	b := &relayWorker{id: "b", weight: 3, healthy: true, remoteLoad: 6}
	pool := &workerSnapshot{nodes: []*relayWorker{a, b}, selection: "least_inflight"}
	require.Same(t, b, pool.selectWorker(1, nil), "less effective load after weighting")
	b.capacity = 6
	require.Same(t, a, pool.selectWorker(1, nil), "a saturated Worker is skipped")
	b.capacity = 7
	pool.selection = "round_robin"
	require.Same(t, a, pool.selectWorker(1, nil))
	require.Same(t, b, pool.selectWorker(1, nil))
	require.Same(t, b, pool.selectWorker(1, map[*relayWorker]bool{a: true}), "an attempt never retries the same Worker")
}
