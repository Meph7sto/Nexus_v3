package repository

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/worker"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

// Isolate the test trust store and resolver from the process running the rest
// of the repository suite. Neither real credentials nor public egress are used.
func TestPhase4RealWorkerRoutes(t *testing.T) {
	if os.Getenv("NEXUS_PHASE4_FIXTURE") != "" {
		runPhase4Routes(t)
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPhase4RealWorkerRoutes$", "-test.timeout=45s", "-test.v")
	cmd.Env = append(os.Environ(), "NEXUS_PHASE4_FIXTURE=1", "GODEBUG=x509usefallbackroots=1,netdns=go")
	data, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", data)
}

func phase4Resolver() *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			var n uint16
			if binary.Read(server, binary.BigEndian, &n) != nil {
				return
			}
			data := make([]byte, n)
			if _, err := io.ReadFull(server, data); err != nil {
				return
			}
			var query dnsmessage.Message
			if query.Unpack(data) != nil || len(query.Questions) != 1 {
				return
			}
			q := query.Questions[0]
			msg := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, RecursionAvailable: true}, Questions: query.Questions}
			if q.Name.String() == "proxy-only.example." {
				msg.Header.RCode = dnsmessage.RCodeNameError
			} else if q.Type == dnsmessage.TypeA {
				msg.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: q.Name, Type: q.Type, Class: q.Class, TTL: 5}, Body: &dnsmessage.AResource{A: [4]byte{1, 1, 1, 1}}}}
			}
			b, _ := msg.Pack()
			_ = binary.Write(server, binary.BigEndian, uint16(len(b)))
			_, _ = server.Write(b)
		}()
		return client, nil
	}}
}

func phase4Cert(t *testing.T, hosts []string) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(94), DNSNames: hosts, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, key.Public(), key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	x509.SetFallbackRoots(roots)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func phase4Bridge(client net.Conn, target string) {
	defer client.Close()
	c, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	go func() { _, _ = io.Copy(c, client); _ = c.Close() }()
	_, _ = io.Copy(client, c)
}

func phase4HTTPProxy(t *testing.T, target string, cert *tls.Certificate) (string, *atomic.Int32, *atomic.Value) {
	var calls atomic.Int32
	var password atomic.Value
	password.Store("credential")
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check := &http.Request{Header: http.Header{"Authorization": r.Header.Values("Proxy-Authorization")}}
		u, p, ok := check.BasicAuth()
		if r.Method != "CONNECT" || r.Host != "1.1.1.1:443" || !ok || u != "test" || p != password.Load().(string) {
			http.Error(w, "denied", 403)
			return
		}
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		calls.Add(1)
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\n\r\n")
		_ = buf.Flush()
		phase4Bridge(conn, target)
	}))
	if cert != nil {
		server.TLS = &tls.Config{Certificates: []tls.Certificate{*cert}}
		server.StartTLS()
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)
	p, _ := url.Parse(server.URL)
	p.User = url.UserPassword("test", "credential")
	return p.String(), &calls, &password
}

func phase4SOCKS(t *testing.T, target, user string, remoteName ...string) (string, *atomic.Int32) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	var calls atomic.Int32
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				read := func(n int) []byte { b := make([]byte, n); _, _ = io.ReadFull(c, b); return b }
				if read(1)[0] != 5 {
					return
				}
				read(int(read(1)[0]))
				_, _ = c.Write([]byte{5, 2})
				if read(1)[0] != 1 {
					return
				}
				name := string(read(int(read(1)[0])))
				pw := string(read(int(read(1)[0])))
				if name != user || pw != "testpw" {
					return
				}
				_, _ = c.Write([]byte{1, 0})
				if len(remoteName) == 0 {
					if !bytes.Equal(read(4), []byte{5, 1, 0, 1}) || !bytes.Equal(read(6), []byte{1, 1, 1, 1, 1, 187}) {
						return
					}
				} else {
					if !bytes.Equal(read(4), []byte{5, 1, 0, 3}) || string(read(int(read(1)[0]))) != remoteName[0] || !bytes.Equal(read(2), []byte{1, 187}) {
						return
					}
				}
				calls.Add(1)
				_, _ = c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				phase4Bridge(c, target)
			}()
		}
	}()
	return user + ":testpw@" + l.Addr().String(), &calls
}

func assertPhase4StoredProxyRouting(t *testing.T, pool *DistributedHTTPUpstream, first, backup string, firstCalls, backupCalls *atomic.Int32, password *atomic.Value) {
	t.Helper()
	ctx := t.Context()
	db, client := newAPIKeyRepoSQLite(t)
	proxies := &proxyRepository{client: client, sql: db.sql}
	accounts := &accountRepository{client: client, sql: db.sql}
	create := func(name, raw string) *service.Proxy {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		pass, _ := u.User.Password()
		port, err := strconv.Atoi(u.Port())
		require.NoError(t, err)
		p := &service.Proxy{Name: name, Protocol: u.Scheme, Host: u.Hostname(), Port: port, Username: u.User.Username(), Password: pass, Status: service.StatusActive, FallbackMode: service.FallbackModeNone}
		require.NoError(t, proxies.Create(ctx, p))
		return p
	}
	firstProxy := create("first", first)
	backupProxy := create("backup", backup)
	a, err := client.Account.Create().SetName("gateway").SetPlatform(service.PlatformOpenAI).SetType(service.AccountTypeOAuth).SetProxyID(firstProxy.ID).Save(ctx)
	require.NoError(t, err)
	forward := func(expected string) {
		t.Helper()
		fresh, err := accounts.GetForWorkerRequest(ctx, a.ID)
		require.NoError(t, err)
		require.NotNil(t, fresh.Proxy)
		require.Equal(t, expected, fresh.Proxy.URL())
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://chatgpt.com/v1/test?a=1&a=2", nil)
		require.NoError(t, err)
		r.Header.Set("Authorization", "Bearer test-upstream")
		resp, err := pool.Do(r, fresh.Proxy.URL(), a.ID, 1)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		require.Contains(t, string(body), "[DONE]")
	}
	forward(first)
	beforeRotation := firstCalls.Load()
	password.Store("rotated-fixture")
	require.NoError(t, client.Proxy.UpdateOneID(firstProxy.ID).SetPassword("rotated-fixture").Exec(ctx))
	rotated, err := url.Parse(first)
	require.NoError(t, err)
	rotated.User = url.UserPassword(rotated.User.Username(), "rotated-fixture")
	forward(rotated.String())
	require.Greater(t, firstCalls.Load(), beforeRotation)
	password.Store("credential")
	oldCalls := backupCalls.Load()
	require.NoError(t, client.Account.UpdateOneID(a.ID).SetProxyID(backupProxy.ID).Exec(ctx))
	forward(backup)
	require.Greater(t, backupCalls.Load(), oldCalls)
	require.NoError(t, client.Account.UpdateOneID(a.ID).SetProxyID(firstProxy.ID).Exec(ctx))
	expired := time.Now().Add(-time.Minute)
	require.NoError(t, client.Proxy.UpdateOneID(firstProxy.ID).SetExpiresAt(expired).SetFallbackMode(service.FallbackModeProxy).SetBackupProxyID(backupProxy.ID).Exec(ctx))
	forward(backup)
	require.NoError(t, client.Proxy.UpdateOneID(firstProxy.ID).SetFallbackMode(service.FallbackModeDirect).ClearBackupProxyID().Exec(ctx))
	fresh, err := accounts.GetForWorkerRequest(ctx, a.ID)
	require.NoError(t, err)
	require.Nil(t, fresh.Proxy)
	require.Nil(t, fresh.ProxyID)
}

func runPhase4Routes(t *testing.T) {
	net.DefaultResolver = phase4Resolver()
	hosts := []string{"chatgpt.com", "api.anthropic.com", "generativelanguage.googleapis.com", "cloudcode-pa.googleapis.com", grokCLIProxyHost, grokOfficialAPIHost, "custom.example", "proxy-only.example"}
	cert := phase4Cert(t, hosts)
	var hellos, fallback atomic.Int32
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(workerproto.HeaderProxy) != "" || r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("Authorization") != "Bearer test-upstream" {
			http.Error(w, "bad headers", 400)
			return
		}
		if r.URL.Path == "/socket" {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			for {
				typ, b, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				if conn.Write(r.Context(), typ, b) != nil {
					return
				}
			}
		}
		if r.Host == grokCLIProxyHost {
			if r.Header.Get("X-XAI-Token-Auth") == "" {
				http.Error(w, "missing identity", 400)
				return
			}
			http.Error(w, "Access denied", 403)
			return
		}
		if r.Host == grokOfficialAPIHost {
			fallback.Add(1)
		}
		if r.URL.RawQuery != "a=1&a=2" {
			http.Error(w, "missing query", 400)
			return
		}
		w.Header().Set("X-Method", r.Method)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: ok\n\n")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	origin.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		if len(hello.CipherSuites) == 2 && hello.CipherSuites[0] == tls.TLS_AES_256_GCM_SHA384 {
			hellos.Add(1)
		}
		return nil, nil
	}}
	origin.StartTLS()
	defer origin.Close()
	p1, c1, password := phase4HTTPProxy(t, origin.Listener.Addr().String(), nil)
	p2, c2, _ := phase4HTTPProxy(t, origin.Listener.Addr().String(), &cert)
	s1, c3 := phase4SOCKS(t, origin.Listener.Addr().String(), "a")
	s2, c4 := phase4SOCKS(t, origin.Listener.Addr().String(), "b")
	cfg := worker.Config{NodeID: "worker", SharedToken: testWorkerToken, AllowPrivateH2C: true}
	zero, err := worker.New(cfg, nil)
	require.NoError(t, err)
	node := testWorkerOrigin(t, "worker", zero)
	pc := testPoolConfig(node)
	pc.DefaultProxyURL = ""
	pc.FallbackLocal = false
	pool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
		t.Error("unexpected local request")
		return nil, io.ErrUnexpectedEOF
	}), pc)
	profile := &tlsfingerprint.Profile{CipherSuites: []uint16{tls.TLS_AES_256_GCM_SHA384, tls.TLS_AES_128_GCM_SHA256}}
	for i, row := range []struct {
		host, method, proxy string
		counter             *atomic.Int32
		tlsProfile          *tlsfingerprint.Profile
		profile             service.HTTPUpstreamProfile
	}{
		{hosts[0], "POST", p1, c1, profile, service.HTTPUpstreamProfileOpenAI},
		{hosts[1], "PUT", p2, c2, profile, ""},
		{hosts[2], "PATCH", "socks5://" + s1, c3, nil, service.HTTPUpstreamProfileLongStream},
		{hosts[3], "DELETE", "socks5h://" + s2, c4, nil, ""},
		{hosts[4], "POST", p1, c1, nil, service.HTTPUpstreamProfileGrok},
		{hosts[6], "HEAD", p1, c1, nil, service.HTTPUpstreamProfileOpenAIHarvest},
	} {
		ctx := service.WithHTTPUpstreamProfile(t.Context(), row.profile)
		req, err := http.NewRequestWithContext(ctx, row.method, "https://"+row.host+"/v1/test?a=1&a=2", strings.NewReader("test"))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer test-upstream")
		resp, err := pool.DoWithTLS(req, row.proxy, 42, 1, row.tlsProfile)
		require.NoError(t, err, "row %d", i)
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, "row %d: %s", i, data)
		require.Equal(t, row.method, resp.Header.Get("X-Method"))
		require.Positive(t, row.counter.Load())
		if row.method != "HEAD" {
			require.Contains(t, string(data), "[DONE]")
		}
	}
	require.Equal(t, int32(2), hellos.Load())
	require.Equal(t, int32(1), fallback.Load())
	assertPhase4StoredProxyRouting(t, pool, p1, "socks5h://"+s2, c1, c4, password)
	// Private self-hosted origins require an explicit authority. A public-only
	// request cannot reuse the exception, even if a socket is already pooled.
	req, _ := http.NewRequestWithContext(t.Context(), "GET", origin.URL+"/v1/test?a=1&a=2", nil)
	req.Header.Set("Authorization", "Bearer test-upstream")
	_, err = pool.Do(req, "", 42, 1)
	require.Error(t, err)
	cfg.AllowedTargets = append(append([]string(nil), hosts...), origin.Listener.Addr().String())
	allow, err := worker.New(cfg, nil)
	require.NoError(t, err)
	pc.Nodes[0] = testWorkerOrigin(t, "worker", allow)
	require.NoError(t, pool.Reload(pc))
	resp, err := pool.Do(req, "", 42, 1)
	require.NoError(t, err)
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	_, err = pool.Do(req.WithContext(service.WithHTTPUpstreamPublicHostsOnly(t.Context())), "", 42, 1)
	require.Error(t, err)
	ws := service.NewDistributedOpenAIWSDialer(pool)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, _, _, err := ws.Dial(ctx, "wss://chatgpt.com/socket", http.Header{"Authorization": {"Bearer test-upstream"}}, p1)
	require.NoError(t, err)
	defer conn.Close()
	load := pool.current.Load().nodes[0]
	require.Equal(t, int64(1), load.localLoad.Load())
	pong := make(chan error, 1)
	go func() { pong <- conn.Ping(ctx) }()
	echo := make(chan error, 1)
	go func() {
		_, err := conn.ReadMessage(ctx)
		echo <- err
	}()
	require.NoError(t, <-pong)
	require.NoError(t, conn.WriteJSON(ctx, "after-ping"))
	require.NoError(t, <-echo)
	allow.BeginDrain()
	_, _, _, err = ws.Dial(ctx, "wss://chatgpt.com/socket", http.Header{"Authorization": {"Bearer test-upstream"}}, p1)
	require.True(t, service.IsHTTPUpstreamRelayTerminal(err), "draining must reject a new socket")
	pc.Enabled = false
	require.NoError(t, pool.Reload(pc))
	require.NoError(t, conn.WriteJSON(ctx, "hello"))
	b, err := conn.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, "\"hello\"", strings.TrimSpace(string(b)))
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return load.localLoad.Load() == 0 }, time.Second, 10*time.Millisecond)
	privateProxy := false
	cfg.AllowPrivateProxy = &privateProxy
	deny, err := worker.New(cfg, nil)
	require.NoError(t, err)
	pc.Enabled = true
	pc.Nodes[0] = testWorkerOrigin(t, "worker", deny)
	require.NoError(t, pool.Reload(pc))
	before := c1.Load()
	r, err := http.NewRequestWithContext(t.Context(), "GET", "https://chatgpt.com/v1/test?a=1&a=2", nil)
	require.NoError(t, err)
	_, err = pool.Do(r, p1, 42, 1)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "credential")
	require.Equal(t, before, c1.Load())
	t.Run("explicit trusted DNS with no static proxy", func(t *testing.T) {
		const host = "proxy-only.example"
		raw, calls := phase4SOCKS(t, origin.Listener.Addr().String(), "trusted", host)
		trustedCfg := worker.Config{SharedToken: testWorkerToken, NodeID: "trusted", AllowPrivateH2C: true, DNSMode: "trusted_proxy", AllowedTargets: []string{host}}
		trusted, err := worker.New(trustedCfg, nil)
		require.NoError(t, err)
		trustedPoolConfig := testPoolConfig(testWorkerOrigin(t, "trusted", trusted))
		trustedPoolConfig.FallbackLocal = false
		trustedPool := testDistributedPool(t, testLocalUpstream(func(*http.Request, string) (*http.Response, error) {
			return nil, io.ErrUnexpectedEOF
		}), trustedPoolConfig)
		req, err := http.NewRequestWithContext(t.Context(), "GET", "https://"+host+"/v1/test?a=1&a=2", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer test-upstream")
		resp, err := trustedPool.Do(req, "socks5h://"+raw, 42, 1)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		_, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		require.EqualValues(t, 1, calls.Load())

		// Neither a public-only request nor socks5 may bypass local resolution.
		_, err = trustedPool.Do(req.WithContext(service.WithHTTPUpstreamPublicHostsOnly(t.Context())), "socks5h://"+raw, 42, 1)
		require.Error(t, err)
		_, err = trustedPool.Do(req, "socks5://"+raw, 42, 1)
		require.Error(t, err)
		require.EqualValues(t, 1, calls.Load())

		ctx, cancel := context.WithTimeout(service.WithWorkerWebSocketAccount(t.Context(), 42), 5*time.Second)
		defer cancel()
		ws := service.NewDistributedOpenAIWSDialer(trustedPool)
		conn, _, _, err := ws.Dial(ctx, "wss://"+host+"/socket", req.Header, "socks5h://"+raw)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		require.EqualValues(t, 2, calls.Load())
	})
}
