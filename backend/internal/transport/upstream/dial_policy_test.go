package upstream

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlledDialPinsActualSOCKS5Destination(t *testing.T) {
	var seenHost atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		seenHost.Store(req.Host)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(server.Close)
	proxy, connections := startTestSOCKS5Proxy(t)
	serverAddr := server.Listener.Addr().String()
	_, port, err := net.SplitHostPort(serverAddr)
	require.NoError(t, err)
	original := "never-resolve.invalid:" + port
	client := NewClient(&Config{DialAddresses: func(_ context.Context, _ string, addr string) ([]string, error) {
		if addr != original {
			t.Errorf("dial policy got %q, want %q", addr, original)
		}
		return []string{net.JoinHostPort("127.0.0.2", port), serverAddr}, nil
	}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+original+"/demo", nil)
	require.NoError(t, err)
	resp, err := client.Do(req, proxy, 1, 1, RequestOptions{})
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "ok", string(body))
	require.Equal(t, original, seenHost.Load())
	require.Equal(t, int64(2), connections.Load())
}

func TestControlledHTTPProxyUsesAbsoluteFormAndPinsEachRedirect(t *testing.T) {
	var requests atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method == http.MethodConnect || r.URL.Host != "93.184.216.34:80" {
			t.Errorf("unexpected proxy request: %s %s", r.Method, r.RequestURI)
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/first":
			http.Redirect(w, r, "/second?a=1&a=2", http.StatusFound)
		case "/second":
			// Redirect resolution is tested with the original authority by the pin callback.
			if r.URL.RawQuery != "a=1&a=2" {
				t.Errorf("redirect query was not preserved: %s", r.URL.RawQuery)
			}
			http.Redirect(w, r, "http://169.254.169.254/", http.StatusFound)
		}
	}))
	t.Cleanup(proxy.Close)
	client := NewClient(&Config{MaxConnections: 1, DialAddresses: func(_ context.Context, _, addr string) ([]string, error) {
		if addr == "upstream.example:80" {
			return []string{"93.184.216.34:80"}, nil
		}
		return nil, errors.New("destination denied by fixture")
	}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://upstream.example/first", nil)
	require.NoError(t, err)
	_, err = client.Do(req, proxy.URL, 1, 1, RequestOptions{})
	require.ErrorContains(t, err, "destination denied by fixture")
	require.EqualValues(t, 2, requests.Load())
}

func TestControlledDialLimitsConnectionsAcrossTransports(t *testing.T) {
	client := NewClient(&Config{MaxConnections: 1})
	dial := func(context.Context, string, string) (net.Conn, error) {
		conn, other := net.Pipe()
		t.Cleanup(func() { _ = other.Close() })
		return conn, nil
	}
	firstTransport := &http.Transport{DialContext: dial}
	secondTransport := &http.Transport{DialContext: dial}
	require.NoError(t, client.configureDialPolicy(firstTransport, false))
	require.NoError(t, client.configureDialPolicy(secondTransport, false))
	conn, err := firstTransport.DialContext(t.Context(), "tcp", "127.0.0.1:443")
	require.NoError(t, err)
	_, err = secondTransport.DialContext(t.Context(), "tcp", "127.0.0.1:443")
	require.ErrorContains(t, err, "connection limit")
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Close())
	conn, err = secondTransport.DialContext(t.Context(), "tcp", "127.0.0.1:443")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestControlledDialReclaimsIdleSocketForNewProxy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(server.Close)
	proxy1, calls1 := startTestSOCKS5Proxy(t)
	proxy2, calls2 := startTestSOCKS5Proxy(t)
	client := NewClient(&Config{
		ConnectionPoolIsolation: ConnectionPoolIsolationAccountProxy,
		MaxConnections:          1, MaxUpstreamClients: 2,
		DialAddresses: func(_ context.Context, _, addr string) ([]string, error) { return []string{addr}, nil },
	})
	for _, proxy := range []string{proxy1, proxy2, proxy1, proxy2} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req, proxy, 1, 1, RequestOptions{})
		require.NoError(t, err)
		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.LessOrEqual(t, len(client.connections), 1)
		require.LessOrEqual(t, len(client.clients), 2)
	}
	require.EqualValues(t, 2, calls1.Load())
	require.EqualValues(t, 2, calls2.Load())
}

func TestControlledDialTrustedDNSRequiresSOCKSAndSendsExactName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	t.Cleanup(server.Close)
	const name = "proxy-dns-only.invalid"
	var wireName atomic.Value
	proxy, _ := startTestSOCKS5ProxyWithResolver(t, func(host string) string {
		wireName.Store(host)
		return "127.0.0.1"
	})
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)
	client := NewClient(&Config{ProxyDNS: true, DialAddresses: func(_ context.Context, _, addr string) ([]string, error) {
		return []string{addr}, nil
	}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+net.JoinHostPort(name, port), nil)
	require.NoError(t, err)
	_, err = client.Do(req, "", 1, 1, RequestOptions{})
	require.ErrorContains(t, err, "requires SOCKS")
	resp, err := client.Do(req, proxy, 1, 1, RequestOptions{})
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 204, resp.StatusCode)
	require.Equal(t, name, wireName.Load())
}
