package upstream

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

func TestHTTPUpstreamLogsDoNotExposeCredentials(t *testing.T) {
	var output bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	client := NewClient(&Config{OpenAIHTTP2: OpenAIHTTP2Config{
		Enabled:                   true,
		AllowProxyFallbackToHTTP1: true,
		FallbackErrorThreshold:    1,
	}})
	proxyURL := "https://proxy-user:proxy-password@proxy.example:8443?token=proxy-query-secret"
	req, err := http.NewRequest(http.MethodGet, "https://upstream.example/v1?api_key=request-query-secret", nil)
	require.NoError(t, err)
	profile := &tlsfingerprint.Profile{Name: "test"}
	requestErr := errors.New("upstream failed, authorization header=upstream-auth-secret")
	opts := RequestOptions{
		WrapTransport: func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, requestErr
			})
		},
	}
	for range 2 {
		resp, err := client.DoWithTLS(req, proxyURL, 1, 1, profile, opts)
		require.ErrorIs(t, err, requestErr)
		require.Nil(t, resp)
	}

	badProxy := "http://bad-proxy-user:bad-proxy-password@proxy.example/%zz"
	_, err = client.DoWithTLS(req, badProxy, 1, 1, profile, opts)
	require.Error(t, err)
	proxyKey, _, err := normalizeProxyURL(proxyURL)
	require.NoError(t, err)
	client.recordOpenAIHTTP2Failure(ProfileOpenAI, upstreamProtocolModeOpenAIH2, proxyKey, errors.New("http2: protocol error"))
	require.True(t, client.isOpenAIHTTP2FallbackActive(proxyKey))

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = buf.WriteString("HTTP/1.1 407 proxy-reflected-secret\r\nContent-Length: 0\r\n\r\n")
		_ = buf.Flush()
	}))
	t.Cleanup(proxy.Close)
	reflectedProxy, err := url.Parse(proxy.URL)
	require.NoError(t, err)
	reflectedProxy.User = url.UserPassword("proxy-user", "proxy-password")
	_, err = client.DoWithTLS(req, reflectedProxy.String(), 1, 1, profile, RequestOptions{})
	require.ErrorContains(t, err, "407")

	logs := output.String()
	for _, event := range []string{
		"tls_fingerprint_creating_new_client",
		"tls_fingerprint_reusing_client",
		"tls_fingerprint_acquire_client_failed",
		"tls_fingerprint_request_failed",
		"openai_http2_proxy_fallback_activated",
		"proxy_host=proxy.example:8443",
		"tls_fingerprint_http_proxy_connect_failed_status",
	} {
		require.Contains(t, logs, event)
	}
	for _, secret := range []string{
		"proxy-user", "proxy-password", "proxy-query-secret", "request-query-secret", "upstream-auth-secret", "proxy-reflected-secret",
	} {
		require.NotContains(t, logs, secret)
	}
}
