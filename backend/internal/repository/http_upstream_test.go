package repository

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestHTTPUpstreamDoCanDisableRedirectsPerRequest(t *testing.T) {
	var redirectedCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	upstream := NewHTTPUpstream(nil)
	req, err := http.NewRequestWithContext(
		service.WithHTTPUpstreamRedirectsDisabled(t.Context()),
		http.MethodGet,
		redirector.URL,
		nil,
	)
	require.NoError(t, err)

	resp, err := upstream.Do(req, "", 1, 1)
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Zero(t, redirectedCalls.Load())
}

func TestHTTPUpstreamDoAppliesGrokCLIIdentityBeforeOAuthRoundTrip(t *testing.T) {
	t.Setenv("XAI_GROK_CLI_VERSION", "")

	for _, endpoint := range []string{"responses", "chat/completions"} {
		t.Run(endpoint, func(t *testing.T) {
			upstream := NewHTTPUpstream(nil)
			captured := make(chan http.Header, 1)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				captured <- req.Header.Clone()
				if req.Header.Get("X-XAI-Token-Auth") != "xai-grok-cli" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(proxy.Close)

			req, err := http.NewRequest(http.MethodPost, "http://cli-chat-proxy.grok.com/v1/"+endpoint, nil)
			require.NoError(t, err)
			req.Header.Set("User-Agent", "legacy-client/1.0")

			resp, err := upstream.Do(req, proxy.URL, 4084, 1)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			capturedHeaders := <-captured
			require.Equal(t, xai.CLIClientVersion, capturedHeaders.Get("x-grok-client-version"))
			require.Equal(t, "1.0.46", capturedHeaders.Get("x-grok-client-version"))
			require.Equal(t, "grok-pager", capturedHeaders.Get("x-grok-client-identifier"))
			require.Equal(t, "interactive", capturedHeaders.Get("x-grok-client-mode"))
			require.Equal(t, "authenticate-response", capturedHeaders.Get("x-authenticateresponse"))
			require.Equal(t, "xai-grok-cli", capturedHeaders.Get("X-XAI-Token-Auth"))
			require.Equal(t, xai.CLIUserAgent(xai.CLIClientVersion), capturedHeaders.Get("User-Agent"))
		})
	}
}

func TestEntryUpstreamTransportFallsBackToOfficialGrokAPIOnCLIAccessDenied(t *testing.T) {
	payload := []byte(`{"model":"grok-4.5","input":"hello"}`)
	req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer oauth-token")
	applyGrokCLIProxyHeaders(req)

	var calls int
	var fallbackBody []byte
	var fallbackHeaders http.Header
	transport := upstreamRequestOptions(req).WrapTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		if calls == 1 {
			require.Equal(t, grokCLIProxyHost, req.URL.Hostname())
			require.Equal(t, "xai-grok-cli", req.Header.Get("X-XAI-Token-Auth"))
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"error":"Access denied"}`)),
				Request:    req,
			}, nil
		}

		fallbackBody = body
		fallbackHeaders = req.Header.Clone()
		require.Equal(t, grokOfficialAPIHost, req.URL.Hostname())
		require.Equal(t, "/v1/responses", req.URL.Path)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"id":"response-ok"}`)),
			Request:    req,
		}, nil
	}))

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	responseBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.JSONEq(t, `{"id":"response-ok"}`, string(responseBody))
	require.Equal(t, 2, calls)
	require.Equal(t, payload, fallbackBody)
	require.Equal(t, "Bearer oauth-token", fallbackHeaders.Get("Authorization"))
	require.Empty(t, fallbackHeaders.Get("X-XAI-Token-Auth"))
	require.Empty(t, fallbackHeaders.Get("x-grok-client-version"))
	require.Empty(t, fallbackHeaders.Get("x-grok-client-mode"))
	require.Empty(t, fallbackHeaders.Get("x-authenticateresponse"))
	require.Empty(t, fallbackHeaders.Get("User-Agent"))
}

func TestGrokAccessDeniedFallbackRecognizesChatEndpointPermissionDenied(t *testing.T) {
	var hosts []string
	transport := &grokAccessDeniedFallbackTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			hosts = append(hosts, req.URL.Hostname())
			if req.URL.Hostname() == grokCLIProxyHost {
				return &http.Response{
					StatusCode: http.StatusForbidden,
					Header:     make(http.Header),
					Body: io.NopCloser(strings.NewReader(
						`{"code":"permission_denied","error":"Access to the chat endpoint is denied. Please ensure you're using the correct credentials. If you believe this is a mistake, please contact support."}`,
					)),
					Request: req,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"id":"response-ok"}`)),
				Request:    req,
			}, nil
		}),
	}

	req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", strings.NewReader(`{"model":"grok-4.5"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer oauth-token")
	req.Header.Set("X-XAI-Token-Auth", "xai-grok-cli")

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, []string{grokCLIProxyHost, grokOfficialAPIHost}, hosts)
}

func TestIsGrokCLICompatibilityAccessDenied(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "legacy compatibility wording", body: `{"error":"Access denied"}`, want: true},
		{
			name: "observed chat endpoint permission denial",
			body: `{"code":"permission_denied","error":"Access to the chat endpoint is denied. Please ensure you're using the correct credentials. If you believe this is a mistake, please contact support."}`,
			want: true,
		},
		{
			name: "entitlement denial using the same broad terms",
			body: `{"code":"permission_denied","error":"Access to the chat endpoint is denied because a subscription is required"}`,
			want: false,
		},
		{
			name: "different permission denied endpoint",
			body: `{"code":"permission_denied","error":"Access to the billing endpoint is denied."}`,
			want: false,
		},
		{
			name: "wrong structured error code",
			body: `{"code":"subscription_required","error":"Access to the chat endpoint is denied. Please ensure you're using the correct credentials. If you believe this is a mistake, please contact support."}`,
			want: false,
		},
		{name: "malformed response", body: `permission_denied: chat endpoint denied`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isGrokCLICompatibilityAccessDenied([]byte(tt.body)))
		})
	}
}

func TestIsGrokCLIAccessDeniedFallbackCandidateRequiresAuthenticatedReplayableCLI403(t *testing.T) {
	newRequest := func() *http.Request {
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", strings.NewReader(`{"model":"grok-4.5"}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer oauth-token")
		req.Header.Set("X-XAI-Token-Auth", "xai-grok-cli")
		return req
	}
	newResponse := func() *http.Response { return &http.Response{StatusCode: http.StatusForbidden} }

	t.Run("valid candidate", func(t *testing.T) {
		require.True(t, isGrokCLIAccessDeniedFallbackCandidate(newRequest(), newResponse()))
	})
	t.Run("non CLI host", func(t *testing.T) {
		req := newRequest()
		req.URL.Host = "api.x.ai"
		require.False(t, isGrokCLIAccessDeniedFallbackCandidate(req, newResponse()))
	})
	t.Run("missing CLI identity", func(t *testing.T) {
		req := newRequest()
		req.Header.Del("X-XAI-Token-Auth")
		require.False(t, isGrokCLIAccessDeniedFallbackCandidate(req, newResponse()))
	})
	t.Run("missing bearer authentication", func(t *testing.T) {
		req := newRequest()
		req.Header.Del("Authorization")
		require.False(t, isGrokCLIAccessDeniedFallbackCandidate(req, newResponse()))
	})
	t.Run("non forbidden response", func(t *testing.T) {
		resp := newResponse()
		resp.StatusCode = http.StatusUnauthorized
		require.False(t, isGrokCLIAccessDeniedFallbackCandidate(newRequest(), resp))
	})
	t.Run("non replayable request", func(t *testing.T) {
		req := newRequest()
		req.GetBody = nil
		require.False(t, isGrokCLIAccessDeniedFallbackCandidate(req, newResponse()))
	})
}

func TestHTTPUpstreamDoDoesNotFallbackForGrokEntitlementDenial(t *testing.T) {
	transport := &grokAccessDeniedFallbackTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"error":"subscription required"}`)),
				Request:    req,
			}, nil
		}),
	}
	req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", strings.NewReader(`{"model":"grok-4.5"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer oauth-token")
	req.Header.Set("X-XAI-Token-Auth", "xai-grok-cli")

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.JSONEq(t, `{"error":"subscription required"}`, string(body))
}

func TestApplyGrokCLIProxyHeaders(t *testing.T) {
	t.Run("uses pinned stable version for the CLI proxy", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "")
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
		require.NoError(t, err)
		req.Header.Set("User-Agent", "legacy-client/1.0")

		applyGrokCLIProxyHeaders(req)

		require.Equal(t, xai.CLIClientVersion, req.Header.Get("x-grok-client-version"))
		require.Equal(t, "xai-grok-cli", req.Header.Get("X-XAI-Token-Auth"))
		require.Equal(t, xai.CLIUserAgent(xai.CLIClientVersion), req.Header.Get("User-Agent"))
	})

	t.Run("accepts a valid operator override", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "1.0.47-alpha.1")
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/chat/completions", nil)
		require.NoError(t, err)

		applyGrokCLIProxyHeaders(req)

		require.Equal(t, "1.0.47-alpha.1", req.Header.Get("x-grok-client-version"))
		require.Equal(t, xai.CLIUserAgent("1.0.47-alpha.1"), req.Header.Get("User-Agent"))
	})

	t.Run("rejects an unsafe override", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "1.0.47\r\nX-Injected: true")
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
		require.NoError(t, err)

		applyGrokCLIProxyHeaders(req)

		require.Equal(t, xai.CLIClientVersion, req.Header.Get("x-grok-client-version"))
		require.Empty(t, req.Header.Get("X-Injected"))
	})

	t.Run("rejects an override below the supported minimum", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "1.0.45")
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
		require.NoError(t, err)

		applyGrokCLIProxyHeaders(req)

		require.Equal(t, xai.CLIClientVersion, req.Header.Get("x-grok-client-version"))
		require.Equal(t, xai.CLIUserAgent(xai.CLIClientVersion), req.Header.Get("User-Agent"))
	})

	t.Run("rejects a prerelease override at the minimum version", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "1.0.46-beta.1")
		req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
		require.NoError(t, err)

		applyGrokCLIProxyHeaders(req)

		require.Equal(t, xai.CLIClientVersion, req.Header.Get("x-grok-client-version"))
		require.Equal(t, xai.CLIUserAgent(xai.CLIClientVersion), req.Header.Get("User-Agent"))
	})

	// Every entry sits above the pinned minimum, so a rejection here can only be
	// caused by the malformed semver and never by the version being too old.
	for _, version := range []string{
		"1.0.047",
		"1.0.47-alpha..1",
		"1.1",
		"2",
		"1.0.47+build.1",
	} {
		t.Run("rejects invalid semver "+version, func(t *testing.T) {
			t.Setenv("XAI_GROK_CLI_VERSION", version)
			req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
			require.NoError(t, err)

			applyGrokCLIProxyHeaders(req)

			require.Equal(t, xai.CLIClientVersion, req.Header.Get("x-grok-client-version"))
			require.Equal(t, xai.CLIUserAgent(xai.CLIClientVersion), req.Header.Get("User-Agent"))
		})
	}

	t.Run("leaves direct xAI API requests unchanged", func(t *testing.T) {
		t.Setenv("XAI_GROK_CLI_VERSION", "1.0.47")
		req, err := http.NewRequest(http.MethodPost, "https://api.x.ai/v1/responses", nil)
		require.NoError(t, err)
		req.Header.Set("User-Agent", "direct-api-client/1.0")

		applyGrokCLIProxyHeaders(req)

		require.Empty(t, req.Header.Get("x-grok-client-version"))
		require.Empty(t, req.Header.Get("X-XAI-Token-Auth"))
		require.Equal(t, "direct-api-client/1.0", req.Header.Get("User-Agent"))
	})
}

func TestHTTPUpstreamDoPublicHostsOnlyRejectsPrivateDestinationBeforeConnecting(t *testing.T) {
	var calls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)

	upstream := NewHTTPUpstream(nil)

	plain, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target.URL, nil)
	require.NoError(t, err)
	resp, err := upstream.Do(plain, "", 1, 1)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int64(1), calls.Load(), "loopback stays reachable for requests without the marker")

	guarded, err := http.NewRequestWithContext(service.WithHTTPUpstreamPublicHostsOnly(t.Context()), http.MethodGet, target.URL, nil)
	require.NoError(t, err)
	resp, err = upstream.Do(guarded, "", 1, 1)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "not allowed")
	require.Equal(t, int64(1), calls.Load(), "marked request must be rejected before any connection is made")
}
