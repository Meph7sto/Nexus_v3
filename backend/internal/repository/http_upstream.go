package repository

import (
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
)

type httpUpstreamService struct {
	client *upstream.Client
}

// NewHTTPUpstream adapts the shared HTTP client to the service port used by Entry.
func NewHTTPUpstream(cfg *config.Config) service.HTTPUpstream {
	return &httpUpstreamService{client: upstream.NewClient(upstreamConfig(cfg))}
}

func (s *httpUpstreamService) workerResponseHeaderTimeout(req *http.Request) time.Duration {
	return s.client.ResponseHeaderTimeout(upstream.Profile(service.HTTPUpstreamProfileFromContext(req.Context())))
}

func (s *httpUpstreamService) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	applyGrokCLIProxyHeaders(req)
	return s.client.Do(req, proxyURL, accountID, accountConcurrency, upstreamRequestOptions(req))
}

func (s *httpUpstreamService) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	applyGrokCLIProxyHeaders(req)
	return s.client.DoWithTLS(req, proxyURL, accountID, accountConcurrency, profile, upstreamRequestOptions(req))
}

type grokRelayHandledKey struct{}

func upstreamRequestOptions(req *http.Request) upstream.RequestOptions {
	opts := upstream.RequestOptions{
		WrapTransport: func(base http.RoundTripper) http.RoundTripper {
			return &grokAccessDeniedFallbackTransport{base: base}
		},
	}
	if req != nil {
		ctx := req.Context()
		if handled, _ := ctx.Value(grokRelayHandledKey{}).(bool); handled {
			opts.WrapTransport = nil
		}
		opts.Profile = upstream.Profile(service.HTTPUpstreamProfileFromContext(ctx))
		opts.RedirectsDisabled = service.HTTPUpstreamRedirectsDisabled(ctx)
		opts.PublicHostsOnly = service.HTTPUpstreamPublicHostsOnly(ctx)
	}
	return opts
}

func upstreamConfig(cfg *config.Config) *upstream.Config {
	if cfg == nil {
		return nil
	}
	gateway := cfg.Gateway
	allowlist := cfg.Security.URLAllowlist
	return &upstream.Config{
		ConnectionPoolIsolation:     gateway.ConnectionPoolIsolation,
		MaxIdleConns:                gateway.MaxIdleConns,
		MaxIdleConnsPerHost:         gateway.MaxIdleConnsPerHost,
		MaxConnsPerHost:             gateway.MaxConnsPerHost,
		MaxUpstreamClients:          gateway.MaxUpstreamClients,
		IdleConnTimeout:             time.Duration(gateway.IdleConnTimeoutSeconds) * time.Second,
		ClientIdleTTL:               time.Duration(gateway.ClientIdleTTLSeconds) * time.Second,
		ResponseHeaderTimeout:       time.Duration(gateway.ResponseHeaderTimeout) * time.Second,
		OpenAIResponseHeaderTimeout: time.Duration(gateway.OpenAIResponseHeaderTimeout) * time.Second,
		GrokResponseHeaderTimeout:   time.Duration(gateway.GrokResponseHeaderTimeout) * time.Second,
		UpstreamDrainTimeout:        time.Duration(gateway.StreamDataIntervalTimeout) * time.Second,
		CheckResolvedIP:             allowlist.Enabled && !allowlist.AllowPrivateHosts,
		OpenAIHTTP2: upstream.OpenAIHTTP2Config{
			Enabled:                   gateway.OpenAIHTTP2.Enabled,
			AllowProxyFallbackToHTTP1: gateway.OpenAIHTTP2.AllowProxyFallbackToHTTP1,
			FallbackErrorThreshold:    gateway.OpenAIHTTP2.FallbackErrorThreshold,
			FallbackWindow:            time.Duration(gateway.OpenAIHTTP2.FallbackWindowSeconds) * time.Second,
			FallbackTTL:               time.Duration(gateway.OpenAIHTTP2.FallbackTTLSeconds) * time.Second,
		},
	}
}
