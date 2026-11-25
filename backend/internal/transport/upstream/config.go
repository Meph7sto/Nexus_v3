package upstream

import (
	"context"
	"time"
)

const (
	ConnectionPoolIsolationProxy        = "proxy"
	ConnectionPoolIsolationAccount      = "account"
	ConnectionPoolIsolationAccountProxy = "account_proxy"
)

// Config contains only settings needed by the upstream HTTP client. A zero
// MaxConnsPerHost means no limit and a zero ResponseHeaderTimeout disables the
// timeout; negative values select the built-in defaults for these settings.
type Config struct {
	ConnectionPoolIsolation     string
	MaxIdleConns                int
	MaxIdleConnsPerHost         int
	MaxConnsPerHost             int
	MaxUpstreamClients          int
	IdleConnTimeout             time.Duration
	ClientIdleTTL               time.Duration
	ResponseHeaderTimeout       time.Duration
	OpenAIResponseHeaderTimeout time.Duration
	GrokResponseHeaderTimeout   time.Duration
	UpstreamDrainTimeout        time.Duration
	CheckResolvedIP             bool
	OpenAIHTTP2                 OpenAIHTTP2Config
	// DialAddresses returns validated IPs with the original port. ProxyDNS also
	// permits the original authority, exclusively through an explicit SOCKS proxy.
	DialAddresses  func(context.Context, string, string) ([]string, error)
	ProxyDNS       bool
	ProxyAddresses func(context.Context, string, string) ([]string, error)
	// MaxConnections bounds actual connections across all pooled clients.
	// Zero retains the existing per-client limits.
	MaxConnections int
}

type OpenAIHTTP2Config struct {
	Enabled                   bool
	AllowProxyFallbackToHTTP1 bool
	FallbackErrorThreshold    int
	FallbackWindow            time.Duration
	FallbackTTL               time.Duration
}
