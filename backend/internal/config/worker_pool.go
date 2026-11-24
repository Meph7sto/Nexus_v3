package config

import (
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
)

// WorkerPoolConfig describes the opt-in static pool used by the Responses pilot.
type WorkerPoolConfig struct {
	Enabled                 bool                  `mapstructure:"enabled"`
	Nodes                   []WorkerNodeConfig    `mapstructure:"nodes"`
	DefaultProxyURL         string                `mapstructure:"default_proxy_url"`
	FallbackLocal           bool                  `mapstructure:"fallback_local"`
	AllowPrivateH2C         bool                  `mapstructure:"allow_private_h2c"`
	TLSCAFile               string                `mapstructure:"tls_ca_file"`
	TLSCertFile             string                `mapstructure:"tls_cert_file"`
	TLSKeyFile              string                `mapstructure:"tls_key_file"`
	HealthInterval          time.Duration         `mapstructure:"health_interval"`
	HealthTimeout           time.Duration         `mapstructure:"health_timeout"`
	MaxAttempts             int                   `mapstructure:"max_attempts"`
	DrainTimeout            time.Duration         `mapstructure:"-"`
	SelectionPolicy         string                `mapstructure:"selection_policy"`
	CircuitFailureThreshold int                   `mapstructure:"circuit_failure_threshold"`
	CircuitOpenTimeout      time.Duration         `mapstructure:"circuit_open_timeout"`
	Bindings                []WorkerBindingConfig `mapstructure:"bindings"`
}

type WorkerNodeConfig struct {
	ID              string `mapstructure:"id"`
	URL             string `mapstructure:"url"`
	SharedToken     string `mapstructure:"shared_token"`
	SharedTokenFile string `mapstructure:"shared_token_file"`
	Weight          int    `mapstructure:"weight"`
	MaxInFlight     int    `mapstructure:"max_inflight"`
}

type WorkerBindingConfig struct {
	Ref             string `mapstructure:"ref"`
	NodeID          string `mapstructure:"node_id"`
	URL             string `mapstructure:"url"`
	SharedToken     string `mapstructure:"shared_token"`
	SharedTokenFile string `mapstructure:"shared_token_file"`
}

func (c WorkerPoolConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("worker_pool.nodes is required when enabled")
	}
	_, proxy, err := proxyurl.Parse(c.DefaultProxyURL)
	if err != nil || proxy == nil || (proxy.Scheme != "socks5" && proxy.Scheme != "socks5h") {
		return fmt.Errorf("worker_pool.default_proxy_url must be a SOCKS5 URL")
	}
	if c.HealthInterval < 0 || c.HealthTimeout < 0 || c.MaxAttempts < 0 {
		return fmt.Errorf("worker_pool health timings and max_attempts must be non-negative")
	}
	return nil
}
