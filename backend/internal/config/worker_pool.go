package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var workerOverlayIPv4 = netip.MustParsePrefix("100.64.0.0/10")

// WorkerPoolConfig describes the upstream relay pool and its local fallback policy.
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

func WorkerEndpointURL(value string, allowPrivateH2C bool) (*url.URL, error) {
	value = strings.TrimSpace(value)
	u, err := url.Parse(value)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || strings.Contains(value, "#") || u.RawPath != "" ||
		(u.Path != "" && u.Path != "/") {
		return nil, errors.New("worker URL must be a private HTTP(S) origin without credentials or path")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !allowPrivateH2C) {
		return nil, errors.New("worker URL requires mTLS or explicitly enabled private h2c")
	}
	port, err := strconv.Atoi(WorkerEndpointPort(u))
	if err != nil || port < 1 || port > 65535 || strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("worker URL has an invalid port")
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !privateWorkerIP(ip) {
		return nil, errors.New("worker URL must use a private address")
	}
	u.Path = ""
	return u, nil
}

func WorkerEndpointPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

// ResolvePrivateWorkerIPs rejects the whole answer if any address is public.
// Dialers must use the returned IPs, not look up the hostname again.
func ResolvePrivateWorkerIPs(ctx context.Context, host string) ([]netip.Addr, error) {
	var ips []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{ip}
	} else {
		ips, err = net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil || len(ips) == 0 {
			return nil, errors.New("worker hostname could not be resolved")
		}
	}
	for i, ip := range ips {
		if !privateWorkerIP(ip) {
			return nil, errors.New("worker hostname must resolve only to private addresses")
		}
		ips[i] = ip.Unmap()
	}
	return ips, nil
}

func privateWorkerIP(ip netip.Addr) bool {
	if ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	return ip.IsPrivate() || ip.IsLoopback() || workerOverlayIPv4.Contains(ip)
}

func ValidWorkerSharedToken(token string) bool {
	return len(token) >= 32 && strings.IndexFunc(token, func(r rune) bool {
		return r < '!' || r > '~' || r == ','
	}) < 0
}

func (c WorkerPoolConfig) validate(clusterEnabled bool) error {
	if !c.Enabled {
		return nil
	}
	if !clusterEnabled && len(c.Nodes) == 0 {
		return fmt.Errorf("worker_pool.nodes is required when enabled")
	}
	if c.HealthInterval < 0 || c.HealthTimeout < 0 || c.MaxAttempts < 0 {
		return fmt.Errorf("worker_pool health timings and max_attempts must be non-negative")
	}
	return nil
}
