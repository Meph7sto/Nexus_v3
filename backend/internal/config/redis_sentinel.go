package config

import (
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

// RedisSentinelConfig is opt-in. Separate authentication and endpoint lists
// keep Redis data credentials out of the Sentinel connections.
type RedisSentinelConfig struct {
	Addresses        []string `mapstructure:"addresses" json:"addresses" yaml:"addresses"`
	AllowedPrimaries []string `mapstructure:"allowed_primaries" json:"allowed_primaries" yaml:"allowed_primaries"`
	MasterName       string   `mapstructure:"master_name" json:"master_name" yaml:"master_name"`
	Username         string   `mapstructure:"username" json:"username" yaml:"username,omitempty"`
	Password         string   `mapstructure:"password" json:"password" yaml:"password,omitempty"`
	EnableTLS        bool     `mapstructure:"enable_tls" json:"enable_tls" yaml:"enable_tls,omitempty"`
}

func (c RedisSentinelConfig) Configured() bool {
	return len(c.Addresses) != 0 || len(c.AllowedPrimaries) != 0 || c.MasterName != "" ||
		c.Username != "" || c.Password != "" || c.EnableTLS
}

var (
	redisSentinelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	redisHostname     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$`)
)

func (c RedisSentinelConfig) Validate() error {
	if !c.Configured() {
		return nil
	}
	if !redisSentinelName.MatchString(c.MasterName) {
		return fmt.Errorf("redis.sentinel.master_name is required and must be a valid name")
	}
	if len(c.Addresses) == 0 || len(c.Addresses) > 16 || len(c.AllowedPrimaries) == 0 || len(c.AllowedPrimaries) > 64 {
		return fmt.Errorf("redis.sentinel requires addresses (max 16) and allowed_primaries (max 64)")
	}
	seen := make(map[string]bool)
	for _, item := range []struct {
		name      string
		addresses []string
	}{
		{"addresses", c.Addresses},
		{"allowed_primaries", c.AllowedPrimaries},
	} {
		for i, addr := range item.addresses {
			normalized, ok := NormalizeRedisEndpoint(addr)
			if !ok || seen[normalized] {
				return fmt.Errorf("redis.sentinel.%s[%d] must be a unique host:port", item.name, i)
			}
			seen[normalized] = true
		}
	}
	if len(c.Username) > 128 || (c.Username != "" && c.Password == "") {
		return fmt.Errorf("redis.sentinel.username requires a password")
	}
	return nil
}

// NormalizeRedisEndpoint accepts only network endpoints, not URLs, Unix socket
// paths or credentials. It also canonicalizes discovery comparisons.
func NormalizeRedisEndpoint(addr string) (string, bool) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return "", false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", false
	}
	if ip, err := netip.ParseAddr(host); err == nil && ip.Zone() == "" {
		return net.JoinHostPort(ip.Unmap().String(), strconv.Itoa(n)), true
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if !redisHostname.MatchString(host) || strings.Contains(host, "..") {
		return "", false
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), true
}
