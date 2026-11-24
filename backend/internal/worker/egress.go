package worker

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/util/urlvalidator"
)

type targetPolicy struct {
	authorities map[string]struct{}
	lookupIP    func(context.Context, string, string) ([]netip.Addr, error)
	proxyDNS    bool
}

var errTargetDNSNotFound = errors.New("allowed upstream has no DNS address")

func newTargetPolicy(authorities []string) (targetPolicy, error) {
	p := targetPolicy{
		authorities: make(map[string]struct{}, len(authorities)),
		lookupIP:    net.DefaultResolver.LookupNetIP,
	}
	for _, raw := range authorities {
		u, err := url.Parse("https://" + strings.TrimSpace(raw))
		if err != nil || u.Path != "" || u.ForceQuery || u.RawQuery != "" || strings.Contains(raw, "#") {
			return targetPolicy{}, errors.New("WORKER_ALLOWED_TARGETS must contain exact HTTPS authorities (host[:port])")
		}
		addr, ok := targetAddress(u)
		if !ok {
			return targetPolicy{}, errors.New("WORKER_ALLOWED_TARGETS contains an invalid or non-public authority")
		}
		p.authorities[addr] = struct{}{}
	}
	if len(p.authorities) == 0 {
		return targetPolicy{}, errors.New("WORKER_ALLOWED_TARGETS must contain at least one exact HTTPS authority")
	}
	return p, nil
}

func targetAddress(u *url.URL) (string, bool) {
	if u == nil || u.Scheme != "https" || u.User != nil || u.Opaque != "" || u.Fragment != "" || strings.HasSuffix(u.Host, ":") {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if !validTargetHost(host) {
		return "", false
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}

func validTargetHost(host string) bool {
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Zone() == "" && publicTargetIP(ip)
	}
	if len(host) > 253 || !strings.Contains(host, ".") || urlvalidator.IsBlockedHost(host) {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func (p targetPolicy) allows(u *url.URL) bool {
	addr, ok := targetAddress(u)
	if !ok {
		return false
	}
	_, allowed := p.authorities[addr]
	return allowed
}

// Validate the exact authority before opening a SOCKS connection in either mode.
func (p targetPolicy) dialAddresses(ctx context.Context, network, addr string) ([]string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.New("invalid outbound address")
	}
	host = strings.ToLower(host)
	if _, ok := p.authorities[net.JoinHostPort(host, port)]; !ok {
		return nil, errors.New("outbound authority denied")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return []string{net.JoinHostPort(ip.Unmap().String(), port)}, nil
	}
	if p.proxyDNS {
		return []string{addr}, nil
	}
	family := "ip"
	switch network {
	case "tcp4":
		family = "ip4"
	case "tcp6":
		family = "ip6"
	}
	ips, err := p.lookupIP(ctx, family, host+".")
	var dnsError *net.DNSError
	if ctx.Err() == nil && errors.As(err, &dnsError) && dnsError.IsNotFound {
		return nil, errTargetDNSNotFound
	}
	if err != nil || len(ips) == 0 {
		return nil, errors.New("unable to resolve outbound authority")
	}
	targets := make([]string, 0, len(ips))
	for _, ip := range ips {
		if !publicTargetIP(ip) {
			return nil, errors.New("outbound authority resolved to a non-public address")
		}
		targets = append(targets, net.JoinHostPort(ip.Unmap().String(), port))
	}
	return targets, nil
}

var nonPublicPrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
}

func publicTargetIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
