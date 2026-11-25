package upstream

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"golang.org/x/net/proxy"
)

type contextDial func(context.Context, string, string) (net.Conn, error)

func (d contextDial) Dial(network, address string) (net.Conn, error) {
	return d(context.Background(), network, address)
}
func (d contextDial) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d(ctx, network, address)
}

// ControlledTransport is also used for WebSocket upgrades: policy checks, proxy
// authentication and the actual socket budget must be identical to HTTP egress.
func (s *Client) ControlledTransport(raw string, profile *tlsfingerprint.Profile) (*http.Transport, error) {
	_, p, err := normalizeProxyURL(raw)
	if err != nil {
		return nil, errors.New("invalid outbound proxy")
	}
	t, err := buildUpstreamTransport(poolSettings{}, p, upstreamProtocolModeDefault)
	if err != nil {
		return nil, errors.New("invalid outbound proxy transport")
	}
	t.DisableKeepAlives = true
	if err := s.configureRoute(t, p); err != nil {
		return nil, err
	}
	if profile != nil {
		setControlledTLSProfile(t, profile)
	}
	return t, nil
}

func setControlledTLSProfile(t *http.Transport, profile *tlsfingerprint.Profile) {
	dial := tlsfingerprint.NewDialer(profile, t.DialContext).DialTLSContext
	t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
		defer cancel()
		return dial(ctx, network, addr)
	}
}

// For an HTTP origin, RFC 9112 forward proxies expect absolute-form requests.
// For HTTPS we keep CONNECT so TLS uses the original SNI. Both paths pin the target.
func (s *Client) withHTTPForwardProxy(t *http.Transport, p *url.URL, settings poolSettings) http.RoundTripper {
	if s.cfg == nil || s.cfg.DialAddresses == nil || p == nil || p.Scheme != "http" && p.Scheme != "https" {
		return t
	}
	plain := &http.Transport{
		Proxy: http.ProxyURL(p), MaxIdleConns: settings.maxIdleConns,
		MaxIdleConnsPerHost: settings.maxIdleConnsPerHost, MaxConnsPerHost: settings.maxConnsPerHost,
		IdleConnTimeout: settings.idleConnTimeout, ResponseHeaderTimeout: settings.responseHeaderTimeout,
		TLSHandshakeTimeout: t.TLSHandshakeTimeout, DisableKeepAlives: t.DisableKeepAlives,
	}
	plain.DialContext = s.proxyDialContext
	return &controlledHTTPForwardProxy{tunnel: t, plain: plain, pin: s.cfg.DialAddresses}
}

type controlledHTTPForwardProxy struct {
	tunnel, plain *http.Transport
	pin           func(context.Context, string, string) ([]string, error)
}

func (p *controlledHTTPForwardProxy) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" {
		return p.tunnel.RoundTrip(req)
	}
	port := req.URL.Port()
	if port == "" {
		port = "80"
	}
	ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
	targets, err := p.pin(ctx, "tcp", net.JoinHostPort(req.URL.Hostname(), port))
	cancel()
	if err != nil || len(targets) == 0 {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		if err != nil {
			return nil, err
		}
		return nil, errors.New("invalid controlled destination")
	}
	target := targets[0]
	ip, targetPort, err := net.SplitHostPort(target)
	if err != nil || net.ParseIP(ip) == nil || targetPort != port {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, errors.New("invalid controlled destination")
	}
	out := req.Clone(req.Context())
	if out.Host == "" {
		out.Host = req.URL.Host
	}
	out.URL.Host = target
	// WriteProxy normally builds the absolute URL from Host. Keep the validated
	// IP in the request target even when the original Host header is present.
	path := req.URL.EscapedPath()
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	out.URL.Opaque = "//" + target + path
	resp, err := p.plain.RoundTrip(out)
	if resp != nil {
		resp.Request = req
	}
	return resp, err
}

func (p *controlledHTTPForwardProxy) CloseIdleConnections() {
	p.tunnel.CloseIdleConnections()
	p.plain.CloseIdleConnections()
}

func (s *Client) proxyDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if pin := s.cfg.ProxyAddresses; pin != nil {
		ips, err := pin(ctx, network, addr)
		if err != nil || len(ips) == 0 {
			return nil, errors.New("proxy address denied")
		}
		addr = ips[0]
	}
	if s.connections != nil && !s.acquireConnectionSlot() {
		return nil, errors.New("upstream connection limit reached")
	}
	conn, err := newUpstreamDialer().DialContext(ctx, network, addr)
	if s.connections == nil {
		return conn, err
	}
	if err != nil {
		<-s.connections
		return nil, err
	}
	return &limitedConn{Conn: conn, slots: s.connections}, nil
}

func (s *Client) configureRoute(t *http.Transport, p *url.URL) error {
	if s.cfg == nil || s.cfg.DialAddresses == nil {
		return s.configureDialPolicy(t, p != nil && p.Scheme == "socks5h")
	}
	dial := contextDial(newUpstreamDialer().DialContext)
	if p != nil {
		proxyDial := dial
		if pin := s.cfg.ProxyAddresses; pin != nil {
			proxyDial = func(ctx context.Context, network, addr string) (net.Conn, error) {
				ips, err := pin(ctx, network, addr)
				if err != nil || len(ips) == 0 {
					return nil, errors.New("proxy address denied")
				}
				return newUpstreamDialer().DialContext(ctx, network, ips[0])
			}
		}
		port := p.Port()
		if port == "" {
			switch p.Scheme {
			case "http":
				port = "80"
			case "https":
				port = "443"
			default:
				port = "1080"
			}
		}
		addr := net.JoinHostPort(p.Hostname(), port)
		switch p.Scheme {
		case "socks5h", "socks5":
			var auth *proxy.Auth
			if p.User != nil {
				pw, _ := p.User.Password()
				auth = &proxy.Auth{User: p.User.Username(), Password: pw}
			}
			d, err := proxy.SOCKS5("tcp", addr, auth, proxyDial)
			if err != nil {
				return errors.New("invalid SOCKS proxy")
			}
			dial = d.(proxy.ContextDialer).DialContext
		case "http", "https":
			dial = func(ctx context.Context, network, target string) (net.Conn, error) {
				conn, err := proxyDial(ctx, network, addr)
				if err != nil {
					return nil, errors.New("proxy unavailable")
				}
				if p.Scheme == "https" {
					conn = tls.Client(conn, &tls.Config{ServerName: p.Hostname(), MinVersion: tls.VersionTLS12})
				}
				// A slow or malicious proxy cannot hold a Worker slot indefinitely.
				deadline := time.Now().Add(15 * time.Second)
				if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
					deadline = d
				}
				_ = conn.SetDeadline(deadline)
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				defer stop()
				req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: target}, Host: target, Header: make(http.Header)}
				if p.User != nil {
					pw, _ := p.User.Password()
					req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(p.User.Username()+":"+pw)))
				}
				if err = req.Write(conn); err == nil {
					limit := &io.LimitedReader{R: conn, N: 32 << 10}
					reader := bufio.NewReader(limit)
					resp, e := http.ReadResponse(reader, req)
					if e == nil && resp.StatusCode == 200 && limit.N > 0 && reader.Buffered() == 0 {
						_ = conn.SetDeadline(time.Time{})
						return conn, nil
					}
				}
				_ = conn.Close()
				return nil, errors.New("proxy tunnel failed")
			}
		default:
			return errors.New("unsupported proxy scheme")
		}
	}
	t.Proxy = nil
	t.DialContext = dial
	return s.configureDialPolicy(t, p != nil && p.Scheme == "socks5h")
}
