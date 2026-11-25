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
