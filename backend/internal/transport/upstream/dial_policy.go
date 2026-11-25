package upstream

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func (s *Client) configureDialPolicy(transport *http.Transport, socks bool) error {
	if s.cfg != nil && s.cfg.ProxyDNS && (!socks || s.cfg.DialAddresses == nil) {
		return errors.New("proxy DNS requires SOCKS and a destination policy")
	}
	if s.cfg == nil || (s.cfg.DialAddresses == nil && s.connections == nil) {
		return nil
	}
	pin, dial := s.cfg.DialAddresses, transport.DialContext
	if pin != nil && transport.Proxy != nil {
		return errors.New("controlled destination dialing does not support HTTP proxies")
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		targets := []string{addr}
		if pin != nil {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			var err error
			targets, err = pin(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			_, port, originalErr := net.SplitHostPort(addr)
			if originalErr != nil || len(targets) == 0 {
				return nil, errors.New("invalid controlled destination")
			}
			for _, target := range targets {
				ip, targetPort, targetErr := net.SplitHostPort(target)
				if targetErr != nil || port != targetPort || (net.ParseIP(ip) == nil && !(s.cfg.ProxyDNS && target == addr)) {
					return nil, errors.New("invalid controlled destination")
				}
			}
		}
		if s.connections != nil && !s.acquireConnectionSlot() {
			return nil, errors.New("upstream connection limit reached")
		}
		var err error
		for i, target := range targets {
			if err = ctx.Err(); err != nil {
				break
			}
			var conn net.Conn
			attempt := ctx
			var cancel context.CancelFunc
			if deadline, ok := ctx.Deadline(); ok && i+1 < len(targets) {
				attempt, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(targets)-i))
			}
			conn, err = dial(attempt, network, target)
			if cancel != nil {
				cancel()
			}
			if err == nil {
				if s.connections == nil {
					return conn, nil
				}
				return &limitedConn{Conn: conn, slots: s.connections}, nil
			}
		}
		if s.connections != nil {
			<-s.connections
		}
		return nil, err
	}
	return nil
}

// A new account or proxy must not wait for a pool of inactive sockets to age out.
func (s *Client) acquireConnectionSlot() bool {
	select {
	case s.connections <- struct{}{}:
		return true
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, entry := range s.clients {
		if atomic.LoadInt64(&entry.inFlight) != 0 {
			continue
		}
		entry.client.CloseIdleConnections()
		select {
		case s.connections <- struct{}{}:
			return true
		default:
		}
	}
	return false
}

type limitedConn struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
	err   error
}

func (c *limitedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		<-c.slots
	})
	return c.err
}
