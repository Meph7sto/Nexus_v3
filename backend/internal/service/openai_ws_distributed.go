package service

import (
	"context"
	"errors"
	"net/http"
)

// WorkerWebSocketRoute is an immutable pool snapshot. An established tunnel
// owns its snapshot until Close; subsequent handshakes observe reloads.
type WorkerWebSocketRoute interface {
	RoundTripWebSocket(*http.Request, string, int64) (*http.Response, error)
}
type workerWebSocketProvider interface{ WorkerWebSocketRoute() WorkerWebSocketRoute }

type workerWebSocketAccountKey struct{}

func WithWorkerWebSocketAccount(ctx context.Context, accountID int64) context.Context {
	return context.WithValue(ctx, workerWebSocketAccountKey{}, accountID)
}

var ErrWorkerWebSocketLocalFallback = errors.New("worker did not accept websocket handshake")

type DistributedOpenAIWSDialer struct {
	pool  workerWebSocketProvider
	local *coderOpenAIWSClientDialer
}

func openAIWSDialerFor(up HTTPUpstream) openAIWSClientDialer {
	if pool, ok := up.(workerWebSocketProvider); ok {
		return NewDistributedOpenAIWSDialer(pool)
	}
	return newDefaultOpenAIWSClientDialer()
}

func NewDistributedOpenAIWSDialer(pool workerWebSocketProvider) *DistributedOpenAIWSDialer {
	return &DistributedOpenAIWSDialer{pool: pool, local: &coderOpenAIWSClientDialer{proxyClients: make(map[string]*openAIWSProxyClientEntry)}}
}

type websocketRoundTrip func(*http.Request) (*http.Response, error)

func (rt websocketRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

func (d *DistributedOpenAIWSDialer) Dial(ctx context.Context, target string, h http.Header, proxy string) (openAIWSClientConn, int, http.Header, error) {
	return d.dialRoute(ctx, target, h, proxy, d.pool.WorkerWebSocketRoute())
}
func (d *DistributedOpenAIWSDialer) dialRoute(ctx context.Context, target string, h http.Header, proxy string, route WorkerWebSocketRoute) (openAIWSClientConn, int, http.Header, error) {
	if route != nil && DistributedHTTPUpstreamEnabled(ctx) {
		accountID, _ := ctx.Value(workerWebSocketAccountKey{}).(int64)
		client := &http.Client{Transport: websocketRoundTrip(func(r *http.Request) (*http.Response, error) { return route.RoundTripWebSocket(r, proxy, accountID) }), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		conn, status, headers, err := d.local.dial(ctx, target, h, "", client)
		if !errors.Is(err, ErrWorkerWebSocketLocalFallback) {
			return conn, status, headers, err
		}
		conn, status, headers, err = d.local.Dial(ctx, target, h, proxy)
		if c, ok := conn.(*coderOpenAIWSClientConn); ok {
			c.relayed = true
		}
		if err != nil && (status == 0 || status == http.StatusSwitchingProtocols) {
			err = &HTTPUpstreamRelayError{Origin: HTTPUpstreamRelayUpstream, LocalFallback: true, Err: err}
		}
		return conn, status, headers, err
	}
	return d.local.Dial(ctx, target, h, proxy)
}
func (d *DistributedOpenAIWSDialer) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot {
	return d.local.SnapshotTransportMetrics()
}
