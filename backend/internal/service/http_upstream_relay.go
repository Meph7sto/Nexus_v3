package service

import (
	"context"
	"errors"
	"net/http"
)

type distributedHTTPUpstreamKey struct{}
type distributedHTTPUpstreamResponseKey struct{}

// WithDistributedHTTPUpstream allows the transport to use the configured Worker pool.
func WithDistributedHTTPUpstream(ctx context.Context) context.Context {
	return context.WithValue(ctx, distributedHTTPUpstreamKey{}, true)
}

func DistributedHTTPUpstreamEnabled(ctx context.Context) bool {
	enabled, _ := ctx.Value(distributedHTTPUpstreamKey{}).(bool)
	return enabled
}

// MarkDistributedHTTPUpstreamResponse marks responses attempted by the Worker pool,
// including a local fallback it selected after proving the Worker did not send.
func MarkDistributedHTTPUpstreamResponse(resp *http.Response, req *http.Request) {
	resp.Request = req.WithContext(context.WithValue(req.Context(), distributedHTTPUpstreamResponseKey{}, true))
}

func isDistributedHTTPUpstreamResponse(resp *http.Response) bool {
	if resp == nil || resp.Request == nil {
		return false
	}
	relayed, _ := resp.Request.Context().Value(distributedHTTPUpstreamResponseKey{}).(bool)
	return relayed
}

type HTTPUpstreamRelayErrorOrigin uint8

const (
	HTTPUpstreamRelayWorker HTTPUpstreamRelayErrorOrigin = iota
	HTTPUpstreamRelayLink
	HTTPUpstreamRelayUpstream
)

// HTTPUpstreamRelayError separates ownership from whether the request was sent.
// Unsent is true only when the caller has proof that the upstream was not called.
type HTTPUpstreamRelayError struct {
	Origin        HTTPUpstreamRelayErrorOrigin
	Unsent        bool
	Persistent    bool
	LocalFallback bool
	Err           error
}

func (e *HTTPUpstreamRelayError) Error() string {
	switch {
	case !e.Unsent:
		return "relay request outcome is unknown"
	case e.Origin == HTTPUpstreamRelayUpstream:
		return "relay could not reach upstream"
	default:
		return "worker route is unavailable"
	}
}

func (e *HTTPUpstreamRelayError) Unwrap() error { return e.Err }

func (e *HTTPUpstreamRelayError) IsTerminal() bool {
	return !e.Unsent || e.Origin != HTTPUpstreamRelayUpstream
}

// IsHTTPUpstreamRelayTerminal prevents account failover for relay faults and
// attempts whose outcome is unknown.
func IsHTTPUpstreamRelayTerminal(err error) bool {
	var relayErr *HTTPUpstreamRelayError
	return errors.As(err, &relayErr) && relayErr.IsTerminal()
}
