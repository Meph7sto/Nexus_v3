package repository

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

func (s *DistributedHTTPUpstream) WorkerWebSocketRoute() service.WorkerWebSocketRoute {
	if pool := s.current.Load(); pool != nil {
		return pool
	}
	return nil
}

func (s *workerSnapshot) RoundTripWebSocket(req *http.Request, proxy string, accountID int64) (*http.Response, error) {
	meta, err := workerproto.EncodeJSON(workerproto.EndToEndHeaders(req.Header))
	if err != nil {
		return nil, err
	}
	spec, err := workerproto.EncodeProxy(proxy)
	if err != nil {
		return nil, err
	}
	attempted := map[*relayWorker]bool{}
	for len(attempted) < s.maxAttempts && !s.retired.Load() && req.Context().Err() == nil {
		n := s.selectWorker(accountID, attempted)
		if n == nil {
			break
		}
		attempted[n] = true
		resp, err, retry := s.wsHandshake(req, n, meta, spec, accountID)
		if !retry {
			return resp, err
		}
	}
	if s.fallbackLocal && !s.retired.Load() && req.Context().Err() == nil {
		return nil, service.ErrWorkerWebSocketLocalFallback
	}
	return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, errNoHealthyWorker)
}

func (s *workerSnapshot) wsHandshake(req *http.Request, n *relayWorker, meta, spec string, accountID int64) (*http.Response, error, bool) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(req.Context()))
	stop := context.AfterFunc(req.Context(), cancel)
	var connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }})
	in, out := io.Pipe()
	r, err := http.NewRequestWithContext(ctx, "POST", n.address+workerproto.WSRelayPath, in)
	if err != nil {
		stop()
		cancel()
		_ = in.Close()
		_ = out.Close()
		return nil, errInvalidRelayReply, false
	}
	r.Header.Set("Authorization", "Bearer "+n.token)
	r.Header.Set(workerproto.HeaderMethod, "GET")
	r.Header.Set(workerproto.HeaderTargetURL, req.URL.String())
	r.Header.Set(workerproto.HeaderAccountID, strconv.FormatInt(accountID, 10))
	r.Header.Set(workerproto.HeaderAccountConcurrency, "1")
	r.Header.Set(workerproto.HeaderUpstreamHeaders, meta)
	r.Header.Set(workerproto.HeaderProxy, spec)
	r.Header.Set(workerproto.HeaderPublicHostsOnly, strconv.FormatBool(service.HTTPUpstreamPublicHostsOnly(req.Context())))
	n.localLoad.Add(1)
	release := func() {
		stop()
		cancel()
		_ = out.Close()
		n.localLoad.Add(-1)
		if s.retired.Load() {
			n.client.CloseIdleConnections()
		}
	}
	outer, err := n.client.Do(r)
	if err != nil {
		unsent := !connected.Load() && errors.Is(err, errWorkerNotConnected)
		if req.Context().Err() == nil {
			n.failure()
		}
		release()
		return nil, relayFailure(service.HTTPUpstreamRelayLink, unsent, errInvalidRelayReply), unsent
	}
	if outer.StatusCode != 200 {
		failure, retry := interpretRelayFailure(ctx, n, outer)
		_ = outer.Body.Close()
		release()
		return nil, failure, retry
	}
	values := outer.Header.Values(workerproto.HeaderUpstreamStatus)
	status, err := strconv.Atoi(outer.Header.Get(workerproto.HeaderUpstreamStatus))
	if len(values) != 1 || err != nil || status != 101 && (status < 200 || status > 599) {
		_ = outer.Body.Close()
		release()
		return nil, relayFailure(service.HTTPUpstreamRelayLink, false, errInvalidRelayReply), false
	}
	resp := &http.Response{StatusCode: status, Header: workerproto.EndToEndHeaders(outer.Header), Request: req}
	if status != 101 {
		data, err := io.ReadAll(io.LimitReader(outer.Body, (64<<10)+1))
		_ = outer.Body.Close()
		release()
		if err != nil || len(data) > 64<<10 || outer.Trailer.Get(workerproto.HeaderCompletion) != workerproto.CompletionComplete {
			return nil, relayFailure(service.HTTPUpstreamRelayLink, false, errIncompleteRelay), false
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
		return resp, nil, false
	}
	stop()
	n.success()
	resp.Header.Set("Connection", "Upgrade")
	resp.Header.Set("Upgrade", "websocket")
	resp.Body = &workerSocket{ReadCloser: outer.Body, Writer: out, release: release}
	return resp, nil, false
}

type workerSocket struct {
	io.ReadCloser
	io.Writer
	release func()
	once    sync.Once
}

func (s *workerSocket) Close() error {
	s.once.Do(func() { s.release(); _ = s.ReadCloser.Close() })
	return nil
}
