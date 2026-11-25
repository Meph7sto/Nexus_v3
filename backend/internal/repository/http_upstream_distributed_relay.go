package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/pkg/servertiming"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

var (
	errNoHealthyWorker   = errors.New("no healthy worker route")
	errInvalidRelayReply = errors.New("invalid worker relay reply")
	errIncompleteRelay   = errors.New("worker relay ended without a completion trailer")
)

func (s *DistributedHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return s.route(req, proxyURL, accountID, accountConcurrency, nil)
}

func (s *DistributedHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return s.route(req, proxyURL, accountID, accountConcurrency, profile)
}

func (s *DistributedHTTPUpstream) route(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	local := func(r *http.Request) (*http.Response, error) {
		if profile != nil {
			return s.local.DoWithTLS(r, proxyURL, accountID, accountConcurrency, profile)
		}
		return s.local.Do(r, proxyURL, accountID, accountConcurrency)
	}
	snapshot := s.current.Load()
	if snapshot == nil || req != nil && !service.DistributedHTTPUpstreamEnabled(req.Context()) {
		return local(req)
	}
	if req == nil || req.URL == nil || req.Host != "" && !strings.EqualFold(req.Host, req.URL.Host) {
		return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, errors.New("invalid upstream request"))
	}
	timeout := ""
	if localTransport, ok := s.local.(interface {
		workerResponseHeaderTimeout(*http.Request) time.Duration
	}); ok {
		timeout = localTransport.workerResponseHeaderTimeout(req).String()
	}
	if strings.EqualFold(req.URL.Hostname(), grokCLIProxyHost) {
		req = req.Clone(req.Context())
		applyGrokCLIProxyHeaders(req)
		return (&grokAccessDeniedFallbackTransport{base: relayRoundTrip(func(r *http.Request) (*http.Response, error) {
			return snapshot.forward(r, proxyURL, accountID, accountConcurrency, profile, timeout, local)
		})}).RoundTrip(req)
	}
	return snapshot.forward(req, proxyURL, accountID, accountConcurrency, profile, timeout, local)
}

type relayRoundTrip func(*http.Request) (*http.Response, error)

func (rt relayRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

func (snapshot *workerSnapshot) forward(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile, timeout string, local relayRoundTrip) (*http.Response, error) {
	meta, err := workerproto.EncodeJSON(workerproto.EndToEndHeaders(req.Header))
	if err != nil {
		return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, err)
	}
	proxy, err := workerproto.EncodeProxy(proxyURL)
	if err != nil {
		return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, err)
	}
	tlsMeta := ""
	if profile != nil {
		tlsMeta, err = workerproto.EncodeJSON(profile)
		if err != nil {
			return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, err)
		}
	}
	attempted := make(map[*relayWorker]bool)
	lastErr := relayFailure(service.HTTPUpstreamRelayWorker, true, errNoHealthyWorker)
	for len(attempted) < snapshot.maxAttempts {
		if snapshot.retired.Load() || !pkghttputil.UpstreamClientActive(req.Context()) {
			break
		}
		node := snapshot.selectWorker(accountID, attempted)
		if node == nil {
			break
		}
		attempt, err := replayWorkerRequest(req, len(attempted) != 0)
		if err != nil {
			return nil, lastErr
		}
		attempted[node] = true
		resp, relayErr, retry := snapshot.relay(node, attempt, accountID, accountConcurrency, meta, proxy, tlsMeta, timeout)
		if relayErr == nil {
			return resp, nil
		}
		lastErr = relayErr
		if !retry || !workerRequestReplayable(req) {
			return nil, lastErr
		}
	}
	if snapshot.retired.Load() || !snapshot.fallbackLocal || !workerRequestReplayable(req) || !pkghttputil.UpstreamClientActive(req.Context()) {
		return nil, lastErr
	}
	attempt, err := replayWorkerRequest(req, len(attempted) != 0)
	if err != nil {
		return nil, lastErr
	}
	if snapshot.retired.Load() || !pkghttputil.UpstreamClientActive(req.Context()) {
		if attempt.Body != nil {
			_ = attempt.Body.Close()
		}
		return nil, lastErr
	}
	resp, err := local(attempt.WithContext(context.WithValue(attempt.Context(), grokRelayHandledKey{}, true)))
	if err != nil {
		relayErr := relayFailure(service.HTTPUpstreamRelayUpstream, false, err)
		relayErr.LocalFallback = true
		return resp, relayErr
	}
	if resp != nil {
		service.MarkDistributedHTTPUpstreamResponse(resp, req)
	}
	return resp, nil
}

func workerRequestReplayable(req *http.Request) bool {
	return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
}

func replayWorkerRequest(req *http.Request, previouslyAttempted bool) (*http.Request, error) {
	if !previouslyAttempted || req.Body == nil || req.Body == http.NoBody {
		return req, nil
	}
	if req.GetBody == nil {
		return nil, errors.New("upstream body cannot be replayed")
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	clone := req.WithContext(req.Context())
	clone.Body = body
	return clone, nil
}

func (s *workerSnapshot) relay(node *relayWorker, req *http.Request, accountID int64, accountConcurrency int, meta, proxy, tlsMeta, timeout string) (*http.Response, *service.HTTPUpstreamRelayError, bool) {
	ctx, cancel := pkghttputil.NewUpstreamAttemptContext(req.Context(), s.drainTimeout)
	var gotConnection atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { gotConnection.Store(true) },
	})
	ctx = servertiming.WithDependencyModule(ctx, "worker")
	body := req.Body
	if body == nil {
		body = http.NoBody
	}
	// Keep even empty requests non-replayable within the HTTP/2 transport. Only
	// the outer loop may replay, and only on explicit proof of no upstream send.
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodPost, node.address+workerproto.RelayPath, unreplayableRelayBody{body})
	if err != nil {
		cancel()
		_ = body.Close()
		return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, errInvalidRelayReply), false
	}
	outgoing.ContentLength = req.ContentLength
	if req.Body == nil || req.Body == http.NoBody {
		outgoing.Body = http.NoBody
	}
	outgoing.Header.Set("Authorization", "Bearer "+node.token)
	outgoing.Header.Set(workerproto.HeaderMethod, req.Method)
	outgoing.Header.Set(workerproto.HeaderTargetURL, req.URL.String())
	outgoing.Header.Set(workerproto.HeaderProxy, proxy)
	if tlsMeta != "" {
		outgoing.Header.Set(workerproto.HeaderTLSProfile, tlsMeta)
	}
	outgoing.Header.Set(workerproto.HeaderAccountID, strconv.FormatInt(accountID, 10))
	outgoing.Header.Set(workerproto.HeaderAccountConcurrency, strconv.Itoa(accountConcurrency))
	outgoing.Header.Set(workerproto.HeaderUpstreamProfile, string(service.HTTPUpstreamProfileFromContext(req.Context())))
	if timeout != "" {
		outgoing.Header.Set(workerproto.HeaderResponseHeaderTimeout, timeout)
	}
	outgoing.Header.Set(workerproto.HeaderRedirectsDisabled, strconv.FormatBool(service.HTTPUpstreamRedirectsDisabled(req.Context())))
	outgoing.Header.Set(workerproto.HeaderPublicHostsOnly, strconv.FormatBool(service.HTTPUpstreamPublicHostsOnly(req.Context())))
	outgoing.Header.Set(workerproto.HeaderUpstreamHeaders, meta)

	node.localLoad.Add(1)
	release := func() {
		cancel()
		node.localLoad.Add(-1)
		if s.retired.Load() {
			node.client.CloseIdleConnections()
		}
	}
	if s.retired.Load() || !pkghttputil.UpstreamClientActive(req.Context()) {
		_ = body.Close()
		release()
		return nil, relayFailure(service.HTTPUpstreamRelayWorker, true, errNoHealthyWorker), false
	}
	outer, err := servertiming.Do(node.client, outgoing)
	if err != nil {
		if ctx.Err() == nil {
			node.failure()
		}
		release()
		unsent := !gotConnection.Load() && errors.Is(err, errWorkerNotConnected)
		return nil, relayFailure(service.HTTPUpstreamRelayLink, unsent, err), unsent
	}
	if outer.StatusCode != http.StatusOK {
		relayErr, retry := interpretRelayFailure(ctx, node, outer)
		_ = outer.Body.Close()
		release()
		return nil, relayErr, retry
	}

	statusValues := outer.Header.Values(workerproto.HeaderUpstreamStatus)
	if len(statusValues) != 1 || len(outer.Header.Values(workerproto.HeaderCompletion)) != 0 {
		_ = outer.Body.Close()
		release()
		if ctx.Err() == nil {
			node.failure()
		}
		return nil, relayFailure(service.HTTPUpstreamRelayLink, false, errInvalidRelayReply), false
	}
	status, err := strconv.Atoi(statusValues[0])
	if err != nil || len(statusValues[0]) != 3 || status < 200 || status > 599 {
		_ = outer.Body.Close()
		release()
		if ctx.Err() == nil {
			node.failure()
		}
		return nil, relayFailure(service.HTTPUpstreamRelayLink, false, errInvalidRelayReply), false
	}
	resp := &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Header:        workerproto.EndToEndHeaders(outer.Header),
		ContentLength: -1,
		Body: &workerRelayBody{
			outer: outer, node: node, snapshot: s, ctx: ctx, release: release,
		},
	}
	service.MarkDistributedHTTPUpstreamResponse(resp, req)
	// Gate account-changing error handling on a complete error envelope. Many
	// channel adapters inspect just the status or a prefix of the error body.
	if status >= 400 {
		const limit = 2 << 20
		data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		_ = resp.Body.Close()
		if err != nil || len(data) > limit {
			return nil, relayFailure(service.HTTPUpstreamRelayLink, false, errIncompleteRelay), false
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
	}
	return resp, nil, false
}

type unreplayableRelayBody struct{ io.ReadCloser }

func interpretRelayFailure(ctx context.Context, node *relayWorker, resp *http.Response) (*service.HTTPUpstreamRelayError, bool) {
	hasControls := len(resp.Header.Values(workerproto.HeaderUpstreamStatus)) != 0 ||
		len(resp.Header.Values(workerproto.HeaderCompletion)) != 0
	if failure, valid := decodeRelayError(resp.Body); !hasControls && valid {
		switch {
		case failure.Outcome == workerproto.OutcomeUnsent &&
			((failure.Code == workerproto.CodeWorkerOverloaded && resp.StatusCode == http.StatusTooManyRequests) ||
				(failure.Code == workerproto.CodeWorkerDraining && resp.StatusCode == http.StatusServiceUnavailable)):
			if failure.Code == workerproto.CodeWorkerDraining {
				node.mu.Lock()
				node.healthy = false
				node.mu.Unlock()
			}
			return relayFailure(service.HTTPUpstreamRelayWorker, true, errors.New("worker did not accept request")), true
		case failure.Outcome == workerproto.OutcomeUnknown &&
			(failure.Code == workerproto.CodeUpstreamError || failure.Code == workerproto.CodeUpstreamPersistent) &&
			resp.StatusCode == http.StatusBadGateway:
			node.success()
			relayErr := relayFailure(service.HTTPUpstreamRelayUpstream, false, errors.New("worker reported upstream transport failure"))
			relayErr.Persistent = failure.Code == workerproto.CodeUpstreamPersistent
			return relayErr, false
		}
	}
	if ctx.Err() == nil {
		node.failure()
	}
	return relayFailure(service.HTTPUpstreamRelayLink, false, errInvalidRelayReply), false
}

// Duplicate and unknown fields cannot establish that the Worker rejected the
// request before sending it upstream.
func decodeRelayError(body io.Reader) (workerproto.RelayError, bool) {
	var result workerproto.RelayError
	limited := &io.LimitedReader{R: body, N: workerHealthBodyLimit + 1}
	decoder := json.NewDecoder(limited)
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return result, false
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return result, false
		}
		switch key {
		case "code":
			err = decoder.Decode(&result.Code)
		case "outcome":
			err = decoder.Decode(&result.Outcome)
		default:
			return result, false
		}
		if err != nil {
			return result, false
		}
		seen[key] = true
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') || len(seen) != 2 {
		return result, false
	}
	_, err = decoder.Token()
	return result, errors.Is(err, io.EOF) && limited.N > 0
}

func relayFailure(origin service.HTTPUpstreamRelayErrorOrigin, unsent bool, err error) *service.HTTPUpstreamRelayError {
	return &service.HTTPUpstreamRelayError{Origin: origin, Unsent: unsent, Err: err}
}

type workerRelayBody struct {
	outer    *http.Response
	node     *relayWorker
	snapshot *workerSnapshot
	ctx      context.Context
	release  func()
	done     sync.Once
	closed   sync.Once
	err      error
}

func (b *workerRelayBody) Read(p []byte) (int, error) {
	n, err := b.outer.Body.Read(p)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		completion := b.outer.Trailer.Values(workerproto.HeaderCompletion)
		if len(completion) == 1 && completion[0] == workerproto.CompletionComplete {
			b.done.Do(func() { b.node.success(); b.release() })
			return n, io.EOF
		}
		err = errIncompleteRelay
	}
	b.done.Do(func() {
		if b.ctx.Err() == nil {
			b.node.failure()
		}
		b.release()
	})
	return n, relayFailure(service.HTTPUpstreamRelayLink, false, err)
}

func (b *workerRelayBody) Close() error {
	b.closed.Do(func() {
		b.done.Do(b.release)
		b.err = b.outer.Body.Close()
		if b.snapshot.retired.Load() {
			b.node.client.CloseIdleConnections()
		}
	})
	return b.err
}
