package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

const (
	testToken    = "test-current-token-not-for-production"
	testOldToken = "test-previous-token-not-for-production"
	testTarget   = "https://api.example.com/v1/responses?stream=true"
)

type forwardFunc func(*http.Request, string, int64, int, upstream.RequestOptions) (*http.Response, error)

func (f forwardFunc) Do(r *http.Request, proxy string, id int64, concurrency int, opts upstream.RequestOptions) (*http.Response, error) {
	return f(r, proxy, id, concurrency, opts)
}

func testConfig() Config {
	return Config{
		NodeID: "fixture", Version: "test", SharedToken: testToken, PreviousToken: testOldToken,
		AllowPrivateH2C: true, AllowedTargets: []string{"api.example.com"}, MaxInFlight: 1,
		DrainTimeout: 2 * time.Second,
	}
}

func startH2C(t *testing.T, f forwardFunc) (*Server, *http.Client, string, context.CancelFunc, <-chan error) {
	t.Helper()
	s, err := New(testConfig(), f)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, listener); close(done) }()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	t.Cleanup(func() {
		cancel()
		client.CloseIdleConnections()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Worker did not terminate after cancellation")
		}
	})
	return s, client, "http://" + listener.Addr().String(), cancel, done
}

func relayRequest(t *testing.T, base string, body io.Reader) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, base+workerproto.RelayPath, body)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set(workerproto.HeaderTargetURL, testTarget)
	r.Header.Set(workerproto.HeaderMethod, http.MethodPost)
	proxy, _ := workerproto.EncodeProxy("")
	r.Header.Set(workerproto.HeaderProxy, proxy)
	r.Header.Set(workerproto.HeaderAccountID, "17")
	r.Header.Set(workerproto.HeaderAccountConcurrency, "3")
	r.Header.Set(workerproto.HeaderUpstreamProfile, string(upstream.ProfileOpenAI))
	return r
}

func readHealth(t *testing.T, client *http.Client, base, token string) (int, workerproto.Health) {
	t.Helper()
	r, _ := http.NewRequest(http.MethodGet, base+workerproto.HealthPath, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	res, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var health workerproto.Health
	if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, health
}

func TestRelayStreamsUploadAndResponseThroughDrain(t *testing.T) {
	const firstRequest = "first upload chunk"
	const secondRequest = "second upload chunk"
	const firstEvent = "data: first\n\n"
	const secondEvent = "data: second\n\n"

	check := func(req *http.Request, proxy string, id int64, concurrency int, opts upstream.RequestOptions) error {
		if req.URL.String() != testTarget || req.Method != http.MethodPost || req.Host != "api.example.com" ||
			id != 17 || concurrency != 3 || opts.Profile != upstream.ProfileOpenAI || opts.RedirectsDisabled || req.GetBody != nil {
			return errors.New("relay changed the target, request policy, or account")
		}
		if req.Header.Get("Authorization") != "Bearer upstream-test" || req.Header.Get("Content-Type") != "application/json" ||
			req.Header.Get("X-Internal-Hop") != "" || req.Header.Get(workerproto.HeaderCompletion) != "" ||
			req.Header.Get("Content-Length") != "" || req.ContentLength != -1 || proxy != "" {
			return errors.New("relay mixed upstream metadata and transport headers")
		}
		first := make([]byte, len(firstRequest))
		if _, err := io.ReadFull(req.Body, first); err != nil || string(first) != firstRequest {
			return errors.New("first request chunk did not stream")
		}
		return nil
	}
	_, client, base, drain, done := startH2C(t, func(req *http.Request, proxy string, id int64, concurrency int, opts upstream.RequestOptions) (*http.Response, error) {
		if err := check(req, proxy, id, concurrency, opts); err != nil {
			t.Error(err)
			return nil, err
		}
		body, write := io.Pipe()
		go func() {
			defer write.Close()
			if _, err := io.WriteString(write, firstEvent); err != nil {
				return
			}
			rest, err := io.ReadAll(req.Body)
			if err != nil || string(rest) != secondRequest {
				_ = write.CloseWithError(errors.New("request body did not preserve the second chunk"))
				return
			}
			_, _ = io.WriteString(write, secondEvent)
		}()
		header := http.Header{
			"Content-Type":               {"text/event-stream"},
			"Connection":                 {"X-Fake-Hop"},
			"X-Fake-Hop":                 {"should not be forwarded"},
			"Content-Length":             {"999"},
			"X-Upstream-Request":         {"preserved"},
			workerproto.HeaderCompletion: {"forged"},
		}
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: body}, nil
	})

	upload, writeUpload := io.Pipe()
	defer writeUpload.Close()
	go func() { _, _ = io.WriteString(writeUpload, firstRequest) }()
	r := relayRequest(t, base, upload)
	h := http.Header{
		"Authorization":              {"Bearer upstream-test"},
		"Content-Type":               {"application/json"},
		"Connection":                 {"X-Internal-Hop"},
		"X-Internal-Hop":             {"not upstream data"},
		"Content-Length":             {"900"},
		"Host":                       {"api.example.com:443"},
		workerproto.HeaderCompletion: {"forged"},
	}
	encoded, err := workerproto.EncodeJSON(h)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(workerproto.HeaderUpstreamHeaders, encoded)
	r.Header.Set(workerproto.HeaderRedirectsDisabled, "false")
	r.Header.Set(workerproto.HeaderPublicHostsOnly, "false")
	res, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.ProtoMajor != 2 || res.StatusCode != http.StatusOK || res.Header.Get(workerproto.HeaderUpstreamStatus) != "429" ||
		res.Header.Get("X-Upstream-Request") != "preserved" || res.Header.Get("X-Fake-Hop") != "" ||
		res.Header.Get(workerproto.HeaderCompletion) != "" || res.Header.Get("Content-Length") != "" {
		t.Fatalf("unexpected relay wire response: HTTP/%d status=%d headers=%v", res.ProtoMajor, res.StatusCode, res.Header)
	}
	first := make([]byte, len(firstEvent))
	if _, err := io.ReadFull(res.Body, first); err != nil || string(first) != firstEvent || res.Trailer.Get(workerproto.HeaderCompletion) != "" {
		t.Fatalf("first event was not available before either stream completed: %q, %v", first, err)
	}

	code, health := readHealth(t, client, base, testOldToken)
	if code != http.StatusOK || health.Status != workerproto.HealthOK || health.InFlight != 1 || health.MaxInFlight != 1 ||
		health.NodeID != "fixture" || health.Version != "test" {
		t.Fatalf("unexpected health during active relay: %d %+v", code, health)
	}
	assertRelayError(t, client, relayRequest(t, base, nil), http.StatusTooManyRequests,
		workerproto.CodeWorkerOverloaded, workerproto.OutcomeUnsent)
	drain()
	for start := time.Now(); ; time.Sleep(time.Millisecond) {
		code, health = readHealth(t, client, base, testToken)
		if code == http.StatusServiceUnavailable && health.Status == workerproto.HealthDraining && health.InFlight == 1 {
			break
		}
		if time.Since(start) > time.Second {
			t.Fatalf("Worker did not enter draining state: %d %+v", code, health)
		}
	}
	assertRelayError(t, client, relayRequest(t, base, nil), http.StatusServiceUnavailable,
		workerproto.CodeWorkerDraining, workerproto.OutcomeUnsent)

	if _, err := io.WriteString(writeUpload, secondRequest); err != nil {
		t.Fatal(err)
	}
	_ = writeUpload.Close()
	remaining, err := io.ReadAll(res.Body)
	if err != nil || string(remaining) != secondEvent || res.Trailer.Get(workerproto.HeaderCompletion) != workerproto.CompletionComplete {
		t.Fatalf("drain interrupted the relay: %q %v, trailers=%v", remaining, err, res.Trailer)
	}
	if err := <-done; err != nil {
		t.Fatalf("graceful drain failed: %v", err)
	}
}

func assertRelayError(t *testing.T, client *http.Client, r *http.Request, status int, code workerproto.ErrorCode, outcome workerproto.Outcome) {
	t.Helper()
	res, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var relayErr workerproto.RelayError
	if err := json.NewDecoder(res.Body).Decode(&relayErr); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != status || relayErr.Code != code || relayErr.Outcome != outcome || res.Header.Get(workerproto.HeaderUpstreamStatus) != "" {
		t.Fatalf("got HTTP %d %+v, want HTTP %d %s %s", res.StatusCode, relayErr, status, code, outcome)
	}
}

type brokenBody struct {
	ctx     context.Context
	partial *strings.Reader
	fail    <-chan struct{}
}

func (b brokenBody) Read(p []byte) (int, error) {
	n, err := b.partial.Read(p)
	if err == io.EOF {
		select {
		case <-b.fail:
			err = io.ErrUnexpectedEOF
		case <-b.ctx.Done():
			err = b.ctx.Err()
		}
	}
	return n, err
}

func (brokenBody) Close() error { return nil }

func TestRelayInterruptedResponseHasNoCompletion(t *testing.T) {
	fail := make(chan struct{})
	_, client, base, _, _ := startH2C(t, func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK,
			Header: http.Header{workerproto.HeaderCompletion: {"complete"}},
			Body:   brokenBody{ctx: req.Context(), partial: strings.NewReader("partial SSE"), fail: fail}}, nil
	})
	res, err := client.Do(relayRequest(t, base, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	prefix := make([]byte, len("partial SSE"))
	if _, err := io.ReadFull(res.Body, prefix); err != nil || string(prefix) != "partial SSE" {
		t.Fatalf("partial response was not flushed: %q %v", prefix, err)
	}
	close(fail)
	if _, err := io.ReadAll(res.Body); err == nil || res.Trailer.Get(workerproto.HeaderCompletion) != "" ||
		res.Header.Get(workerproto.HeaderCompletion) != "" {
		t.Fatal("a truncated upstream response must never have a successful completion marker")
	}
}

func TestRelayCancellationReleasesQuotaAndTransportErrorsAreUnknown(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	_, client, base, _, _ := startH2C(t, func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
		if req.URL.Path == "/cancel" {
			close(started)
			<-req.Context().Done()
			close(cancelled)
			return nil, req.Context().Err()
		}
		return nil, errors.New("simulated loss waiting for upstream headers")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := relayRequest(t, base, nil).WithContext(ctx)
	r.Header.Set(workerproto.HeaderTargetURL, "https://api.example.com/cancel")
	done := make(chan error, 1)
	go func() {
		res, err := client.Do(r)
		if res != nil {
			_ = res.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream was not called")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected client error: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("client cancellation did not reach upstream")
	}
	for start := time.Now(); ; time.Sleep(time.Millisecond) {
		_, health := readHealth(t, client, base, testToken)
		if health.InFlight == 0 {
			break
		}
		if time.Since(start) > time.Second {
			t.Fatalf("cancelled request retained its admission slot: %+v", health)
		}
	}
	assertRelayError(t, client, relayRequest(t, base, nil), http.StatusBadGateway,
		workerproto.CodeUpstreamError, workerproto.OutcomeUnknown)
}

func TestRelayRejectsUntrustedMetadataBeforeForwarding(t *testing.T) {
	s, err := New(testConfig(), forwardFunc(func(*http.Request, string, int64, int, upstream.RequestOptions) (*http.Response, error) {
		t.Error("rejected request was forwarded")
		return nil, errors.New("unexpected forward")
	}))
	if err != nil {
		t.Fatal(err)
	}
	wrongHost, _ := workerproto.EncodeJSON(http.Header{"Host": {"other.example.com"}})
	for _, tc := range []struct {
		key, value string
		status     int
		code       workerproto.ErrorCode
	}{
		{"Authorization", "Bearer unknown", http.StatusUnauthorized, workerproto.CodeUnauthorized},
		{workerproto.HeaderProxyID, "custom", http.StatusForbidden, workerproto.CodeTargetDenied},
		{workerproto.HeaderTLSProfile, "Cg==", http.StatusBadRequest, workerproto.CodeInvalidRequest},
		{workerproto.HeaderUpstreamHeaders, wrongHost, http.StatusBadRequest, workerproto.CodeInvalidRequest},
		{workerproto.HeaderTargetURL, "https://api.example.com:444/a", http.StatusForbidden, workerproto.CodeTargetDenied},
		{workerproto.HeaderTargetURL, "http://api.example.com/a", http.StatusForbidden, workerproto.CodeTargetDenied},
	} {
		t.Run(fmt.Sprintf("%s_%d", tc.key, tc.status), func(t *testing.T) {
			r := relayRequest(t, "http://localhost", nil)
			r.Header.Set(tc.key, tc.value)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			var relayErr workerproto.RelayError
			if err := json.Unmarshal(w.Body.Bytes(), &relayErr); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || relayErr.Code != tc.code || relayErr.Outcome != workerproto.OutcomeUnsent {
				t.Fatalf("got %d %+v", w.Code, relayErr)
			}
		})
	}
}

func TestTargetPolicyPinsAResolvedPublicAddress(t *testing.T) {
	p, err := newTargetPolicy([]string{"api.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	public := netip.MustParseAddr("1.1.1.1")
	ipSet := []netip.Addr{public}
	p.lookupIP = func(_ context.Context, family, host string) ([]netip.Addr, error) {
		if family != "ip" || host != "api.example.com." {
			t.Fatalf("unexpected DNS query %q %q", family, host)
		}
		return ipSet, nil
	}
	if addr, err := p.dialAddresses(context.Background(), "tcp", "api.example.com:443"); err != nil || len(addr) != 1 || addr[0] != "1.1.1.1:443" {
		t.Fatalf("expected a pinned public address, got %s %v", addr, err)
	}
	ipSet = append(ipSet, netip.MustParseAddr("192.168.10.1"))
	if _, err := p.dialAddresses(context.Background(), "tcp", "api.example.com:443"); err == nil {
		t.Fatal("mixed DNS answer containing a private IP must be denied")
	}
	p.proxyDNS = true
	p.lookupIP = nil // An explicitly trusted proxy must receive the name without local DNS.
	if names, err := p.dialAddresses(t.Context(), "tcp", "api.example.com:443"); err != nil || len(names) != 1 || names[0] != "api.example.com:443" {
		t.Fatalf("proxy DNS: got %v, %v", names, err)
	}
	if _, err := p.dialAddresses(context.Background(), "tcp", "api.example.com:444"); err == nil {
		t.Fatal("unapproved port must be denied at dial time")
	}
}

func TestRelayPersistentFailureRequiresConfirmedTargetDNSAttribution(t *testing.T) {
	p, err := newTargetPolicy([]string{"api.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	p.lookupIP = func(_ context.Context, _, _ string) ([]netip.Addr, error) {
		return nil, &net.DNSError{IsNotFound: true}
	}
	_, client, base, _, _ := startH2C(t, func(req *http.Request, _ string, _ int64, _ int, _ upstream.RequestOptions) (*http.Response, error) {
		if req.URL.Path == "/v1/responses" {
			_, err := p.dialAddresses(req.Context(), "tcp", "api.example.com:443")
			return nil, fmt.Errorf("upstream round trip: %w", err)
		}
		return nil, &net.DNSError{IsNotFound: true, Name: "proxy.example.com"}
	})
	assertRelayError(t, client, relayRequest(t, base, nil), http.StatusBadGateway,
		workerproto.CodeUpstreamPersistent, workerproto.OutcomeUnknown)
	r := relayRequest(t, base, nil)
	r.Header.Set(workerproto.HeaderTargetURL, "https://api.example.com/proxy-error")
	assertRelayError(t, client, r, http.StatusBadGateway, workerproto.CodeUpstreamError, workerproto.OutcomeUnknown)
}

func TestWorkerDrainEndpointRequiresBearerAndRemainsObservable(t *testing.T) {
	_, client, base, _, _ := startH2C(t, func(*http.Request, string, int64, int, upstream.RequestOptions) (*http.Response, error) {
		t.Error("drain must not contact the upstream")
		return nil, nil
	})
	request := func(token string) *http.Request {
		r, _ := http.NewRequest(http.MethodPost, base+workerproto.DrainPath, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	assertRelayError(t, client, request("missing"), http.StatusUnauthorized, workerproto.CodeUnauthorized, workerproto.OutcomeUnsent)
	for range 2 {
		res, err := client.Do(request(testToken))
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			t.Fatalf("expected an idempotent drain response, got HTTP %d", res.StatusCode)
		}
	}
	if code, health := readHealth(t, client, base, testToken); code != http.StatusServiceUnavailable ||
		health.Status != workerproto.HealthDraining || health.InFlight != 0 {
		t.Fatalf("drained Worker did not continue reporting health: HTTP %d %+v", code, health)
	}
}
