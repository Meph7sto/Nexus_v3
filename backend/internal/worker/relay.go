package worker

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
	"golang.org/x/net/http/httpguts"
)

func (s *Server) relay(w http.ResponseWriter, r *http.Request) {
	if code := s.admit(); code != "" {
		status := http.StatusServiceUnavailable
		if code == workerproto.CodeWorkerOverloaded {
			status = http.StatusTooManyRequests
		}
		writeRelayError(w, status, code, workerproto.OutcomeUnsent)
		return
	}
	defer s.release()

	req, profile, accountID, concurrency, code := s.parseRelay(r)
	if code != "" {
		status := http.StatusBadRequest
		if code == workerproto.CodeTargetDenied {
			status = http.StatusForbidden
		}
		writeRelayError(w, status, code, workerproto.OutcomeUnsent)
		return
	}

	proxy, tlsProfile, err := s.routeMetadata(r)
	if err != nil {
		writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
		return
	}
	opts := upstream.RequestOptions{Profile: profile, RedirectsDisabled: r.Header.Get(workerproto.HeaderRedirectsDisabled) != "false", PublicHostsOnly: r.Header.Get(workerproto.HeaderPublicHostsOnly) == "true"}
	if raw := r.Header.Get(workerproto.HeaderResponseHeaderTimeout); raw != "" {
		timeout, err := time.ParseDuration(raw)
		if err != nil || timeout < 0 {
			writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
			return
		}
		opts.ResponseHeaderTimeout = &timeout
	}
	var resp *http.Response
	forward := s.upstream
	if s.legacyClient != nil && r.Header.Get(workerproto.HeaderProxyID) == workerproto.DefaultProxyID && !opts.PublicHostsOnly {
		forward = s.legacyClient
	}
	if tlsProfile == nil {
		resp, err = forward.Do(req, proxy, accountID, concurrency, opts)
	} else if tlsClient, ok := forward.(interface {
		DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile, upstream.RequestOptions) (*http.Response, error)
	}); ok {
		resp, err = tlsClient.DoWithTLS(req, proxy, accountID, concurrency, tlsProfile, opts)
	} else {
		writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
		return
	}
	if err != nil || resp == nil || resp.Body == nil || resp.StatusCode < 200 || resp.StatusCode > 599 {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		code := workerproto.CodeUpstreamError
		if errors.Is(err, errTargetDNSNotFound) {
			code = workerproto.CodeUpstreamPersistent
		}
		writeRelayError(w, http.StatusBadGateway, code, workerproto.OutcomeUnknown)
		return
	}
	defer resp.Body.Close()

	for key, values := range workerproto.EndToEndHeaders(resp.Header) {
		if key == "Authorization" {
			continue
		}
		w.Header()[key] = values
	}
	w.Header().Set(workerproto.HeaderUpstreamStatus, strconv.Itoa(resp.StatusCode))
	w.Header().Set("Trailer", workerproto.HeaderCompletion)
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		panic(http.ErrAbortHandler)
	}
	buf := make([]byte, 32<<10)
	if _, err := io.CopyBuffer(flushWriter{w: w, controller: controller}, readerOnly{resp.Body}, buf); err != nil {
		panic(http.ErrAbortHandler)
	}
	w.Header().Set(workerproto.HeaderCompletion, workerproto.CompletionComplete)
}

// Prevent a response body's optional WriteTo from bypassing the chunk size.
type readerOnly struct{ io.Reader }

type flushWriter struct {
	w          http.ResponseWriter
	controller *http.ResponseController
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err == nil {
		err = w.controller.Flush()
	}
	return n, err
}

func (s *Server) parseRelay(r *http.Request) (*http.Request, upstream.Profile, int64, int, workerproto.ErrorCode) {
	for _, name := range []string{
		workerproto.HeaderTargetURL, workerproto.HeaderMethod, workerproto.HeaderProxyID, workerproto.HeaderProxy,
		workerproto.HeaderAccountID, workerproto.HeaderAccountConcurrency, workerproto.HeaderUpstreamProfile,
		workerproto.HeaderTLSProfile, workerproto.HeaderRedirectsDisabled, workerproto.HeaderPublicHostsOnly, workerproto.HeaderResponseHeaderTimeout,
		workerproto.HeaderUpstreamHeaders,
	} {
		if len(r.Header.Values(name)) > 1 {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	switch r.Header.Get(workerproto.HeaderMethod) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead:
	default:
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	if id := r.Header.Get(workerproto.HeaderProxyID); id != "" && (id != workerproto.DefaultProxyID || r.Header.Get(workerproto.HeaderProxy) != "" || s.proxyURL == "") {
		return nil, "", 0, 0, workerproto.CodeTargetDenied
	}
	for _, name := range []string{workerproto.HeaderRedirectsDisabled, workerproto.HeaderPublicHostsOnly} {
		if value := r.Header.Get(name); value != "" && value != "true" && value != "false" {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	profile := upstream.Profile(r.Header.Get(workerproto.HeaderUpstreamProfile))
	switch profile {
	case upstream.ProfileDefault, upstream.ProfileOpenAI, upstream.ProfileOpenAIHarvest, upstream.ProfileGrok, upstream.ProfileLongStream:
	default:
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	accountID, idErr := strconv.ParseInt(r.Header.Get(workerproto.HeaderAccountID), 10, 64)
	concurrency, concurrencyErr := strconv.Atoi(r.Header.Get(workerproto.HeaderAccountConcurrency))
	if idErr != nil || concurrencyErr != nil || concurrency < 0 {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}

	rawURL := r.Header.Get(workerproto.HeaderTargetURL)
	target, err := url.Parse(rawURL)
	if err != nil || strings.Contains(rawURL, "#") || !s.targets.allows(target) || r.Header.Get(workerproto.HeaderPublicHostsOnly) == "true" && privateLiteral(target.Hostname()) {
		return nil, "", 0, 0, workerproto.CodeTargetDenied
	}
	h := make(http.Header)
	if encoded := r.Header.Get(workerproto.HeaderUpstreamHeaders); encoded != "" {
		if workerproto.DecodeJSON(encoded, &h) != nil || !validUpstreamHeaders(h, target) {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	// Do not buffer the input or set GetBody: relay uploads cannot be replayed.
	var body io.Reader = r.Body
	if r.ContentLength == 0 {
		body = nil
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Header.Get(workerproto.HeaderMethod), target.String(), body)
	if err != nil {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	req.ContentLength = r.ContentLength
	req.GetBody = nil
	req.Header = workerproto.EndToEndHeaders(h)
	return req, profile, accountID, concurrency, ""
}

func (s *Server) routeMetadata(r *http.Request) (string, *tlsfingerprint.Profile, error) {
	invalid := errors.New("invalid route metadata")
	raw := r.Header.Get(workerproto.HeaderProxy)
	proxy := s.proxyURL
	if raw != "" {
		var spec workerproto.ProxySpec
		if workerproto.DecodeJSON(raw, &spec) != nil {
			return "", nil, invalid
		}
		switch spec.Mode {
		case "direct":
			if spec.URL != "" {
				return "", nil, invalid
			}
			proxy = ""
		case "url":
			_, p, err := proxyurl.Parse(spec.URL)
			if err != nil || p == nil || p.Hostname() == "" || p.RawQuery != "" || p.ForceQuery || p.Fragment != "" || p.Path != "" && p.Path != "/" {
				return "", nil, invalid
			}
			proxy = p.String()
		default:
			return "", nil, invalid
		}
	} else if r.Header.Get(workerproto.HeaderProxyID) != workerproto.DefaultProxyID || proxy == "" {
		return "", nil, invalid
	}
	var profile *tlsfingerprint.Profile
	if raw := r.Header.Get(workerproto.HeaderTLSProfile); raw != "" {
		if workerproto.DecodeJSON(raw, &profile) != nil || profile == nil {
			return "", nil, invalid
		}
	}
	return proxy, profile, nil
}

func validUpstreamHeaders(h http.Header, target *url.URL) bool {
	seen := make(map[string]bool, len(h))
	for name, values := range h {
		key := http.CanonicalHeaderKey(name)
		if !httpguts.ValidHeaderFieldName(name) || seen[key] {
			return false
		}
		seen[key] = true
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return false
			}
		}
		if key == "Host" {
			if len(values) != 1 {
				return false
			}
			wanted, _ := targetAddress(target)
			got, ok := targetAddress(&url.URL{Scheme: "https", Host: values[0]})
			if !ok || got != wanted {
				return false
			}
		}
	}
	return true
}
