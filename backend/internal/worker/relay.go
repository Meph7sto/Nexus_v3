package worker

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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

	// The Worker disables redirects and enforces the local destination policy
	// even when the caller omits these controls or sets them to false.
	opts := upstream.RequestOptions{Profile: profile, RedirectsDisabled: true}
	resp, err := s.upstream.Do(req, s.proxyURL, accountID, concurrency, opts)
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
		workerproto.HeaderTargetURL, workerproto.HeaderMethod, workerproto.HeaderProxyID,
		workerproto.HeaderAccountID, workerproto.HeaderAccountConcurrency, workerproto.HeaderUpstreamProfile,
		workerproto.HeaderTLSProfile, workerproto.HeaderRedirectsDisabled, workerproto.HeaderPublicHostsOnly,
		workerproto.HeaderUpstreamHeaders,
	} {
		if len(r.Header.Values(name)) > 1 {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	if r.Header.Get(workerproto.HeaderMethod) != http.MethodPost || r.Header.Get(workerproto.HeaderTLSProfile) != "" {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	if r.Header.Get(workerproto.HeaderProxyID) != workerproto.DefaultProxyID {
		return nil, "", 0, 0, workerproto.CodeTargetDenied
	}
	for _, name := range []string{workerproto.HeaderRedirectsDisabled, workerproto.HeaderPublicHostsOnly} {
		if value := r.Header.Get(name); value != "" && value != "true" && value != "false" {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	profile := upstream.Profile(r.Header.Get(workerproto.HeaderUpstreamProfile))
	if profile != upstream.ProfileDefault && profile != upstream.ProfileOpenAI {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	accountID, idErr := strconv.ParseInt(r.Header.Get(workerproto.HeaderAccountID), 10, 64)
	concurrency, concurrencyErr := strconv.Atoi(r.Header.Get(workerproto.HeaderAccountConcurrency))
	if idErr != nil || accountID <= 0 || concurrencyErr != nil || concurrency < 0 {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}

	rawURL := r.Header.Get(workerproto.HeaderTargetURL)
	target, err := url.Parse(rawURL)
	if err != nil || strings.Contains(rawURL, "#") || !s.targets.allows(target) {
		return nil, "", 0, 0, workerproto.CodeTargetDenied
	}
	h := make(http.Header)
	if encoded := r.Header.Get(workerproto.HeaderUpstreamHeaders); encoded != "" {
		if workerproto.DecodeJSON(encoded, &h) != nil || !validUpstreamHeaders(h, target) {
			return nil, "", 0, 0, workerproto.CodeInvalidRequest
		}
	}
	// Do not buffer the input or set GetBody: relay uploads cannot be replayed.
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target.String(), r.Body)
	if err != nil {
		return nil, "", 0, 0, workerproto.CodeInvalidRequest
	}
	req.ContentLength = r.ContentLength
	req.GetBody = nil
	req.Header = workerproto.EndToEndHeaders(h)
	return req, profile, accountID, concurrency, ""
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
