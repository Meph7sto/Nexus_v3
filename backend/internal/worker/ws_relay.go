package worker

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

// The inner handshake is HTTP/1.1. After 101 the HTTP/2 request and response
// bodies carry the original masked/unmasked frames, including ping and close.
func (s *Server) wsRelay(w http.ResponseWriter, r *http.Request) {
	if code := s.admit(); code != "" {
		status := http.StatusServiceUnavailable
		if code == workerproto.CodeWorkerOverloaded {
			status = 429
		}
		writeRelayError(w, status, code, workerproto.OutcomeUnsent)
		return
	}
	defer s.release()
	req, _, _, _, code := s.parseRelay(r)
	if code != "" || req.Method != "GET" || req.URL.Scheme != "https" || privateLiteral(req.URL.Hostname()) || req.Header.Get("Sec-WebSocket-Key") == "" {
		writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
		return
	}
	proxy, profile, err := s.routeMetadata(r)
	if err != nil {
		writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
		return
	}
	client := s.client
	if s.useTrustedProxyDNS(proxy, r.Header.Get(workerproto.HeaderPublicHostsOnly) == "true") {
		client = s.trustedProxyClient
	}
	t, err := client.ControlledTransport(proxy, profile)
	if err != nil {
		writeRelayError(w, 400, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
		return
	}
	defer t.CloseIdleConnections()
	t.ResponseHeaderTimeout = 30 * time.Second
	req.Body = nil
	req.ContentLength = 0
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	resp, err := t.RoundTrip(req)
	if err != nil {
		writeRelayError(w, 502, workerproto.CodeUpstreamError, workerproto.OutcomeUnknown)
		return
	}
	defer resp.Body.Close()
	for k, v := range workerproto.EndToEndHeaders(resp.Header) {
		if k != "Authorization" {
			w.Header()[k] = v
		}
	}
	w.Header().Set(workerproto.HeaderUpstreamStatus, strconv.Itoa(resp.StatusCode))
	if resp.StatusCode != http.StatusSwitchingProtocols {
		w.Header().Set("Trailer", workerproto.HeaderCompletion)
		w.WriteHeader(200)
		if _, err = io.Copy(w, resp.Body); err != nil {
			panic(http.ErrAbortHandler)
		}
		w.Header().Set(workerproto.HeaderCompletion, workerproto.CompletionComplete)
		return
	}
	socket, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	ctrl := http.NewResponseController(w)
	w.WriteHeader(200)
	if ctrl.Flush() != nil {
		return
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = io.Copy(socket, r.Body); _ = socket.Close() }()
	_, _ = io.Copy(flushWriter{w, ctrl}, socket)
	_ = r.Body.Close()
	_ = socket.Close()
	<-done
}
