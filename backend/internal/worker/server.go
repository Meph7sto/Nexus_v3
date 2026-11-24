package worker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"runtime/metrics"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

type Upstream interface {
	Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, opts upstream.RequestOptions) (*http.Response, error)
}

const maxInFlight = 65535

type Server struct {
	addr         string
	nodeID       string
	version      string
	drainTimeout time.Duration
	maxInFlight  int
	started      time.Time
	tlsConfig    *tls.Config
	targets      targetPolicy
	proxyURL     string
	upstream     Upstream
	tokens       [2][sha256.Size]byte
	hasPrevious  int

	mu       sync.Mutex
	draining bool
	inFlight int
	drained  chan struct{}
}

func New(cfg Config, forward Upstream) (*Server, error) {
	cfg.defaults()
	if cfg.MaxInFlight < 1 || cfg.MaxInFlight > maxInFlight || cfg.MaxConnections < 1 || cfg.DrainTimeout <= 0 {
		return nil, errors.New("invalid Worker capacity or drain timeout")
	}
	if !validToken(cfg.SharedToken) || (cfg.PreviousToken != "" && !validToken(cfg.PreviousToken)) {
		return nil, errors.New("Worker bearer tokens must contain at least 32 visible ASCII characters without delimiters")
	}
	tlsConfig, err := cfg.inboundTLS()
	if err != nil {
		return nil, err
	}
	targets, err := newTargetPolicy(cfg.AllowedTargets)
	if err != nil {
		return nil, err
	}
	switch cfg.DNSMode {
	case "", "local":
	case "trusted_proxy":
		targets.proxyDNS = true
	default:
		return nil, errors.New("WORKER_DNS_MODE must be local or trusted_proxy")
	}
	if cfg.ProxyURL != "" {
		_, parsed, err := proxyurl.Parse(cfg.ProxyURL)
		if err != nil || parsed == nil || parsed.Scheme != "socks5h" || parsed.RawPath != "" ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
			return nil, errors.New("WORKER_PROXY_URL must be a configured SOCKS5 endpoint")
		}
		cfg.ProxyURL = parsed.String()
	} else if forward == nil {
		return nil, errors.New("WORKER_PROXY_URL is required")
	}
	if forward == nil {
		forward = upstream.NewClient(&upstream.Config{
			ConnectionPoolIsolation:     upstream.ConnectionPoolIsolationAccountProxy,
			MaxIdleConns:                min(64, cfg.MaxConnections),
			MaxIdleConnsPerHost:         min(16, cfg.MaxConnections),
			MaxConnsPerHost:             cfg.MaxConnections,
			MaxUpstreamClients:          max(128, cfg.MaxInFlight),
			IdleConnTimeout:             45 * time.Second,
			ResponseHeaderTimeout:       60 * time.Second,
			OpenAIResponseHeaderTimeout: 120 * time.Second,
			OpenAIHTTP2: upstream.OpenAIHTTP2Config{
				Enabled: true,
			},
			DialAddresses:  targets.dialAddresses,
			ProxyDNS:       targets.proxyDNS,
			MaxConnections: cfg.MaxConnections,
		})
	}
	s := &Server{
		addr:         cfg.ListenAddr,
		nodeID:       cfg.NodeID,
		version:      cfg.Version,
		drainTimeout: cfg.DrainTimeout,
		maxInFlight:  cfg.MaxInFlight,
		started:      time.Now(),
		tlsConfig:    tlsConfig,
		targets:      targets,
		proxyURL:     cfg.ProxyURL,
		upstream:     forward,
		tokens:       [2][sha256.Size]byte{sha256.Sum256([]byte(cfg.SharedToken)), sha256.Sum256([]byte(cfg.PreviousToken))},
		drained:      make(chan struct{}),
	}
	if cfg.PreviousToken != "" {
		s.hasPrevious = 1
	}
	return s, nil
}

func validToken(token string) bool {
	if len(token) < 32 {
		return false
	}
	for _, c := range token {
		if c < '!' || c > '~' || c == ',' {
			return false
		}
	}
	return true
}

func (s *Server) auth(h http.Header) bool {
	values := h.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return false
	}
	digest := sha256.Sum256([]byte(token))
	current := subtle.ConstantTimeCompare(digest[:], s.tokens[0][:])
	previous := subtle.ConstantTimeCompare(digest[:], s.tokens[1][:])
	return current|(previous&s.hasPrevious) == 1
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case workerproto.HealthPath, workerproto.RelayPath, workerproto.DrainPath:
		if !s.auth(r.Header) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeRelayError(w, http.StatusUnauthorized, workerproto.CodeUnauthorized, workerproto.OutcomeUnsent)
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	switch r.URL.Path {
	case workerproto.HealthPath:
		if r.Method == http.MethodGet {
			s.health(w)
			return
		}
		w.Header().Set("Allow", http.MethodGet)
	case workerproto.RelayPath:
		if r.Method == http.MethodPost {
			s.relay(w, r)
			return
		}
		w.Header().Set("Allow", http.MethodPost)
	case workerproto.DrainPath:
		if r.Method == http.MethodPost {
			s.BeginDrain()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Allow", http.MethodPost)
	}
	writeRelayError(w, http.StatusMethodNotAllowed, workerproto.CodeInvalidRequest, workerproto.OutcomeUnsent)
}

func (s *Server) health(w http.ResponseWriter) {
	s.mu.Lock()
	inFlight, draining := s.inFlight, s.draining
	s.mu.Unlock()
	status, code := workerproto.HealthOK, http.StatusOK
	if draining {
		status, code = workerproto.HealthDraining, http.StatusServiceUnavailable
	}
	sample := [1]metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample[:])
	writeJSON(w, code, workerproto.Health{
		Status: status, NodeID: s.nodeID, InFlight: inFlight, MaxInFlight: s.maxInFlight,
		MemAllocMB: float64(sample[0].Value.Uint64()) / (1024 * 1024),
		UptimeSec:  int64(time.Since(s.started) / time.Second), Version: s.version,
	})
}

func (s *Server) admit() workerproto.ErrorCode {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.draining:
		return workerproto.CodeWorkerDraining
	case s.inFlight >= s.maxInFlight:
		return workerproto.CodeWorkerOverloaded
	default:
		s.inFlight++
		return ""
	}
}

func (s *Server) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.draining && s.inFlight == 0 {
		close(s.drained)
	}
}

// BeginDrain refuses new relay requests while the admitted ones can finish.
func (s *Server) BeginDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.draining {
		s.draining = true
		if s.inFlight == 0 {
			close(s.drained)
		}
	}
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, listener)
}

// Serve owns listener and uses HTTP/2, either over verified mTLS or private h2c.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	protocols := new(http.Protocols)
	protocols.SetHTTP2(s.tlsConfig != nil)
	protocols.SetUnencryptedHTTP2(s.tlsConfig == nil)
	server := &http.Server{
		Handler:           s,
		Protocols:         protocols,
		TLSConfig:         s.tlsConfig,
		MaxHeaderBytes:    64 << 10,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	done := make(chan error, 1)
	go func() {
		if s.tlsConfig != nil {
			done <- server.ServeTLS(listener, "", "")
		} else {
			done <- server.Serve(listener)
		}
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}

	s.BeginDrain()
	deadline, cancel := context.WithTimeout(context.Background(), s.drainTimeout)
	defer cancel()
	select {
	case <-s.drained:
		if err := server.Shutdown(deadline); err != nil {
			_ = server.Close()
			return err
		}
		err := <-done
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-deadline.Done():
		_ = server.Close()
		return deadline.Err()
	}
}

func writeRelayError(w http.ResponseWriter, status int, code workerproto.ErrorCode, outcome workerproto.Outcome) {
	writeJSON(w, status, workerproto.RelayError{Code: code, Outcome: outcome})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
