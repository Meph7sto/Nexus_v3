package repository

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

const (
	workerFailureThreshold = 3
	workerCircuitDuration  = 15 * time.Second
	workerHealthBodyLimit  = 8 << 10
)

var (
	errWorkerNotConnected = errors.New("worker connection could not be established")
	workerOverlayIPv4     = netip.MustParsePrefix("100.64.0.0/10")
)

// DistributedHTTPUpstream routes upstream calls through the active Worker pool.
type DistributedHTTPUpstream struct {
	local   service.HTTPUpstream
	current atomic.Pointer[workerSnapshot]
	mu      sync.Mutex
	closed  bool
}

var _ service.HTTPUpstream = (*DistributedHTTPUpstream)(nil)

type workerSnapshot struct {
	nodes          []*relayWorker
	fallbackLocal  bool
	maxAttempts    int
	healthInterval time.Duration
	healthTimeout  time.Duration
	drainTimeout   time.Duration
	selection      string
	roundRobin     atomic.Uint64
	stop           context.CancelFunc
	retired        atomic.Bool
}

type relayWorker struct {
	id           string
	address      string
	token        string
	weight       int
	capacity     int
	failureLimit int
	openDuration time.Duration
	client       *http.Client
	localLoad    atomic.Int64

	mu         sync.Mutex
	healthy    bool
	remoteLoad int
	failures   int
	openUntil  time.Time
}

func NewDistributedHTTPUpstream(local service.HTTPUpstream, cfg config.WorkerPoolConfig) (*DistributedHTTPUpstream, error) {
	if local == nil {
		return nil, errors.New("local upstream is required")
	}
	pool := &DistributedHTTPUpstream{local: local}
	if err := pool.Reload(cfg); err != nil {
		return nil, err
	}
	return pool, nil
}

// Reload replaces routing for new calls. Requests holding the old snapshot keep
// their transport and response body; only its background health checks stop.
func (s *DistributedHTTPUpstream) Reload(cfg config.WorkerPoolConfig) error {
	next, err := buildWorkerSnapshot(cfg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("worker pool is closed")
	}
	previous := s.current.Load()
	if next != nil {
		next.start()
	}
	s.current.Store(next)
	if previous != nil {
		previous.retired.Store(true)
		previous.retire()
	}
	return nil
}

func (s *DistributedHTTPUpstream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	previous := s.current.Load()
	if previous != nil {
		previous.retired.Store(true)
		s.current.Store(nil)
		previous.retire()
	}
	return nil
}

func (s *DistributedHTTPUpstream) Validate(cfg config.WorkerPoolConfig) error {
	_, err := buildWorkerSnapshot(cfg)
	return err
}

func buildWorkerSnapshot(cfg config.WorkerPoolConfig) (*workerSnapshot, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.HealthInterval < 0 || cfg.HealthTimeout < 0 || cfg.MaxAttempts < 0 ||
		cfg.CircuitFailureThreshold < 0 || cfg.CircuitOpenTimeout < 0 {
		return nil, errors.New("worker pool requires non-negative timings and attempt limits")
	}
	switch cfg.SelectionPolicy {
	case "", "least_inflight_affinity", "least_inflight", "round_robin":
	default:
		return nil, errors.New("invalid Worker selection policy")
	}
	if cfg.CircuitFailureThreshold == 0 {
		cfg.CircuitFailureThreshold = workerFailureThreshold
	}
	if cfg.CircuitOpenTimeout == 0 {
		cfg.CircuitOpenTimeout = workerCircuitDuration
	}
	snapshot := &workerSnapshot{
		fallbackLocal:  cfg.FallbackLocal,
		maxAttempts:    cfg.MaxAttempts,
		healthInterval: cfg.HealthInterval,
		healthTimeout:  cfg.HealthTimeout,
		drainTimeout:   cfg.DrainTimeout,
		selection:      cfg.SelectionPolicy,
	}
	if snapshot.maxAttempts == 0 {
		snapshot.maxAttempts = 2
	}
	if snapshot.healthInterval == 0 {
		snapshot.healthInterval = 5 * time.Second
	}
	if snapshot.healthTimeout == 0 {
		snapshot.healthTimeout = 2 * time.Second
	}
	seenIDs := make(map[string]bool, len(cfg.Nodes))
	seenAddresses := make(map[string]bool, len(cfg.Nodes))
	var tlsConfig *tls.Config
	for i, node := range cfg.Nodes {
		id := strings.TrimSpace(node.ID)
		address, err := workerEndpointURL(node.URL, cfg.AllowPrivateH2C)
		if err != nil {
			return nil, fmt.Errorf("worker node %d: %w", i+1, err)
		}
		urlKey := address.Scheme + "://" + net.JoinHostPort(strings.ToLower(address.Hostname()), workerPort(address))
		if id == "" || seenIDs[id] || seenAddresses[urlKey] || node.Weight < 0 || node.MaxInFlight < 0 {
			return nil, fmt.Errorf("worker node %d has invalid or duplicate identity, address, or weight", i+1)
		}
		token, err := workerToken(node)
		if err != nil {
			return nil, fmt.Errorf("worker node %d: %w", i+1, err)
		}
		if address.Scheme == "https" && tlsConfig == nil {
			tlsConfig, err = workerClientTLSConfig(cfg)
			if err != nil {
				return nil, err
			}
		}
		seenIDs[id], seenAddresses[urlKey] = true, true
		snapshot.nodes = append(snapshot.nodes, &relayWorker{
			id: id, address: address.String(), token: token, weight: max(1, node.Weight),
			capacity: node.MaxInFlight, failureLimit: cfg.CircuitFailureThreshold, openDuration: cfg.CircuitOpenTimeout,
			client: newWorkerHTTPClient(address.Scheme, tlsConfig),
		})
	}
	return snapshot, nil
}

func workerEndpointURL(value string, allowPrivateH2C bool) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(value))
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" ||
		(u.Path != "" && u.Path != "/") {
		return nil, errors.New("worker URL must be a private HTTP(S) origin without credentials or path")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !allowPrivateH2C) {
		return nil, errors.New("worker URL requires mTLS or explicitly enabled private h2c")
	}
	port, err := strconv.Atoi(workerPort(u))
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("worker URL has an invalid port")
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !privateWorkerIP(ip) {
		return nil, errors.New("worker URL must use a private address")
	}
	u.Path = ""
	return u, nil
}

func workerPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

func workerToken(node config.WorkerNodeConfig) (string, error) {
	if node.SharedToken != "" && node.SharedTokenFile != "" {
		return "", errors.New("set only one worker token source")
	}
	token := strings.TrimSpace(node.SharedToken)
	if node.SharedTokenFile != "" {
		content, err := os.ReadFile(node.SharedTokenFile)
		if err != nil {
			return "", errors.New("cannot read worker token file")
		}
		token = strings.TrimSpace(string(content))
	}
	if len(token) < 32 || strings.IndexFunc(token, func(r rune) bool {
		return r < '!' || r > '~' || r == ','
	}) >= 0 {
		return "", errors.New("a valid worker bearer token is required")
	}
	return token, nil
}

func workerClientTLSConfig(cfg config.WorkerPoolConfig) (*tls.Config, error) {
	ca, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, errors.New("cannot read the worker CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("worker CA file contains no valid certificate")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, errors.New("cannot load the worker client certificate and key")
	}
	return &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}, nil
}

func newWorkerHTTPClient(scheme string, tlsConfig *tls.Config) *http.Client {
	protocols := &http.Protocols{}
	if scheme == "http" {
		protocols.SetUnencryptedHTTP2(true)
	} else {
		protocols.SetHTTP2(true)
	}
	return &http.Client{
		Transport: &http.Transport{
			Protocols:              protocols,
			HTTP2:                  &http.HTTP2Config{SendPingTimeout: 30 * time.Second, PingTimeout: 10 * time.Second},
			DialContext:            dialPrivateWorker,
			TLSClientConfig:        tlsConfig,
			TLSHandshakeTimeout:    5 * time.Second,
			IdleConnTimeout:        90 * time.Second,
			MaxIdleConnsPerHost:    2,
			DisableCompression:     true,
			MaxResponseHeaderBytes: 64 << 10,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func privateWorkerIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.Zone() == "" && (ip.IsPrivate() || ip.IsLoopback() || workerOverlayIPv4.Contains(ip))
}

func dialPrivateWorker(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errWorkerNotConnected
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupNetIP(dialCtx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errWorkerNotConnected
	}
	for _, ip := range ips {
		if !privateWorkerIP(ip) {
			return nil, errWorkerNotConnected
		}
	}
	var dialer net.Dialer
	for _, ip := range ips {
		conn, err := dialer.DialContext(dialCtx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, errWorkerNotConnected
}

func (s *workerSnapshot) start() {
	ctx, stop := context.WithCancel(context.Background())
	s.stop = stop
	var ready sync.WaitGroup
	for _, node := range s.nodes {
		ready.Add(1)
		go func() {
			node.probe(ctx, s.healthTimeout)
			ready.Done()
			ticker := time.NewTicker(s.healthInterval)
			defer ticker.Stop()
			defer node.client.CloseIdleConnections()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					node.probe(ctx, s.healthTimeout)
				}
			}
		}()
	}
	ready.Wait()
}

func (s *workerSnapshot) retire() {
	s.stop()
	for _, node := range s.nodes {
		node.client.CloseIdleConnections()
	}
}

func (n *relayWorker) probe(parent context.Context, timeout time.Duration) {
	start := time.Now()
	n.mu.Lock()
	open := n.openUntil.After(start)
	n.mu.Unlock()
	if open {
		return
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	health, err := n.readHealth(ctx)
	ok := err == nil && health.Status == workerproto.HealthOK
	if parent.Err() != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// A probe that started before the circuit opened must not re-enable it.
	if n.openUntil.After(start) {
		return
	}
	n.healthy = ok
	if ok {
		n.remoteLoad = health.InFlight
		if !n.openUntil.IsZero() {
			n.failures = 0
			n.openUntil = time.Time{}
		}
	}
}

func decodeWorkerJSON(reader io.Reader, dst any) bool {
	body, err := io.ReadAll(io.LimitReader(reader, workerHealthBodyLimit+1))
	return err == nil && len(body) <= workerHealthBodyLimit && json.Unmarshal(body, dst) == nil
}

func (n *relayWorker) failure() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.openUntil.IsZero() {
		return
	}
	n.failures++
	if n.failures >= n.failureLimit {
		n.healthy = false
		n.openUntil = time.Now().Add(n.openDuration)
	}
}

func (n *relayWorker) success() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.openUntil.IsZero() {
		n.failures = 0
	}
}

func (n *relayWorker) load() (float64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	inFlight := max(n.localLoad.Load(), int64(n.remoteLoad))
	return float64(inFlight) / float64(n.weight),
		n.healthy && n.openUntil.IsZero() && (n.capacity == 0 || inFlight < int64(n.capacity))
}

func (s *workerSnapshot) selectWorker(accountID int64, attempted map[*relayWorker]bool) *relayWorker {
	if len(s.nodes) == 0 {
		return nil
	}
	if s.selection == "round_robin" {
		start := int((s.roundRobin.Add(1) - 1) % uint64(len(s.nodes)))
		for i := range len(s.nodes) {
			node := s.nodes[(start+i)%len(s.nodes)]
			if _, eligible := node.load(); eligible && !attempted[node] {
				return node
			}
		}
		return nil
	}
	var preferred, best *relayWorker
	var preference uint64
	var preferredLoad, bestLoad float64
	account := strconv.FormatInt(accountID, 10)
	for _, node := range s.nodes {
		load, eligible := node.load()
		if attempted[node] || !eligible {
			continue
		}
		digest := sha256.Sum256([]byte(account + ":" + node.id))
		score := binary.BigEndian.Uint64(digest[:8])
		if preferred == nil || score > preference {
			preferred, preferredLoad, preference = node, load, score
		}
		if best == nil || load < bestLoad {
			best, bestLoad = node, load
		}
	}
	if s.selection == "least_inflight" || preferredLoad > bestLoad+2 {
		return best
	}
	return preferred
}

func (n *relayWorker) readHealth(ctx context.Context) (workerproto.Health, error) {
	var health workerproto.Health
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.address+workerproto.HealthPath, nil)
	if err != nil {
		return health, service.ErrWorkerClusterUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+n.token)
	resp, err := n.client.Do(req)
	if err != nil {
		return health, service.ErrWorkerClusterUnavailable
	}
	defer resp.Body.Close()
	if !decodeWorkerJSON(resp.Body, &health) || health.NodeID != n.id ||
		health.InFlight < 0 || health.MaxInFlight <= 0 || health.InFlight > health.MaxInFlight ||
		!(resp.StatusCode == http.StatusOK && health.Status == workerproto.HealthOK ||
			resp.StatusCode == http.StatusServiceUnavailable && health.Status == workerproto.HealthDraining) {
		return health, service.ErrWorkerClusterUnavailable
	}
	return health, nil
}

func newWorkerAdminClient(cfg config.WorkerPoolConfig, node config.WorkerNodeConfig) (*relayWorker, error) {
	address, err := workerEndpointURL(node.URL, cfg.AllowPrivateH2C)
	if err != nil {
		return nil, err
	}
	token, err := workerToken(node)
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if address.Scheme == "https" {
		tlsConfig, err = workerClientTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
	}
	return &relayWorker{id: node.ID, address: address.String(), token: token, client: newWorkerHTTPClient(address.Scheme, tlsConfig)}, nil
}

func (s *DistributedHTTPUpstream) ProbeWorker(ctx context.Context, cfg config.WorkerPoolConfig, node config.WorkerNodeConfig) (service.WorkerProbe, error) {
	peer, err := newWorkerAdminClient(cfg, node)
	if err != nil {
		return service.WorkerProbe{}, service.ErrWorkerClusterInvalid
	}
	defer peer.client.CloseIdleConnections()
	timeout := cfg.HealthTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	health, err := peer.readHealth(ctx)
	if err != nil {
		return service.WorkerProbe{}, err
	}
	return service.WorkerProbe{Status: string(health.Status), NodeID: health.NodeID, Version: health.Version, InFlight: health.InFlight,
		MaxInFlight: health.MaxInFlight, MemAllocMB: health.MemAllocMB, RTTMillis: time.Since(start).Milliseconds()}, nil
}

func (s *DistributedHTTPUpstream) DrainWorker(ctx context.Context, cfg config.WorkerPoolConfig, node config.WorkerNodeConfig) error {
	peer, err := newWorkerAdminClient(cfg, node)
	if err != nil {
		return service.ErrWorkerClusterInvalid
	}
	defer peer.client.CloseIdleConnections()
	timeout := cfg.HealthTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.address+workerproto.DrainPath, nil)
	if err != nil {
		return service.ErrWorkerClusterUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+peer.token)
	resp, err := peer.client.Do(req)
	if err != nil {
		return service.ErrWorkerClusterUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return service.ErrWorkerClusterUnavailable
	}
	return nil
}
