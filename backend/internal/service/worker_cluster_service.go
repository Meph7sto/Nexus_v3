package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	workerConfigPoll    = 30 * time.Second
	entryHeartbeatEvery = 10 * time.Second
	entryHeartbeatTTL   = 30 * time.Second
	workerDrainPoll     = 5 * time.Second
	workerStoreTimeout  = 5 * time.Second
)

type WorkerClusterService struct {
	store         WorkerClusterStore
	coord         WorkerClusterCoordination
	transport     WorkerClusterRuntime
	bootstrap     config.WorkerPoolConfig
	bindings      map[string]config.WorkerBindingConfig
	encryptor     SecretEncryptor
	keyConfigured bool
	status        func() EntryProcessStatus
	nodeID        string
	name          string
	role          string
	instanceID    string
	pollEvery     time.Duration

	lifeMu sync.Mutex
	stop   context.CancelFunc
	done   chan struct{}
	closed atomic.Bool

	heartbeatMu    sync.Mutex
	applyMu        sync.Mutex
	appliedVersion int64
	failedVersion  int64
	applyError     string
	healthTimeout  time.Duration
	registered     bool
}

func NewWorkerClusterService(store WorkerClusterStore, coord WorkerClusterCoordination, transport WorkerClusterRuntime, cfg *config.Config, status func() EntryProcessStatus, encryptor SecretEncryptor) (*WorkerClusterService, error) {
	if store == nil || coord == nil || transport == nil || cfg == nil || !cfg.EntryNode.ClusterEnabled {
		return nil, ErrWorkerClusterUnavailable
	}
	entry := cfg.EntryNode
	nodeID, name, role := strings.TrimSpace(entry.ID), strings.TrimSpace(entry.Name), strings.TrimSpace(entry.Role)
	if !clusterIdentifier(nodeID) {
		return nil, fmt.Errorf("%w: invalid Entry node ID", ErrWorkerClusterInvalid)
	}
	if name == "" {
		name = nodeID
	}
	if role == "" {
		role = "all"
	}
	if role != "all" && role != "gateway_only" && role != "control_only" {
		return nil, fmt.Errorf("%w: invalid Entry role", ErrWorkerClusterInvalid)
	}
	s := &WorkerClusterService{
		store: store, coord: coord, transport: transport, bootstrap: cfg.WorkerPool,
		bindings: make(map[string]config.WorkerBindingConfig), status: status, encryptor: encryptor,
		keyConfigured: cfg.Totp.EncryptionKeyConfigured,
		nodeID:        nodeID, name: name, role: role, instanceID: nodeID + "-" + rand.Text(),
		pollEvery: workerConfigPoll,
	}
	if cfg.Gateway.StreamDataIntervalTimeout > 0 {
		s.bootstrap.DrainTimeout = time.Duration(cfg.Gateway.StreamDataIntervalTimeout) * time.Second
	}
	for _, node := range cfg.WorkerPool.Nodes {
		if err := s.addBinding(config.WorkerBindingConfig{
			Ref: node.ID, NodeID: node.ID, URL: node.URL,
			SharedToken: node.SharedToken, SharedTokenFile: node.SharedTokenFile,
		}); err != nil {
			return nil, err
		}
	}
	for _, binding := range cfg.WorkerPool.Bindings {
		if err := s.addBinding(binding); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *WorkerClusterService) Start(ctx context.Context) error {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.closed.Load() {
		return ErrWorkerClusterUnavailable
	}
	if s.stop != nil {
		return nil
	}
	runCtx, stop := context.WithCancel(ctx)
	ch := s.coord.SubscribeWorkerChanges(runCtx)
	initialCtx, cancel := context.WithTimeout(ctx, workerStoreTimeout)
	defer cancel()
	err := s.Reconcile(initialCtx)
	s.stop, s.done = stop, make(chan struct{})
	go s.run(runCtx, ch, s.done)
	return err
}

func (s *WorkerClusterService) Close() {
	s.lifeMu.Lock()
	if s.closed.Swap(true) {
		s.lifeMu.Unlock()
		return
	}
	stop, done := s.stop, s.done
	s.lifeMu.Unlock()
	if stop == nil {
		return
	}
	stop()
	<-done
	s.retireEntry()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	_ = s.coord.DeleteEntryHeartbeat(ctx, s.instanceID)
}

func (s *WorkerClusterService) retireEntry() {
	if s.role == config.EntryRoleControlOnly {
		return
	}
	// A missing heartbeat is not a retirement. Keep membership if we could
	// not stop routing new Worker requests, including on startup failure.
	s.applyMu.Lock()
	if !s.registered {
		s.applyMu.Unlock()
		return
	}
	err := s.transport.Reload(config.WorkerPoolConfig{})
	s.applyMu.Unlock()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), workerStoreTimeout)
	defer cancel()
	_ = s.store.SetEntryMembership(ctx, EntryMembership{InstanceID: s.instanceID, Retired: true})
}

func (s *WorkerClusterService) initialize(ctx context.Context) error {
	if _, err := s.store.Load(ctx); !errors.Is(err, ErrWorkerClusterUninitialized) {
		return err
	}
	initial := snapshotFromBootstrap(s.bootstrap)
	if err := s.validateSnapshot(initial); err != nil {
		return err
	}
	if err := s.store.Save(ctx, 0, initial); err != nil && !errors.Is(err, ErrWorkerClusterConflict) {
		return err
	}
	return nil
}

// Held under applyMu, so no snapshot can begin serving before this process has
// registered on the same database that publishes Worker membership changes.
func (s *WorkerClusterService) ensureRegistered(ctx context.Context) error {
	if s.registered {
		return nil
	}
	if err := s.initialize(ctx); err != nil {
		return err
	}
	if s.role != config.EntryRoleControlOnly {
		if err := s.store.SetEntryMembership(ctx, EntryMembership{InstanceID: s.instanceID}); err != nil {
			return err
		}
	}
	s.registered = true
	return nil
}

func (s *WorkerClusterService) run(ctx context.Context, updates <-chan int64, done chan struct{}) {
	defer close(done)
	poll := time.NewTicker(s.pollEvery)
	heartbeat := time.NewTicker(entryHeartbeatEvery)
	defer poll.Stop()
	defer heartbeat.Stop()
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		ticker := time.NewTicker(workerDrainPoll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.advanceDrains(ctx)
			}
		}
	}()
	defer func() { <-drainDone }()
	for {
		select {
		case <-ctx.Done():
			return
		case version, open := <-updates:
			if !open {
				updates = nil
			} else if version > s.applied() {
				s.runBounded(ctx, s.Reconcile)
			}
		case <-poll.C:
			s.runBounded(ctx, s.Reconcile)
		case <-heartbeat.C:
			s.runBounded(ctx, s.publishHeartbeat)
		}
	}
}

func (s *WorkerClusterService) runBounded(ctx context.Context, fn func(context.Context) error) {
	bounded, cancel := context.WithTimeout(ctx, workerStoreTimeout)
	defer cancel()
	_ = fn(bounded)
}

func (s *WorkerClusterService) applied() int64 {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.appliedVersion
}

// Reconcile reads one consistent database snapshot and only publishes a newer
// version after the transport has accepted it.
func (s *WorkerClusterService) Reconcile(ctx context.Context) error {
	s.applyMu.Lock()
	if s.closed.Load() {
		s.applyMu.Unlock()
		return ErrWorkerClusterUnavailable
	}
	var desired WorkerClusterSnapshot
	err := s.ensureRegistered(ctx)
	if err == nil {
		desired, err = s.store.Load(ctx)
	}
	if err != nil && s.failedVersion == 0 {
		s.applyError = "Worker configuration could not be loaded"
	}
	if err == nil && desired.Version > s.appliedVersion {
		var cfg config.WorkerPoolConfig
		cfg, err = s.runtimeConfig(desired)
		if err == nil {
			err = s.transport.Reload(cfg)
		}
		if err == nil {
			s.appliedVersion, s.failedVersion, s.applyError = desired.Version, 0, ""
			s.healthTimeout = cfg.HealthTimeout
		} else if desired.Version >= s.failedVersion {
			s.failedVersion = desired.Version
			s.applyError = "Worker configuration could not be applied"
			err = ErrWorkerClusterInvalid
		}
	}
	if err == nil && s.failedVersion == 0 && desired.Version == s.appliedVersion {
		s.applyError = ""
	}
	s.applyMu.Unlock()
	heartbeatErr := s.publishHeartbeat(ctx)
	return errors.Join(err, heartbeatErr)
}

func (s *WorkerClusterService) workerProbeConfig() config.WorkerPoolConfig {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	cfg := s.bootstrap
	if s.healthTimeout > 0 {
		cfg.HealthTimeout = s.healthTimeout
	}
	return cfg
}

func (s *WorkerClusterService) publishHeartbeat(ctx context.Context) error {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	if s.closed.Load() {
		return ErrWorkerClusterUnavailable
	}
	s.applyMu.Lock()
	applied, applyError := s.appliedVersion, s.applyError
	s.applyMu.Unlock()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var status EntryProcessStatus
	if s.status != nil {
		status = s.status()
	}
	return s.coord.PutEntryHeartbeat(ctx, EntryHeartbeat{
		InstanceID: s.instanceID, NodeID: s.nodeID, Name: s.name, Role: s.role, Version: status.Version,
		AppliedVersion: applied, ApplyError: applyError, MemAllocMB: float64(mem.Alloc) / (1 << 20),
		ActiveConnections: status.ActiveConnections, SharedTasksRunning: status.SharedTasksRunning, UpdatedAt: time.Now().UTC(),
	}, entryHeartbeatTTL)
}

func (s *WorkerClusterService) View(ctx context.Context) (WorkerClusterView, error) {
	desired, err := s.store.Load(ctx)
	if err != nil {
		return WorkerClusterView{}, ErrWorkerClusterUnavailable
	}
	entries, err := s.coord.ListEntryHeartbeats(ctx)
	if err != nil {
		return WorkerClusterView{}, ErrWorkerClusterUnavailable
	}
	slices.SortFunc(entries, func(a, b EntryHeartbeat) int { return strings.Compare(a.InstanceID, b.InstanceID) })
	bindings := make([]WorkerBindingView, 0, len(s.bindings))
	for _, binding := range s.bindings {
		bindings = append(bindings, WorkerBindingView{Ref: binding.Ref, NodeID: binding.NodeID, URL: binding.URL})
	}
	slices.SortFunc(bindings, func(a, b WorkerBindingView) int { return strings.Compare(a.Ref, b.Ref) })
	return WorkerClusterView{Desired: s.publicSnapshot(desired), Entries: entries, Bindings: bindings}, nil
}

func (s *WorkerClusterService) mutate(ctx context.Context, expected int64, apply func(*WorkerClusterSnapshot) error) (WorkerClusterSnapshot, error) {
	next, err := s.store.Load(ctx)
	if err != nil {
		return WorkerClusterSnapshot{}, ErrWorkerClusterUnavailable
	}
	if expected != next.Version || expected == math.MaxInt64 {
		return WorkerClusterSnapshot{}, ErrWorkerClusterConflict
	}
	next.Version++
	if err := apply(&next); err != nil {
		return WorkerClusterSnapshot{}, err
	}
	if err := s.validateSnapshot(next); err != nil {
		return WorkerClusterSnapshot{}, err
	}
	if err := s.store.Save(ctx, expected, next); err != nil {
		if errors.Is(err, ErrWorkerClusterConflict) {
			return WorkerClusterSnapshot{}, ErrWorkerClusterConflict
		}
		return WorkerClusterSnapshot{}, ErrWorkerClusterUnavailable
	}
	// Post-commit notification is an optimization. The committed version still
	// converges through polling when Redis publish fails or a subscriber misses it.
	s.runBounded(ctx, func(ctx context.Context) error {
		return s.coord.PublishWorkerChange(ctx, next.Version)
	})
	s.runBounded(ctx, s.Reconcile)
	return s.publicSnapshot(next), nil
}

func (s *WorkerClusterService) publicSnapshot(snapshot WorkerClusterSnapshot) WorkerClusterSnapshot {
	snapshot.Workers = slices.Clone(snapshot.Workers)
	for i := range snapshot.Workers {
		node := &snapshot.Workers[i]
		if node.EncryptedToken != "" {
			node.HasToken, node.CredentialSource = true, "managed"
			node.EncryptedToken = ""
		} else {
			_, node.HasToken = s.bindings[node.SecretRef]
			node.CredentialSource = "binding"
		}
	}
	return snapshot
}

func (s *WorkerClusterService) validateSnapshot(snapshot WorkerClusterSnapshot) error {
	cfg, err := s.runtimeConfig(snapshot)
	if err != nil {
		return err
	}
	if err := s.transport.Validate(cfg); err != nil {
		return fmt.Errorf("%w: Worker transport settings could not be validated", ErrWorkerClusterInvalid)
	}
	return nil
}
