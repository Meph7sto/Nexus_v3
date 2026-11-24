package service

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func clusterIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func canonicalWorkerBindingURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || len(raw) > 350 || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" ||
		parsed.RawPath != "" || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.ForceQuery || parsed.RawQuery != "" || strings.Contains(raw, "#") ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", false
	}
	parsed.Path = ""
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String(), true
}

func (s *WorkerClusterService) addBinding(binding config.WorkerBindingConfig) error {
	binding.Ref = strings.TrimSpace(binding.Ref)
	binding.NodeID = strings.TrimSpace(binding.NodeID)
	address, valid := canonicalWorkerBindingURL(binding.URL)
	if !valid || !clusterIdentifier(binding.Ref) || !clusterIdentifier(binding.NodeID) ||
		(binding.SharedToken == "") == (binding.SharedTokenFile == "") {
		return fmt.Errorf("%w: invalid provisioned Worker binding", ErrWorkerClusterInvalid)
	}
	if _, exists := s.bindings[binding.Ref]; exists {
		return fmt.Errorf("%w: duplicate provisioned Worker binding", ErrWorkerClusterInvalid)
	}
	binding.URL = address
	s.bindings[binding.Ref] = binding
	return nil
}

func snapshotFromBootstrap(cfg config.WorkerPoolConfig) WorkerClusterSnapshot {
	snapshot := WorkerClusterSnapshot{Version: 1, Workers: make([]WorkerNode, 0, len(cfg.Nodes))}
	for _, node := range cfg.Nodes {
		address, _ := canonicalWorkerBindingURL(node.URL)
		snapshot.Workers = append(snapshot.Workers, WorkerNode{
			NodeID: node.ID, Name: node.ID, URL: address, UseH2C: strings.HasPrefix(address, "http://"),
			Weight: max(1, node.Weight), MaxInFlight: node.MaxInFlight,
			Enabled: true, SecretRef: node.ID, Tags: []string{}, DrainEntries: []string{},
		})
	}
	policy := WorkerPoolPolicy{
		Enabled: cfg.Enabled, FallbackLocal: cfg.FallbackLocal, SelectionPolicy: cfg.SelectionPolicy,
		HealthIntervalSeconds: int(cfg.HealthInterval / time.Second), HealthTimeoutSeconds: int(cfg.HealthTimeout / time.Second),
		CircuitFailureThreshold: cfg.CircuitFailureThreshold, CircuitOpenSeconds: int(cfg.CircuitOpenTimeout / time.Second),
		MaxAttempts: cfg.MaxAttempts,
	}
	if policy.SelectionPolicy == "" {
		policy.SelectionPolicy = "least_inflight_affinity"
	}
	if policy.HealthIntervalSeconds == 0 {
		policy.HealthIntervalSeconds = 5
	}
	if policy.HealthTimeoutSeconds == 0 {
		policy.HealthTimeoutSeconds = 2
	}
	if policy.CircuitFailureThreshold == 0 {
		policy.CircuitFailureThreshold = 3
	}
	if policy.CircuitOpenSeconds == 0 {
		policy.CircuitOpenSeconds = 15
	}
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = 2
	}
	snapshot.Policy = policy
	return snapshot
}

func (s *WorkerClusterService) runtimeConfig(snapshot WorkerClusterSnapshot) (config.WorkerPoolConfig, error) {
	cfg := s.bootstrap
	cfg.Nodes, cfg.Bindings = nil, nil
	p := snapshot.Policy
	switch p.SelectionPolicy {
	case "least_inflight_affinity", "least_inflight", "round_robin":
	default:
		return cfg, fmt.Errorf("%w: unknown Worker selection policy", ErrWorkerClusterInvalid)
	}
	if p.HealthIntervalSeconds < 1 || p.HealthIntervalSeconds > 300 ||
		p.HealthTimeoutSeconds < 1 || p.HealthTimeoutSeconds > 60 ||
		p.CircuitFailureThreshold < 1 || p.CircuitFailureThreshold > 100 ||
		p.CircuitOpenSeconds < 1 || p.CircuitOpenSeconds > 3600 ||
		p.MaxAttempts < 1 || p.MaxAttempts > 10 || len(snapshot.Workers) > 256 {
		return cfg, fmt.Errorf("%w: Worker policy outside supported bounds", ErrWorkerClusterInvalid)
	}
	cfg.Enabled, cfg.FallbackLocal, cfg.SelectionPolicy = p.Enabled, p.FallbackLocal, p.SelectionPolicy
	cfg.HealthInterval = time.Duration(p.HealthIntervalSeconds) * time.Second
	cfg.HealthTimeout = time.Duration(p.HealthTimeoutSeconds) * time.Second
	cfg.CircuitFailureThreshold = p.CircuitFailureThreshold
	cfg.CircuitOpenTimeout = time.Duration(p.CircuitOpenSeconds) * time.Second
	cfg.MaxAttempts = p.MaxAttempts
	nodes := make(map[string]bool, len(snapshot.Workers))
	for _, node := range snapshot.Workers {
		if nodes[node.NodeID] || node.Weight < 1 || node.Weight > 10000 ||
			node.MaxInFlight < 0 || node.MaxInFlight > 65535 || !validWorkerNodeText(node) ||
			node.DrainVersion < 0 || node.DrainVersion > snapshot.Version || node.DrainVersion > 0 && node.Enabled {
			return cfg, fmt.Errorf("%w: invalid or duplicate Worker node", ErrWorkerClusterInvalid)
		}
		nodes[node.NodeID] = true
		resolved, err := s.resolveWorker(node)
		if err != nil {
			return cfg, err
		}
		if node.Enabled {
			cfg.Nodes = append(cfg.Nodes, resolved)
		}
	}
	return cfg, nil
}

func validWorkerNodeText(node WorkerNode) bool {
	if !clusterIdentifier(node.NodeID) || strings.TrimSpace(node.Name) == "" ||
		utf8.RuneCountInString(node.Name) > 120 || utf8.RuneCountInString(node.Note) > 1000 ||
		strings.ContainsRune(node.Name, 0) || strings.ContainsRune(node.Note, 0) || len(node.Tags) > 32 {
		return false
	}
	for _, tag := range node.Tags {
		if strings.TrimSpace(tag) == "" || utf8.RuneCountInString(tag) > 64 || strings.ContainsRune(tag, 0) {
			return false
		}
	}
	return true
}

func (s *WorkerClusterService) resolveWorker(node WorkerNode) (config.WorkerNodeConfig, error) {
	binding, exists := s.bindings[node.SecretRef]
	address, valid := canonicalWorkerBindingURL(node.URL)
	if !exists || !valid || binding.NodeID != node.NodeID || binding.URL != address ||
		node.UseH2C != strings.HasPrefix(address, "http://") || node.UseH2C && !s.bootstrap.AllowPrivateH2C {
		return config.WorkerNodeConfig{}, fmt.Errorf("%w: Worker does not match a provisioned endpoint binding", ErrWorkerClusterInvalid)
	}
	return config.WorkerNodeConfig{
		ID: node.NodeID, URL: address, Weight: node.Weight, MaxInFlight: node.MaxInFlight,
		SharedToken: binding.SharedToken, SharedTokenFile: binding.SharedTokenFile,
	}, nil
}

func (s *WorkerClusterService) UpsertWorker(ctx context.Context, node WorkerNode, expectedVersion int64) (WorkerClusterSnapshot, error) {
	node.NodeID, node.Name, node.SecretRef = strings.TrimSpace(node.NodeID), strings.TrimSpace(node.Name), strings.TrimSpace(node.SecretRef)
	node.URL, _ = canonicalWorkerBindingURL(node.URL)
	node.DrainVersion, node.DrainEntries = 0, []string{}
	if node.Tags == nil {
		node.Tags = []string{}
	}
	return s.mutate(ctx, expectedVersion, func(snapshot *WorkerClusterSnapshot) error {
		for i, existing := range snapshot.Workers {
			if existing.NodeID == node.NodeID {
				if existing.DrainVersion != 0 {
					if node.Enabled || node.URL != existing.URL || node.SecretRef != existing.SecretRef {
						return fmt.Errorf("%w: cannot change routing for a draining Worker", ErrWorkerClusterConflict)
					}
					node.DrainVersion, node.DrainEntries = existing.DrainVersion, existing.DrainEntries
				}
				snapshot.Workers[i] = node
				return nil
			}
		}
		snapshot.Workers = append(snapshot.Workers, node)
		return nil
	})
}

func (s *WorkerClusterService) DeleteWorker(ctx context.Context, nodeID string, expectedVersion int64) (WorkerClusterSnapshot, error) {
	return s.deleteWorker(ctx, nodeID, expectedVersion, false)
}

// ForgetOfflineWorker is an operator attestation, not a successful drain.
func (s *WorkerClusterService) ForgetOfflineWorker(ctx context.Context, nodeID string, expectedVersion int64) (WorkerClusterSnapshot, error) {
	return s.deleteWorker(ctx, nodeID, expectedVersion, true)
}

func (s *WorkerClusterService) deleteWorker(ctx context.Context, nodeID string, expectedVersion int64, fenced bool) (WorkerClusterSnapshot, error) {
	return s.mutate(ctx, expectedVersion, func(snapshot *WorkerClusterSnapshot) error {
		for i, node := range snapshot.Workers {
			if node.NodeID != nodeID {
				continue
			}
			if node.Enabled || (fenced && node.DrainVersion == 0) {
				return fmt.Errorf("%w: drain the Worker before deleting it", ErrWorkerClusterConflict)
			}
			if node.DrainVersion != 0 {
				status, err := s.drainStatus(ctx, node)
				if err != nil {
					return err
				}
				if fenced {
					if len(status.PendingEntries) != 0 || status.Health != nil {
						return ErrWorkerClusterConflict
					}
				} else if !status.Complete {
					return fmt.Errorf("%w: Worker drain has not completed", ErrWorkerClusterConflict)
				}
			}
			snapshot.Workers = slices.Delete(snapshot.Workers, i, i+1)
			return nil
		}
		return ErrWorkerNodeNotFound
	})
}

// The caller must independently fence the process. Redis absence alone never
// retires a member; a positive heartbeat vetoes even the manual operation.
func (s *WorkerClusterService) RetireOfflineEntry(ctx context.Context, id string) error {
	if id == "" || id == s.instanceID {
		return ErrWorkerClusterConflict
	}
	members, err := s.store.ListEntryMemberships(ctx)
	if err != nil {
		return ErrWorkerClusterUnavailable
	}
	if !slices.ContainsFunc(members, func(m EntryMembership) bool { return m.InstanceID == id && !m.Retired }) {
		return ErrWorkerClusterConflict
	}
	entries, err := s.coord.ListEntryHeartbeats(ctx)
	if err != nil {
		return ErrWorkerClusterUnavailable
	}
	if slices.ContainsFunc(entries, func(e EntryHeartbeat) bool { return e.InstanceID == id }) {
		return ErrWorkerClusterConflict
	}
	if err := s.store.SetEntryMembership(ctx, EntryMembership{InstanceID: id, Retired: true}); err != nil {
		return ErrWorkerClusterUnavailable
	}
	return nil
}

func (s *WorkerClusterService) UpdatePolicy(ctx context.Context, policy WorkerPoolPolicy, expectedVersion int64) (WorkerClusterSnapshot, error) {
	return s.mutate(ctx, expectedVersion, func(snapshot *WorkerClusterSnapshot) error {
		snapshot.Policy = policy
		return nil
	})
}

func (s *WorkerClusterService) loadWorker(ctx context.Context, nodeID string) (WorkerNode, error) {
	snapshot, err := s.store.Load(ctx)
	if err != nil {
		return WorkerNode{}, ErrWorkerClusterUnavailable
	}
	for _, node := range snapshot.Workers {
		if node.NodeID == nodeID {
			return node, nil
		}
	}
	return WorkerNode{}, ErrWorkerNodeNotFound
}

func (s *WorkerClusterService) ProbeWorker(ctx context.Context, nodeID string) (WorkerProbe, error) {
	node, err := s.loadWorker(ctx, nodeID)
	if err != nil {
		return WorkerProbe{}, err
	}
	return s.probe(ctx, node)
}

func (s *WorkerClusterService) probe(ctx context.Context, node WorkerNode) (WorkerProbe, error) {
	cfg, err := s.resolveWorker(node)
	if err != nil {
		return WorkerProbe{}, err
	}
	health, err := s.transport.ProbeWorker(ctx, s.workerProbeConfig(), cfg)
	if err != nil {
		return WorkerProbe{}, ErrWorkerClusterUnavailable
	}
	return health, nil
}

func (s *WorkerClusterService) JoinTemplate(ctx context.Context, nodeID string) (string, error) {
	node, err := s.loadWorker(ctx, nodeID)
	if err != nil {
		return "", err
	}
	if _, err := s.resolveWorker(node); err != nil {
		return "", err
	}
	return fmt.Sprintf("# Provision WORKER_ENV_FILE, WORKER_SECRETS_DIR and private WORKER_BIND_IP separately.\n"+
		"WORKER_NODE_ID='%s' docker compose -f deploy/worker/compose.yaml up -d --build worker", node.NodeID), nil
}

func (s *WorkerClusterService) DrainWorker(ctx context.Context, nodeID string, expectedVersion int64) (WorkerClusterSnapshot, error) {
	return s.mutate(ctx, expectedVersion, func(snapshot *WorkerClusterSnapshot) error {
		for i, node := range snapshot.Workers {
			if node.NodeID != nodeID {
				continue
			}
			if node.DrainVersion != 0 {
				return fmt.Errorf("%w: Worker is already draining", ErrWorkerClusterConflict)
			}
			members, err := s.store.ListEntryMemberships(ctx)
			if err != nil {
				return ErrWorkerClusterUnavailable
			}
			for _, member := range members {
				if !member.Retired {
					node.DrainEntries = append(node.DrainEntries, member.InstanceID)
				}
			}
			slices.Sort(node.DrainEntries)
			if len(node.DrainEntries) == 0 {
				return fmt.Errorf("%w: no registered traffic Entries", ErrWorkerClusterUnavailable)
			}
			node.Enabled, node.DrainVersion = false, snapshot.Version
			snapshot.Workers[i] = node
			return nil
		}
		return ErrWorkerNodeNotFound
	})
}

func trafficEntry(entry EntryHeartbeat) bool {
	return entry.Role == "all" || entry.Role == "gateway_only"
}

func pendingDrainEntries(node WorkerNode, members []EntryMembership, entries []EntryHeartbeat) (pending, uncertain []string) {
	want := make(map[string]bool, len(node.DrainEntries)+len(members))
	for _, instanceID := range node.DrainEntries {
		want[instanceID] = true
	}
	for _, member := range members {
		if member.Retired {
			delete(want, member.InstanceID)
		} else {
			want[member.InstanceID] = true
		}
	}
	acks := make(map[string]int64, len(entries))
	for _, entry := range entries {
		if trafficEntry(entry) {
			acks[entry.InstanceID] = entry.AppliedVersion
		}
	}
	pending, uncertain = []string{}, []string{}
	for instanceID := range want {
		version, seen := acks[instanceID]
		if !seen || version < node.DrainVersion {
			pending = append(pending, instanceID)
		}
		if !seen {
			uncertain = append(uncertain, instanceID)
		}
	}
	slices.Sort(pending)
	slices.Sort(uncertain)
	return pending, uncertain
}

func (s *WorkerClusterService) DrainStatus(ctx context.Context, nodeID string) (WorkerDrainStatus, error) {
	node, err := s.loadWorker(ctx, nodeID)
	if err != nil {
		return WorkerDrainStatus{}, err
	}
	return s.drainStatus(ctx, node)
}

func (s *WorkerClusterService) drainStatus(ctx context.Context, node WorkerNode) (WorkerDrainStatus, error) {
	status := WorkerDrainStatus{Version: node.DrainVersion, PendingEntries: []string{}, UncertainEntries: []string{}}
	if node.DrainVersion == 0 {
		return status, nil
	}
	entries, err := s.coord.ListEntryHeartbeats(ctx)
	if err != nil {
		return status, ErrWorkerClusterUnavailable
	}
	members, err := s.store.ListEntryMemberships(ctx)
	if err != nil {
		return status, ErrWorkerClusterUnavailable
	}
	status.PendingEntries, status.UncertainEntries = pendingDrainEntries(node, members, entries)
	health, err := s.probe(ctx, node)
	if err == nil {
		status.Health = &health
		status.Complete = len(status.PendingEntries) == 0 && health.Status == "draining" && health.InFlight == 0
	}
	return status, nil
}

func (s *WorkerClusterService) advanceDrains(ctx context.Context) {
	if s.role == config.EntryRoleGatewayOnly {
		return
	}
	s.runBounded(ctx, func(ctx context.Context) error {
		snapshot, err := s.store.Load(ctx)
		if err != nil || s.applied() < snapshot.Version {
			return err
		}
		entries, err := s.coord.ListEntryHeartbeats(ctx)
		if err != nil {
			return err
		}
		members, err := s.store.ListEntryMemberships(ctx)
		if err != nil {
			return err
		}
		for _, node := range snapshot.Workers {
			pending, _ := pendingDrainEntries(node, members, entries)
			if node.DrainVersion == 0 || len(pending) != 0 {
				continue
			}
			health, err := s.probe(ctx, node)
			if err != nil || health.Status != "ok" {
				continue
			}
			resolved, err := s.resolveWorker(node)
			if err == nil {
				_ = s.transport.DrainWorker(ctx, s.workerProbeConfig(), resolved)
			}
		}
		return nil
	})
}
