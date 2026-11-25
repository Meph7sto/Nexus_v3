package service

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

var (
	ErrWorkerClusterUninitialized  = errors.New("worker cluster configuration has not been initialized")
	ErrWorkerClusterConflict       = errors.New("worker cluster configuration changed; reload before editing")
	ErrWorkerClusterInvalid        = errors.New("invalid worker cluster configuration")
	ErrWorkerClusterUnavailable    = errors.New("worker cluster is unavailable")
	ErrWorkerNodeNotFound          = errors.New("worker node not found")
	ErrWorkerTokenKeyNotConfigured = errors.New("a fixed TOTP_ENCRYPTION_KEY is required for managed Workers")
)

type WorkerNode struct {
	NodeID           string   `json:"node_id"`
	Name             string   `json:"name"`
	URL              string   `json:"url"`
	UseH2C           bool     `json:"use_h2c"`
	Weight           int      `json:"weight"`
	MaxInFlight      int      `json:"max_inflight"`
	Tags             []string `json:"tags"`
	Note             string   `json:"note"`
	Enabled          bool     `json:"enabled"`
	SecretRef        string   `json:"secret_ref"`
	DrainVersion     int64    `json:"drain_version"`
	DrainEntries     []string `json:"drain_entries"`
	EncryptedToken   string   `json:"-"`
	HasToken         bool     `json:"has_token"`
	CredentialSource string   `json:"credential_source"`
}

type WorkerPoolPolicy struct {
	Enabled                 bool   `json:"enabled"`
	SelectionPolicy         string `json:"selection_policy"`
	HealthIntervalSeconds   int    `json:"health_interval_seconds"`
	HealthTimeoutSeconds    int    `json:"health_timeout_seconds"`
	CircuitFailureThreshold int    `json:"circuit_failure_threshold"`
	CircuitOpenSeconds      int    `json:"circuit_open_seconds"`
	MaxAttempts             int    `json:"max_attempts"`
	FallbackLocal           bool   `json:"fallback_local"`
}

type WorkerClusterSnapshot struct {
	Version int64            `json:"version"`
	Workers []WorkerNode     `json:"workers"`
	Policy  WorkerPoolPolicy `json:"policy"`
}

type WorkerClusterView struct {
	Desired  WorkerClusterSnapshot `json:"desired"`
	Entries  []EntryHeartbeat      `json:"entries"`
	Bindings []WorkerBindingView   `json:"bindings"`
}

type WorkerBindingView struct {
	Ref    string `json:"ref"`
	NodeID string `json:"node_id"`
	URL    string `json:"url"`
}

type EntryProcessStatus struct {
	Version            string
	ActiveConnections  int
	SharedTasksRunning bool
}

type EntryHeartbeat struct {
	InstanceID         string    `json:"instance_id"`
	NodeID             string    `json:"node_id"`
	Name               string    `json:"name"`
	Role               string    `json:"role"`
	Version            string    `json:"version"`
	AppliedVersion     int64     `json:"applied_version"`
	ApplyError         string    `json:"apply_error"`
	MemAllocMB         float64   `json:"mem_alloc_mb"`
	ActiveConnections  int       `json:"active_connections"`
	SharedTasksRunning bool      `json:"shared_tasks_running"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type WorkerProbe struct {
	Status      string  `json:"status"`
	NodeID      string  `json:"node_id"`
	Version     string  `json:"version"`
	InFlight    int     `json:"in_flight"`
	MaxInFlight int     `json:"max_in_flight"`
	MemAllocMB  float64 `json:"mem_alloc_mb"`
	RTTMillis   int64   `json:"rtt_ms"`
}

type WorkerDrainStatus struct {
	Version          int64        `json:"version"`
	PendingEntries   []string     `json:"pending_entries"`
	UncertainEntries []string     `json:"uncertain_entries"`
	Complete         bool         `json:"complete"`
	Health           *WorkerProbe `json:"health"`
}

// EntryMembership survives Redis loss. Retirement requires graceful shutdown
// or a human operator's explicit attestation that the instance is fenced.
type EntryMembership struct {
	InstanceID string `json:"instance_id"`
	Retired    bool   `json:"retired"`
}

type WorkerClusterStore interface {
	Load(context.Context) (WorkerClusterSnapshot, error)
	Save(context.Context, int64, WorkerClusterSnapshot) error
	SetEntryMembership(context.Context, EntryMembership) error
	ListEntryMemberships(context.Context) ([]EntryMembership, error)
}

type WorkerClusterCoordination interface {
	SubscribeWorkerChanges(context.Context) <-chan int64
	PublishWorkerChange(context.Context, int64) error
	PutEntryHeartbeat(context.Context, EntryHeartbeat, time.Duration) error
	ListEntryHeartbeats(context.Context) ([]EntryHeartbeat, error)
	DeleteEntryHeartbeat(context.Context, string) error
}

type WorkerClusterRuntime interface {
	Validate(config.WorkerPoolConfig) error
	Reload(config.WorkerPoolConfig) error
	ProbeWorker(context.Context, config.WorkerPoolConfig, config.WorkerNodeConfig) (WorkerProbe, error)
	DrainWorker(context.Context, config.WorkerPoolConfig, config.WorkerNodeConfig) error
}
