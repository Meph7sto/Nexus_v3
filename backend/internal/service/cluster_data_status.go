package service

import (
	"context"
	"errors"
	"net"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/lib/pq"
)

type ClusterCurrentPostgres struct {
	Status       string `json:"status"`
	ReplicaState string `json:"replica_state"`
	Streaming    int    `json:"streaming"`
	Replicas     int    `json:"replicas"`
}

type ClusterCurrentRedis struct {
	Status  string `json:"status"`
	Mode    string `json:"mode,omitempty"`
	Primary string `json:"primary,omitempty"`
}

type ClusterCurrentBackup struct {
	Status    string `json:"status"`
	StartedAt string `json:"started_at,omitempty"`
}

func (s *ClusterDataService) currentPostgres(ctx context.Context) ClusterCurrentPostgres {
	out := ClusterCurrentPostgres{Status: "unknown", ReplicaState: "unknown"}
	if s.pool == nil {
		out.Status = "not_configured"
		return out
	}
	var inRecovery, readOnly bool
	const statusQuery = `SELECT pg_is_in_recovery(), current_setting('transaction_read_only') = 'on'`
	if err := s.pool.QueryRowContext(ctx, statusQuery).Scan(&inRecovery, &readOnly); err != nil {
		out.Status = clusterStatusError(err)
		return out
	}
	switch {
	case inRecovery:
		out.Status = "standby"
	case readOnly:
		out.Status = "read_only"
	default:
		out.Status = "primary"
	}
	var allowed bool
	if err := s.pool.QueryRowContext(ctx, `SELECT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER')`).Scan(&allowed); err != nil {
		out.ReplicaState = clusterStatusError(err)
		return out
	}
	if !allowed {
		out.ReplicaState = "permission_denied"
		return out
	}
	const replicaQuery = `SELECT COUNT(*) FILTER (WHERE state = 'streaming'), COUNT(*) FROM pg_stat_replication`
	if err := s.pool.QueryRowContext(ctx, replicaQuery).Scan(&out.Streaming, &out.Replicas); err != nil {
		out.ReplicaState = clusterStatusError(err)
		return out
	}
	out.ReplicaState = "measured"
	return out
}

func (s *ClusterDataService) currentRedis(ctx context.Context) ClusterCurrentRedis {
	mode := ""
	if s.active.Redis.Sentinel.Configured() {
		mode = "sentinel"
	}
	client, addr := openProbeRedis(ctx, s.active)
	if client == nil {
		if mode != "" {
			return ClusterCurrentRedis{Status: "unavailable", Mode: mode}
		}
		return ClusterCurrentRedis{Status: "not_configured"}
	}
	defer client.Close()
	info, err := client.Info(ctx, "server", "replication").Result()
	if err != nil {
		return ClusterCurrentRedis{Status: clusterStatusError(err), Mode: mode}
	}
	values := make(map[string]string)
	for line := range strings.SplitSeq(info, "\r\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok {
			values[k] = v
		}
	}
	out := ClusterCurrentRedis{Status: "unknown"}
	switch mode := values["redis_mode"]; mode {
	case "standalone", "sentinel", "cluster":
		out.Mode = mode
	}
	if mode != "" {
		out.Mode = mode
	}
	switch values["role"] {
	case "master":
		out.Status, out.Primary = "primary", addr
	case "slave":
		out.Status = "standby"
		if values["master_link_status"] == "up" && values["master_host"] != "" && values["master_port"] != "" {
			out.Primary = net.JoinHostPort(values["master_host"], values["master_port"])
		}
	}
	return out
}

func (s *ClusterDataService) currentBackup(ctx context.Context) ClusterCurrentBackup {
	if s.backup == nil {
		return ClusterCurrentBackup{Status: "unknown"}
	}
	cfg, err := s.backup.loadS3Config(ctx)
	if err != nil {
		return ClusterCurrentBackup{Status: clusterStatusError(err)}
	}
	if cfg == nil || !cfg.IsConfigured() {
		return ClusterCurrentBackup{Status: "not_configured"}
	}
	list, err := s.backup.ListBackups(ctx)
	if err != nil {
		return ClusterCurrentBackup{Status: clusterStatusError(err)}
	}
	if len(list) == 0 {
		return ClusterCurrentBackup{Status: "no_history"}
	}
	status := list[0].Status
	switch status {
	case "pending", "running", "completed", "failed":
	default:
		status = "unknown"
	}
	return ClusterCurrentBackup{Status: status, StartedAt: list[0].StartedAt}
}

func clusterStatusError(err error) string {
	var pgErr *pq.Error
	if infraerrors.IsForbidden(err) || (errors.As(err, &pgErr) && pgErr.Code == "42501") || strings.HasPrefix(err.Error(), "NOPERM") {
		return "permission_denied"
	}
	return "unknown"
}
