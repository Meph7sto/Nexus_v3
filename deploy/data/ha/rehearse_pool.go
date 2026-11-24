// Local-only PostgreSQL HA fixture driven by rehearse-pg.ps1.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

const postgresImage = "postgres:18-alpine@sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7"

type rehearsal struct {
	project string
	compose string
	secrets string
	mode    string
	network string
	db      *sql.DB
	id      map[string]string
	ip      map[string]string
	etcdID  string
}

type result struct {
	Mode                     string  `json:"mode"`
	InitialPrimary           string  `json:"initial_primary"`
	EntryNetworkConnected    bool    `json:"entry_network_connected"`
	CrashReadableSeconds     float64 `json:"crash_readable_seconds"`
	CrashRTOSeconds          float64 `json:"crash_rto_seconds"`
	CrashStrictWriteWait     bool    `json:"crash_strict_write_wait"`
	CrashMissingAcknowledged int     `json:"crash_missing_acknowledged"`
	PartitionRTOSeconds      float64 `json:"partition_rto_seconds"`
	PartitionReadableSeconds float64 `json:"partition_readable_seconds"`
	PartitionStrictWriteWait bool    `json:"partition_strict_write_wait"`
	OldPrimaryFencedSeconds  float64 `json:"old_primary_fenced_seconds"`
	PartitionMissingAck      int     `json:"partition_missing_acknowledged"`
	OriginalPoolReconnected  bool    `json:"original_pool_reconnected"`
	OldPrimaryRejoined       bool    `json:"old_primary_rejoined"`
	RewindObserved           bool    `json:"pg_rewind_observed"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	run := rehearsal{
		project: os.Getenv("NEXUS_HA_REHEARSAL_PROJECT"), compose: os.Getenv("NEXUS_HA_REHEARSAL_COMPOSE"),
		secrets: os.Getenv("NEXUS_HA_REHEARSAL_SECRETS"), mode: os.Getenv("NEXUS_HA_REHEARSAL_MODE"),
		id: make(map[string]string),
		ip: make(map[string]string),
	}
	if err := run.start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func command(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (r *rehearsal) projectCommand(ctx context.Context, args ...string) (string, error) {
	return command(ctx, append([]string{"compose", "-f", r.compose, "-p", r.project}, args...)...)
}

func (r *rehearsal) start(ctx context.Context) error {
	if !regexp.MustCompile(`^nexushapg-[a-f0-9]{12}$`).MatchString(r.project) ||
		!filepath.IsAbs(r.compose) || !filepath.IsAbs(r.secrets) || filepath.Base(r.secrets) != r.project ||
		(r.mode != "sync" && r.mode != "sync_strict" && r.mode != "async") {
		return errors.New("rehearsal requires an isolated Compose project")
	}
	r.network = r.project + "_ha"
	label, err := command(ctx, "network", "inspect", "--format", `{{index .Labels "com.docker.compose.project"}}`, r.network)
	if err != nil || label != r.project {
		return errors.New("unexpected rehearsal network")
	}
	for _, node := range []string{"data-1", "data-2"} {
		id, err := r.projectCommand(ctx, "ps", "-q", node)
		if err != nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(id) {
			return fmt.Errorf("cannot identify rehearsal container %s", node)
		}
		owner, err := command(ctx, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, id)
		if err != nil || owner != r.project {
			return errors.New("unexpected data container project")
		}
		r.id[node] = id
		if err := r.captureNodeIP(ctx, node); err != nil {
			return err
		}
	}
	r.etcdID, err = r.projectCommand(ctx, "ps", "-q", "etcd-1")
	if err != nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(r.etcdID) {
		return errors.New("cannot identify the isolated etcd member")
	}
	addr, err := r.projectCommand(ctx, "port", "pg-proxy", "5432")
	host, port, parseErr := net.SplitHostPort(addr)
	if err != nil || parseErr != nil || host != "127.0.0.1" {
		return errors.New("rehearsal proxy is not published only on loopback")
	}
	password, err := os.ReadFile(filepath.Join(r.secrets, "patroni-common", "postgres-password"))
	if err != nil {
		return errors.New("missing rehearsal PostgreSQL password file")
	}
	dsn := url.URL{Scheme: "postgres", Host: net.JoinHostPort(host, port), Path: "/postgres", User: url.UserPassword("postgres", strings.TrimSpace(string(password)))}
	q := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {filepath.Join(r.secrets, "ca.crt")}, "connect_timeout": {"2"}}
	dsn.RawQuery = q.Encode()
	r.db, err = sql.Open("postgres", dsn.String())
	if err != nil {
		return errors.New("cannot open the rehearsal connection pool")
	}
	defer r.db.Close()
	r.db.SetMaxOpenConns(3)
	r.db.SetMaxIdleConns(3)

	var initialIP string
	if err := until(ctx, 90*time.Second, func(ctx context.Context) bool {
		return r.db.QueryRowContext(ctx, `SELECT host(inet_server_addr()) WHERE NOT pg_is_in_recovery()`).Scan(&initialIP) == nil
	}); err != nil {
		return errors.New("no writable PostgreSQL node became available through HAProxy")
	}
	old := r.nodeAt(initialIP)
	if _, ok := r.id[old]; !ok {
		return errors.New("unexpected PostgreSQL primary identity")
	}
	if err := r.awaitReplica(ctx); err != nil {
		return err
	}
	if err := r.entryNetworkProbe(ctx); err != nil {
		return err
	}
	fmt.Println("Both PostgreSQL members ready; Entry-network verified with pg-proxy TLS")
	if _, err := r.db.ExecContext(ctx, `CREATE TABLE nexus_ha_probe (id TEXT PRIMARY KEY)`); err != nil {
		return fmt.Errorf("cannot create rehearsal ledger: %w", err)
	}
	acknowledged := []string{"acked-1", "acked-2", "acked-3"}
	for _, id := range acknowledged {
		if _, err := r.db.ExecContext(ctx, `INSERT INTO nexus_ha_probe VALUES ($1)`, id); err != nil {
			return fmt.Errorf("cannot record rehearsal ledger item: %w", err)
		}
	}
	report := result{Mode: r.mode, InitialPrimary: old, EntryNetworkConnected: true}

	oldConn, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer oldConn.Close()
	var connectionNode string
	if err := oldConn.QueryRowContext(ctx, `SELECT host(inet_server_addr())`).Scan(&connectionNode); err != nil || connectionNode != initialIP {
		return errors.New("could not pin a pre-failure pool connection")
	}
	crash := time.Now()
	if _, err := command(ctx, "stop", "--time", "0", r.id[old]); err != nil {
		return errors.New("could not stop the isolated primary")
	}
	leader, err := r.awaitPrimary(ctx, old)
	if err != nil {
		return err
	}
	report.CrashReadableSeconds = time.Since(crash).Seconds()
	fmt.Printf("Replacement primary %s was readable in %.2fs after stopping %s\n", leader, report.CrashReadableSeconds, old)
	report.CrashMissingAcknowledged, err = r.missing(ctx, acknowledged)
	if err != nil {
		return err
	}
	var crashWrite *strictWrite
	if r.mode == "sync_strict" {
		crashWrite, err = r.startStrictWrite(ctx, "crash-recovered")
		if err != nil {
			return err
		}
		defer crashWrite.cancel()
		report.CrashStrictWriteWait = true
	} else {
		if _, err := r.awaitWrite(ctx, old, "crash-recovered"); err != nil {
			return err
		}
		report.CrashRTOSeconds = time.Since(crash).Seconds()
	}
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err = oldConn.QueryRowContext(checkCtx, `SELECT host(inet_server_addr())`).Scan(&connectionNode)
	cancel()
	if err == nil {
		return errors.New("the pre-failure pooled connection unexpectedly remained usable")
	}
	oldConn.Close()
	report.OriginalPoolReconnected = true
	crashRestore := time.Now()
	if _, err := r.projectCommand(ctx, "start", old); err != nil {
		return errors.New("could not restart the original primary")
	}
	if err := r.captureNodeIP(ctx, old); err != nil {
		return err
	}
	if err := r.awaitReplica(ctx); err != nil {
		return err
	}
	if err := r.awaitLocalReplica(ctx, old); err != nil {
		return err
	}
	if r.mode == "sync_strict" {
		finished, err := crashWrite.committed(ctx, crashRestore)
		if err != nil {
			return err
		}
		report.CrashRTOSeconds = finished.Sub(crash).Seconds()
	}
	acknowledged = append(acknowledged, "crash-recovered")
	report.OldPrimaryRejoined = true

	// The old leader stays running, but cannot reach the DCS or its replica.
	// Leave a private divergent WAL record before promotion and never count it
	// as an acknowledged Entry transaction.
	partition := time.Now()
	if _, err := command(ctx, "network", "disconnect", r.network, r.id[leader]); err != nil {
		return errors.New("could not isolate the second primary")
	}
	if out, err := r.localSQL(ctx, leader, `SET synchronous_commit=local; INSERT INTO nexus_ha_probe VALUES ('isolated-divergence')`); err != nil {
		return fmt.Errorf("could not prepare synthetic divergent WAL before leader replacement: %s", out)
	}
	report.OldPrimaryFencedSeconds, report.PartitionReadableSeconds, err = r.awaitPartition(ctx, leader, partition)
	if err != nil {
		return err
	}
	fmt.Printf("Partitioned old primary stopped accepting writes after %.2fs\n", report.OldPrimaryFencedSeconds)
	var partitionWrite *strictWrite
	if r.mode == "sync_strict" {
		partitionWrite, err = r.startStrictWrite(ctx, "partition-recovered")
		if err != nil {
			return err
		}
		defer partitionWrite.cancel()
		report.PartitionStrictWriteWait = true
	} else {
		if _, err := r.awaitWrite(ctx, leader, "partition-recovered"); err != nil {
			return err
		}
		report.PartitionRTOSeconds = time.Since(partition).Seconds()
	}
	report.PartitionMissingAck, err = r.missing(ctx, acknowledged)
	if err != nil {
		return err
	}
	partitionRestore := time.Now()
	if _, err := command(ctx, "network", "connect", "--alias", leader, r.network, r.id[leader]); err != nil {
		return errors.New("could not reconnect the isolated node")
	}
	if err := r.awaitReplica(ctx); err != nil {
		return err
	}
	if err := r.awaitLocalReplica(ctx, leader); err != nil {
		return err
	}
	if r.mode == "sync_strict" {
		finished, err := partitionWrite.committed(ctx, partitionRestore)
		if err != nil {
			return err
		}
		report.PartitionRTOSeconds = finished.Sub(partition).Seconds()
	}
	row, err := r.localSQL(ctx, leader, `SELECT count(*) FROM nexus_ha_probe WHERE id='isolated-divergence'`)
	if err != nil || row != "0" {
		return errors.New("divergent isolated WAL was not removed from the rejoined replica")
	}
	logs, err := command(ctx, "logs", "--since", partition.UTC().Format(time.RFC3339Nano), r.id[leader])
	report.RewindObserved = err == nil && strings.Contains(strings.ToLower(logs), "pg_rewind")
	if !report.RewindObserved {
		return errors.New("no pg_rewind evidence in the rejoining primary's Patroni log")
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return err
	}
	if report.CrashMissingAcknowledged != 0 || report.PartitionMissingAck != 0 {
		return errors.New("the rehearsal lost acknowledged ledger items")
	}
	return nil
}

func (r *rehearsal) awaitPartition(ctx context.Context, previous string, start time.Time) (fenced, readable float64, err error) {
	var overlap bool
	err = until(ctx, 70*time.Second, func(ctx context.Context) bool {
		if r.localFenceObserved(ctx, previous) && fenced == 0 {
			fenced = time.Since(start).Seconds()
		}
		var address string
		if err := r.db.QueryRowContext(ctx, `SELECT host(inet_server_addr())
			WHERE NOT pg_is_in_recovery() AND inet_server_addr()<>$1::inet`, r.ip[previous]).Scan(&address); err != nil || r.nodeAt(address) == "" {
			return false
		}
		readable = time.Since(start).Seconds()
		overlap = fenced == 0 || !r.localFenceObserved(ctx, previous)
		return true
	})
	if overlap {
		return fenced, readable, errors.New("promoted primary was reachable before the isolated old node was fenced")
	}
	if err != nil {
		return fenced, readable, errors.New("did not observe both old-primary fencing and new-primary routing")
	}
	return fenced, readable, nil
}

func (r *rehearsal) entryNetworkProbe(ctx context.Context) error {
	// Same DNS path used by a separately-compiled Entry Compose project
	// attached to the external HA bridge, without the host port mapping.
	secrets := strings.ReplaceAll(r.secrets, "\\", "/")
	out, err := command(ctx, "run", "--rm", "--pull", "never", "--network", r.network,
		"--mount", "type=bind,source="+secrets+"/ca.crt,target=/run/rehearsal-ca.crt,readonly",
		"--mount", "type=bind,source="+secrets+"/patroni-common/postgres-password,target=/run/rehearsal-password,readonly",
		"--entrypoint", "sh", postgresImage, "-ec",
		`PGPASSWORD=$(cat /run/rehearsal-password); export PGPASSWORD;
		export PGSSLMODE=verify-full PGSSLROOTCERT=/run/rehearsal-ca.crt PGCONNECT_TIMEOUT=3;
		psql -h pg-proxy -p 5432 -U postgres -d postgres -Atc "SELECT NOT pg_is_in_recovery();"`)
	if err != nil || out != "t" {
		return fmt.Errorf("independent container could not reach the primary via pg-proxy: %s", out)
	}
	return nil
}

func (r *rehearsal) awaitReplica(ctx context.Context) error {
	if err := until(ctx, 75*time.Second, func(ctx context.Context) bool {
		var primaryIP, standby string
		err := r.db.QueryRowContext(ctx, `SELECT host(inet_server_addr()), application_name
			FROM pg_stat_replication WHERE state='streaming' AND ($1 = 'async' OR sync_state='sync')`, r.mode).Scan(&primaryIP, &standby)
		return err == nil && (r.mode == "async" || r.syncCandidateRecorded(ctx, r.nodeAt(primaryIP), standby))
	}); err != nil {
		return errors.New("replica did not start streaming in the configured mode")
	}
	return nil
}

func (r *rehearsal) awaitPrimary(ctx context.Context, previous string) (string, error) {
	var address string
	err := until(ctx, 60*time.Second, func(ctx context.Context) bool {
		return r.db.QueryRowContext(ctx, `SELECT host(inet_server_addr())
			WHERE NOT pg_is_in_recovery() AND inet_server_addr()<>$1::inet`, r.ip[previous]).Scan(&address) == nil
	})
	node := r.nodeAt(address)
	if err != nil || node == previous || node == "" {
		return "", errors.New("HAProxy could not reach a promoted PostgreSQL primary")
	}
	return node, nil
}

type strictWriteResult struct {
	at  time.Time
	err error
}

type strictWrite struct {
	cancel context.CancelFunc
	done   <-chan strictWriteResult
}

func (r *rehearsal) startStrictWrite(ctx context.Context, marker string) (*strictWrite, error) {
	attempt, cancel := context.WithTimeout(ctx, 110*time.Second)
	done := make(chan strictWriteResult, 1)
	go func() {
		_, err := r.db.ExecContext(attempt, `INSERT INTO nexus_ha_probe (id) VALUES ($1)`, marker)
		done <- strictWriteResult{at: time.Now(), err: err}
	}()
	err := until(ctx, 4*time.Second, func(ctx context.Context) bool {
		var waiting bool
		queryErr := r.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event='SyncRep' AND query='INSERT INTO nexus_ha_probe (id) VALUES ($1)')`).Scan(&waiting)
		return queryErr == nil && waiting
	})
	if err != nil {
		cancel()
		return nil, errors.New("strict synchronous write did not demonstrably wait for the missing standby")
	}
	select {
	case <-done:
		cancel()
		return nil, errors.New("strict synchronous write finished before standby restoration")
	default:
	}
	fmt.Println("Strict synchronous write is waiting for replication; retaining the same transaction for recovery")
	return &strictWrite{cancel: cancel, done: done}, nil
}

func (w *strictWrite) committed(ctx context.Context, restoreStarted time.Time) (time.Time, error) {
	select {
	case result := <-w.done:
		if result.err != nil || result.at.Before(restoreStarted) {
			return time.Time{}, errors.New("waiting synchronous write did not succeed during standby recovery")
		}
		return result.at, nil
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}
}

func (r *rehearsal) syncCandidateRecorded(ctx context.Context, primary, standby string) bool {
	data, err := command(ctx, "exec", r.etcdID, "/usr/local/bin/etcdctl",
		"--endpoints=https://etcd-1:2379", "--cacert=/run/etcd-tls/ca.crt",
		"--cert=/run/etcd-tls/server.crt", "--key=/run/etcd-tls/server.key",
		"get", "/nexus-ha/"+r.project+"/sync", "--print-value-only")
	if err != nil {
		return false
	}
	var state struct {
		Leader      string `json:"leader"`
		SyncStandby string `json:"sync_standby"`
	}
	if json.Unmarshal([]byte(data), &state) != nil || state.Leader != primary {
		return false
	}
	for name := range strings.SplitSeq(state.SyncStandby, ",") {
		if name == standby {
			return true
		}
	}
	return false
}

func (r *rehearsal) awaitLocalReplica(ctx context.Context, node string) error {
	return until(ctx, 45*time.Second, func(ctx context.Context) bool {
		out, err := r.localSQL(ctx, node, `SELECT pg_is_in_recovery()`)
		return err == nil && out == "t"
	})
}

func (r *rehearsal) awaitWrite(ctx context.Context, previous, marker string) (string, error) {
	var address string
	err := until(ctx, 60*time.Second, func(ctx context.Context) bool {
		return r.db.QueryRowContext(ctx, `
			INSERT INTO nexus_ha_probe (id)
			SELECT $1 WHERE inet_server_addr()<>$2::inet
			ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id
			RETURNING host(inet_server_addr())`, marker, r.ip[previous]).Scan(&address) == nil
	})
	node := r.nodeAt(address)
	if err != nil || node == previous || r.id[node] == "" {
		return "", errors.New("the original pool could not write to the replacement primary")
	}
	return node, nil
}

func (r *rehearsal) captureNodeIP(ctx context.Context, node string) error {
	template := fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, r.network)
	address, err := command(ctx, "inspect", "--format", template, r.id[node])
	if err != nil || net.ParseIP(address) == nil {
		return fmt.Errorf("cannot find the address of rehearsal container %s", node)
	}
	r.ip[node] = address
	return nil
}

func (r *rehearsal) nodeAt(address string) string {
	for node, candidate := range r.ip {
		if candidate == address {
			return node
		}
	}
	return ""
}

func (r *rehearsal) localSQL(ctx context.Context, node, query string) (string, error) {
	return command(ctx, "exec", "--user", "postgres", "--env", "PGOPTIONS=-c statement_timeout=1500",
		r.id[node], "psql", "-h", "/var/run/postgresql", "-d", "postgres", "-At", "-v", "ON_ERROR_STOP=1", "-c", query)
}

func (r *rehearsal) localFenceObserved(ctx context.Context, node string) bool {
	out, err := r.localSQL(ctx, node, `BEGIN; INSERT INTO nexus_ha_probe VALUES ('must-not-write'); ROLLBACK;`)
	if err == nil {
		return false
	}
	return strings.Contains(out, "read-only transaction") || strings.Contains(out, "Connection refused") ||
		strings.Contains(out, "No such file or directory") || strings.Contains(out, "database system is shutting down") ||
		strings.Contains(out, "database system is starting up")
}

func (r *rehearsal) missing(ctx context.Context, acknowledged []string) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT count(*) FROM nexus_ha_probe WHERE id=ANY($1)`, pq.Array(acknowledged)).Scan(&count)
	return len(acknowledged) - count, err
}

func until(ctx context.Context, max time.Duration, check func(context.Context) bool) error {
	ctx, cancel := context.WithTimeout(ctx, max)
	defer cancel()
	ticker := time.NewTicker(350 * time.Millisecond)
	defer ticker.Stop()
	for {
		attempt, finish := context.WithTimeout(ctx, 3*time.Second)
		ok := check(attempt)
		finish()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
