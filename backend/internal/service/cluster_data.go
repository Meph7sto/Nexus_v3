package service

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/redisconn"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type ClusterMigrationCheck func(context.Context, *sql.Conn) (int, error)

type ClusterDataService struct {
	active   config.ClusterDataProfile
	pool     *sql.DB
	backup   *BackupService
	profiles map[string]config.ClusterDataProfile
	check    ClusterMigrationCheck
	probes   chan struct{}
}

type ClusterDataProfileView struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Mode  string `json:"mode"`
}

type ClusterDataView struct {
	Profiles []ClusterDataProfileView `json:"profiles"`
	PGActive int                      `json:"pg_active"`
	PGIdle   int                      `json:"pg_idle"`
	Postgres ClusterCurrentPostgres   `json:"postgres"`
	Redis    ClusterCurrentRedis      `json:"redis"`
	Backup   ClusterCurrentBackup     `json:"backup"`
}

type ClusterDataRequest struct {
	ProfileID string `json:"profile_id"`
	Intent    string `json:"intent"`
}

const (
	DataIntentSameCluster = "same_cluster"
	DataIntentMigration   = "migration"
)

type ClusterDataCheck struct {
	Key string `json:"key"`
	OK  bool   `json:"ok"`
}

type ClusterDataResult struct {
	ProfileID      string             `json:"profile_id"`
	Intent         string             `json:"intent"`
	Mode           string             `json:"mode"`
	Ready          bool               `json:"ready"`
	MigrationCount int                `json:"migration_count"`
	RedisPrimary   string             `json:"redis_primary,omitempty"`
	Checks         []ClusterDataCheck `json:"checks"`
}

type ClusterDataExport struct {
	Preflight   ClusterDataResult `json:"preflight"`
	Config      map[string]any    `json:"config"`
	RequiredEnv []string          `json:"required_env"`
}

var dataProfileID = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func NewClusterDataService(cfg *config.Config, pool *sql.DB, check ClusterMigrationCheck, backup *BackupService) *ClusterDataService {
	s := &ClusterDataService{
		pool: pool, check: check, backup: backup, probes: make(chan struct{}, 2),
		profiles: make(map[string]config.ClusterDataProfile),
	}
	if cfg == nil {
		return s
	}
	s.active = config.ClusterDataProfile{Postgres: cfg.Database, Redis: cfg.Redis}
	for _, profile := range cfg.ClusterData.Profiles {
		if !dataProfileID.MatchString(profile.ID) {
			continue
		}
		switch profile.Mode {
		case "":
			profile.Mode = "single"
		case "single", "ha", "managed":
		default:
			continue
		}
		if profile.Label == "" {
			profile.Label = profile.ID
		}
		s.profiles[profile.ID] = profile
	}
	return s
}

func (s *ClusterDataService) View(ctx context.Context) ClusterDataView {
	view := ClusterDataView{Profiles: make([]ClusterDataProfileView, 0, len(s.profiles))}
	for _, profile := range s.profiles {
		view.Profiles = append(view.Profiles, ClusterDataProfileView{ID: profile.ID, Label: profile.Label, Mode: profile.Mode})
	}
	sort.Slice(view.Profiles, func(i, j int) bool { return view.Profiles[i].ID < view.Profiles[j].ID })
	if s.pool != nil {
		stats := s.pool.Stats()
		view.PGActive, view.PGIdle = stats.InUse, stats.Idle
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	view.Postgres = s.currentPostgres(ctx)
	view.Redis = s.currentRedis(ctx)
	view.Backup = s.currentBackup(ctx)
	return view
}

func (s *ClusterDataService) Probe(ctx context.Context, req ClusterDataRequest) (ClusterDataResult, error) {
	profile, ok := s.profiles[req.ProfileID]
	if !ok {
		return ClusterDataResult{}, infraerrors.NotFound("CLUSTER_DATA_PROFILE_NOT_FOUND", "Data target is not configured")
	}
	if req.Intent != DataIntentSameCluster && req.Intent != DataIntentMigration {
		return ClusterDataResult{}, infraerrors.BadRequest("CLUSTER_DATA_INTENT_INVALID", "Choose same cluster or migration")
	}
	select {
	case s.probes <- struct{}{}:
		defer func() { <-s.probes }()
	default:
		return ClusterDataResult{}, infraerrors.TooManyRequests("CLUSTER_DATA_BUSY", "Another Data preflight is running")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	out := ClusterDataResult{ProfileID: profile.ID, Intent: req.Intent, Mode: profile.Mode, Ready: true}
	add := func(key string, ok bool) {
		out.Checks = append(out.Checks, ClusterDataCheck{Key: key, OK: ok})
		out.Ready = out.Ready && ok
	}

	candidate, candidateClose := openProbePG(ctx, profile)
	defer candidateClose()
	add("postgres_connect", candidate != nil)
	writable := probePGWritable(ctx, candidate)
	add("postgres_writable", writable)
	migrationsOK := false
	if candidate != nil && s.check != nil {
		var err error
		out.MigrationCount, err = s.check(ctx, candidate)
		migrationsOK = err == nil
	}
	add("postgres_migrations", migrationsOK)

	if req.Intent == DataIntentSameCluster {
		current, closeCurrent := openProbePG(ctx, s.active)
		defer closeCurrent()
		same := writable && probePGWritable(ctx, current) && samePGIdentity(ctx, current, candidate)
		add("postgres_identity", same)
		add("postgres_shared_write", same && samePGWriteState(ctx, current, candidate))
	}

	red, addr := openProbeRedis(ctx, profile)
	if red != nil {
		defer red.Close()
	}
	out.RedisPrimary = addr
	key, value := newRedisProbe()
	targetWritable := redisProbeWrite(ctx, red, key, value)
	add("redis_writable", targetWritable)
	if req.Intent == DataIntentSameCluster {
		oldRedis, _ := openProbeRedis(ctx, s.active)
		if oldRedis != nil {
			defer oldRedis.Close()
		}
		oldKey, oldValue := newRedisProbe()
		same := targetWritable && s.active.Redis.DB == profile.Redis.DB && redisProbeWrite(ctx, oldRedis, oldKey, oldValue)
		add("redis_shared_write", same && redisProbeRead(ctx, oldRedis, key, value) && redisProbeRead(ctx, red, oldKey, oldValue))
	}
	return out, nil
}

func (s *ClusterDataService) Export(ctx context.Context, req ClusterDataRequest) (ClusterDataExport, error) {
	report, err := s.Probe(ctx, req)
	if err != nil {
		return ClusterDataExport{}, err
	}
	if !report.Ready {
		return ClusterDataExport{}, infraerrors.Conflict("CLUSTER_DATA_PREFLIGHT_FAILED", "Data preflight must pass before exporting")
	}
	p := s.profiles[req.ProfileID]
	redisHost, redisPortText, _ := net.SplitHostPort(report.RedisPrimary)
	redisPort, _ := strconv.Atoi(redisPortText)
	required := make([]string, 0, 2)
	if p.Postgres.Password != "" || p.PostgresPasswordEnv != "" {
		required = append(required, "DATABASE_PASSWORD")
	}
	if p.Redis.Password != "" || p.RedisPasswordEnv != "" {
		required = append(required, "REDIS_PASSWORD")
	}
	redisConfig := map[string]any{
		"host": redisHost, "port": redisPort, "db": p.Redis.DB,
		"username": p.Redis.Username, "enable_tls": p.Redis.EnableTLS,
	}
	if p.Redis.PoolSize > 0 {
		redisConfig["pool_size"] = p.Redis.PoolSize
		if p.Redis.MinIdleConns >= 0 && p.Redis.MinIdleConns <= p.Redis.PoolSize {
			redisConfig["min_idle_conns"] = p.Redis.MinIdleConns
		}
	}
	resolved, ok := resolveDataRedisConfig(p)
	if !ok {
		return ClusterDataExport{}, infraerrors.Conflict("CLUSTER_DATA_PREFLIGHT_FAILED", "Redis profile changed after preflight")
	}
	if sentinel := resolved.Sentinel; sentinel.Configured() {
		if sentinel.Password != "" {
			required = append(required, "REDIS_SENTINEL_PASSWORD")
		}
		redisConfig["sentinel"] = map[string]any{
			"addresses": sentinel.Addresses, "allowed_primaries": sentinel.AllowedPrimaries,
			"master_name": sentinel.MasterName, "username": sentinel.Username, "enable_tls": sentinel.EnableTLS,
		}
	}
	return ClusterDataExport{
		Preflight:   report,
		RequiredEnv: required,
		Config: map[string]any{
			"database": map[string]any{
				"host": p.Postgres.Host, "port": p.Postgres.Port, "dbname": p.Postgres.DBName,
				"user": p.Postgres.User, "sslmode": pgSSLMode(p.Postgres.SSLMode),
				"max_open_conns": p.Postgres.MaxOpenConns, "max_idle_conns": p.Postgres.MaxIdleConns,
			},
			"redis": redisConfig,
		},
	}, nil
}

func dataProbeSecret(inline, env string) (string, bool) {
	if env == "" {
		return inline, true
	}
	value, ok := os.LookupEnv(env)
	return value, inline == "" && ok && value != ""
}

func dataProbeAddr(host string, port int) string {
	if host == "" || port <= 0 || port > 65535 {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func pgSSLMode(mode string) string {
	if mode == "" {
		return "require"
	}
	return mode
}

func openProbePG(ctx context.Context, p config.ClusterDataProfile) (*sql.Conn, func()) {
	pg := p.Postgres
	host := dataProbeAddr(pg.Host, pg.Port)
	password, ok := dataProbeSecret(pg.Password, p.PostgresPasswordEnv)
	if host == "" || pg.DBName == "" || pg.User == "" || !ok {
		return nil, func() {}
	}
	dsn := url.URL{
		Scheme: "postgres", Host: host, Path: "/" + pg.DBName,
		User:     url.UserPassword(pg.User, password),
		RawQuery: url.Values{"sslmode": {pgSSLMode(pg.SSLMode)}, "connect_timeout": {"2"}}.Encode(),
	}
	connector, err := pq.NewConnector(dsn.String())
	if err != nil {
		return nil, func() {}
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, func() {}
	}
	return conn, func() { _ = conn.Close(); _ = db.Close() }
}

func probePGWritable(ctx context.Context, conn *sql.Conn) bool {
	if conn == nil {
		return false
	}
	var primary bool
	return conn.QueryRowContext(ctx, `SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off'`).Scan(&primary) == nil && primary
}

func samePGIdentity(ctx context.Context, a, b *sql.Conn) bool {
	const q = `SELECT system_identifier::text, current_database() FROM pg_control_system()`
	var aID, bID, aDB, bDB string
	return a.QueryRowContext(ctx, q).Scan(&aID, &aDB) == nil && b.QueryRowContext(ctx, q).Scan(&bID, &bDB) == nil &&
		aID != "" && aID == bID && aDB != "" && aDB == bDB
}

// A random transaction-scoped advisory lock verifies shared writable state.
// A former primary or physical clone can share system_identifier, but not locks.
func samePGWriteState(ctx context.Context, a, b *sql.Conn) bool {
	first, err := a.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer first.Rollback()
	second, err := b.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer second.Rollback()
	var nonce [8]byte
	rand.Read(nonce[:])
	key := int64(binary.LittleEndian.Uint64(nonce[:]))
	const q = `SELECT pg_try_advisory_xact_lock($1) WHERE NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off'`
	var acquired, collided bool
	return first.QueryRowContext(ctx, q, key).Scan(&acquired) == nil && acquired &&
		second.QueryRowContext(ctx, q, key).Scan(&collided) == nil && !collided
}

func openProbeRedis(ctx context.Context, p config.ClusterDataProfile) (*redis.Client, string) {
	cfg, ok := resolveDataRedisConfig(p)
	if !ok {
		return nil, ""
	}
	if cfg.Sentinel.Configured() {
		addr, err := probeSentinelPrimary(ctx, cfg.Sentinel)
		if err != nil {
			return nil, ""
		}
		cfg.DialTimeoutSeconds = 2
		cfg.ReadTimeoutSeconds = 2
		cfg.WriteTimeoutSeconds = 2
		cfg.PoolSize = 1
		cfg.MinIdleConns = 0
		return redisconn.NewFailover(cfg), addr
	}
	addr := dataProbeAddr(cfg.Host, cfg.Port)
	return redis.NewClient(&redis.Options{
		Addr: addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
		PoolSize: 1, MaxRetries: -1, ContextTimeoutEnabled: true, TLSConfig: dataProbeTLS(cfg.EnableTLS),
	}), addr
}

func dataProbeTLS(enabled bool) *tls.Config {
	if !enabled {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func resolveDataRedisConfig(p config.ClusterDataProfile) (config.RedisConfig, bool) {
	cfg := p.Redis
	password, ok := dataProbeSecret(cfg.Password, p.RedisPasswordEnv)
	if !ok || cfg.DB < 0 {
		return config.RedisConfig{}, false
	}
	cfg.Password = password
	activeLegacy := len(p.Sentinel.Addresses) > 0 || p.Sentinel.MasterName != "" || p.Sentinel.Username != "" ||
		p.Sentinel.PasswordEnv != "" || len(p.Sentinel.AllowedPrimaries) > 0 || p.Sentinel.EnableTLS
	if activeLegacy {
		if cfg.Sentinel.Configured() {
			return config.RedisConfig{}, false
		}
		password, ok := dataProbeSecret("", p.Sentinel.PasswordEnv)
		if !ok {
			return config.RedisConfig{}, false
		}
		allowed := slices.Clone(p.Sentinel.AllowedPrimaries)
		if addr, valid := config.NormalizeRedisEndpoint(dataProbeAddr(cfg.Host, cfg.Port)); valid {
			if !slices.ContainsFunc(allowed, func(other string) bool {
				normalized, _ := config.NormalizeRedisEndpoint(other)
				return normalized == addr
			}) {
				allowed = append(allowed, addr)
			}
		}
		cfg.Sentinel = config.RedisSentinelConfig{
			Addresses: p.Sentinel.Addresses, AllowedPrimaries: allowed, MasterName: p.Sentinel.MasterName,
			Username: p.Sentinel.Username, Password: password, EnableTLS: p.Sentinel.EnableTLS,
		}
	}
	if cfg.Sentinel.Configured() {
		return cfg, cfg.Sentinel.Validate() == nil
	}
	return cfg, dataProbeAddr(cfg.Host, cfg.Port) != ""
}

func probeSentinelPrimary(ctx context.Context, cfg config.RedisSentinelConfig) (string, error) {
	for _, addr := range cfg.Addresses {
		normalized, ok := config.NormalizeRedisEndpoint(addr)
		if !ok {
			continue
		}
		client := redis.NewSentinelClient(&redis.Options{
			Addr: normalized, Username: cfg.Username, Password: cfg.Password, TLSConfig: dataProbeTLS(cfg.EnableTLS),
			DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, MaxRetries: -1, ContextTimeoutEnabled: true,
		})
		primary, err := client.GetMasterAddrByName(ctx, cfg.MasterName).Result()
		_ = client.Close()
		if err != nil || len(primary) != 2 {
			continue
		}
		host, ok := config.NormalizeRedisEndpoint(net.JoinHostPort(primary[0], primary[1]))
		if !ok {
			continue
		}
		for _, allowed := range cfg.AllowedPrimaries {
			if normalized, _ := config.NormalizeRedisEndpoint(allowed); host == normalized {
				return host, nil
			}
		}
	}
	return "", errors.New("authorised Redis primary not verified")
}

func newRedisProbe() (string, string) {
	var nonce [32]byte
	rand.Read(nonce[:])
	return "nexus:cluster:data-probe:" + hex.EncodeToString(nonce[:16]), hex.EncodeToString(nonce[16:])
}

func redisProbeWrite(ctx context.Context, client *redis.Client, key, value string) bool {
	return client != nil && client.SetNX(ctx, key, value, 10*time.Second).Val()
}

func redisProbeRead(ctx context.Context, client *redis.Client, key, value string) bool {
	got, err := client.Get(ctx, key).Result()
	return err == nil && got == value
}
