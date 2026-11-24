//go:build backup_rehearsal

package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const rehearsalAdmin = "nexus_rehearsal_admin"
const rehearsalWriter = "nexus_rehearsal_writer"
const rehearsalCounter = "nexus:rehearsal:settled-usage"

// Backup metadata lives on the operator side, outside both fenced writer
// identities. This keeps the archive catalog available after a cold loss.
type rehearsalCatalog struct {
	service.SettingRepository
	values map[string]string
}

func (c *rehearsalCatalog) GetValue(_ context.Context, key string) (string, error) {
	return c.values[key], nil
}

func (c *rehearsalCatalog) Set(_ context.Context, key, value string) error {
	c.values[key] = value
	return nil
}

func TestDataBackupRehearsal(t *testing.T) {
	name, secret := os.Getenv("NEXUS_REHEARSAL_DB"), os.Getenv("NEXUS_REHEARSAL_SECRET")
	require.Regexp(t, `^nexus_backup_rehearsal_[a-f0-9]{12}$`, name, "run through backup-rehearsal.ps1")
	require.True(t, regexp.MustCompile(`^[a-f0-9]{48}$`).MatchString(secret), "missing rehearsal-only secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg := func(key string) *config.Config {
		var encryption [32]byte
		rand.Read(encryption[:])
		cfg := &config.Config{Database: config.DatabaseConfig{
			Host: "127.0.0.1", Port: rehearsalPort(t, key), User: rehearsalAdmin, Password: secret, DBName: name, SSLMode: "disable",
		}}
		cfg.Totp.EncryptionKey = hex.EncodeToString(encryption[:])
		cfg.Totp.EncryptionKeyConfigured = true
		return cfg
	}
	oldCfg, newCfg := pg("OLD_PG"), pg("NEW_PG")
	newCfg.Totp = oldCfg.Totp
	oldPG, newPG := rehearsalPG(t, ctx, oldCfg.Database, true), rehearsalPG(t, ctx, newCfg.Database, true)
	oldRedis := rehearsalRedis(t, ctx, "OLD_REDIS")
	newRedis := rehearsalRedis(t, ctx, "NEW_REDIS")
	require.NoError(t, ApplyMigrations(ctx, oldPG))
	rehearsalExec(t, ctx, oldPG, rehearsalSeed)

	// psql must return an error and leave no partially restored schema.
	bad := strings.NewReader("CREATE TABLE rehearsal_partial_restore (id int);\nSELECT 1 / 0;\n")
	require.Error(t, NewPgDumper(newCfg, newPG).Restore(ctx, bad))
	var partiallyRestored bool
	require.NoError(t, newPG.QueryRowContext(ctx, `SELECT to_regclass('public.rehearsal_partial_restore') IS NOT NULL`).Scan(&partiallyRestored))
	require.False(t, partiallyRestored)

	// The rehearsal keeps two logical Entry clients and a synthetic task and
	// usage queue. Ledger effects themselves use the real billing repository.
	oldApps := activateRehearsalWriters(t, ctx, oldPG, oldRedis, oldCfg.Database, secret)
	ids := loadRehearsalIDs(t, ctx, oldApps[0].pg)
	bill := func(db *sql.DB, request string, balance, sub float64) (bool, error) {
		cmd := &service.UsageBillingCommand{RequestID: request, APIKeyID: ids.key, UserID: ids.user, AccountID: ids.account,
			SubscriptionID: &ids.subscription, BalanceCost: balance, SubscriptionCost: sub, APIKeyQuotaCost: balance + sub}
		result, err := NewUsageBillingRepository(nil, db).Apply(ctx, cmd)
		if err != nil || !result.Applied {
			return false, err
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO usage_logs (request_id,user_id,api_key_id,account_id,subscription_id,model,input_tokens,output_tokens,actual_cost)
			VALUES ($1,$2,$3,$4,$5,'fixture-model',17,8,$6)`, request, ids.user, ids.key, ids.account, ids.subscription, balance+sub)
		return err == nil, err
	}
	applied, err := bill(oldApps[0].pg, "before-cutover", 1.25, 0)
	require.NoError(t, err)
	require.True(t, applied)

	tasks, stopTasks := context.WithCancel(ctx)
	defer stopTasks()
	tasksDone := make(chan struct{})
	go func() { <-tasks.Done(); close(tasksDone) }()
	usageSettled := make(chan error, 1)
	releaseUsage := make(chan struct{})
	go func() {
		<-releaseUsage
		applied, err := bill(oldApps[1].pg, "drained-tail", 0, 0.5)
		if err == nil && !applied {
			err = errors.New("admitted usage did not settle")
		}
		usageSettled <- err
	}()
	// Simulated ingress stops accepting new work, while an already admitted
	// bill completes and scheduled work comes to a stop.
	cutoverStart := time.Now()
	stopTasks()
	<-tasksDone
	close(releaseUsage)
	require.NoError(t, <-usageSettled)
	fenceRehearsalWriters(t, ctx, oldPG, oldRedis, oldApps)
	require.ErrorIs(t, newRedis.Get(ctx, rehearsalCounter).Err(), redis.Nil)
	rehearsalExec(t, ctx, oldPG, `SELECT 1`)

	cat := &rehearsalCatalog{values: make(map[string]string)}
	encryptor, err := NewAESEncryptor(oldCfg)
	require.NoError(t, err)
	factory := NewS3BackupStoreFactory()
	bucket := strings.ReplaceAll(name, "_", "-")
	s3Cfg := service.BackupS3Config{
		Endpoint: "http://127.0.0.1:" + strconv.Itoa(rehearsalPort(t, "S3")), Bucket: bucket,
		Region: "us-east-1", AccessKeyID: "rehearsal-local-only", SecretAccessKey: secret,
		Prefix: "fixture-archives", ForcePathStyle: true,
	}
	s3Client, err := newS3Client(ctx, s3ClientParams{
		Endpoint: s3Cfg.Endpoint, Region: s3Cfg.Region, AccessKeyID: s3Cfg.AccessKeyID, SecretAccessKey: s3Cfg.SecretAccessKey, ForcePathStyle: true,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		probe, stop := context.WithTimeout(ctx, 1*time.Second)
		defer stop()
		_, err := s3Client.ListBuckets(probe, &s3.ListBucketsInput{})
		return err == nil
	}, 45*time.Second, 300*time.Millisecond, "isolated S3 endpoint did not become ready")
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)
	oldBackup := service.NewBackupService(cat, oldCfg, encryptor, factory, NewPgDumper(oldCfg, oldPG))
	newBackup := service.NewBackupService(cat, newCfg, encryptor, factory, NewPgDumper(newCfg, newPG))
	defer oldBackup.Stop()
	defer newBackup.Stop()
	_, err = oldBackup.UpdateS3Config(ctx, s3Cfg)
	require.NoError(t, err)
	require.NoError(t, oldBackup.TestS3Connection(ctx, s3Cfg))

	oldSnap := rehearsalSnapshot(t, ctx, oldPG)
	archive := backupAndRestore(t, ctx, oldBackup, newBackup)
	assertRehearsalSchema(t, ctx, newPG)
	require.Equal(t, oldSnap, rehearsalSnapshot(t, ctx, newPG), "cold restore differed from the fenced snapshot")
	rebuildRehearsalCounter(t, ctx, newPG, newRedis)

	newApps := activateRehearsalWriters(t, ctx, newPG, newRedis, newCfg.Database, secret)
	for _, instance := range newApps {
		require.Equal(t, oldSnap, rehearsalSnapshot(t, ctx, instance.pg), "part of the Entry group failed to switch")
		value, err := instance.redis.Get(ctx, rehearsalCounter).Result()
		require.NoError(t, err)
		require.Equal(t, "2", value)
	}
	cutoverDuration := time.Since(cutoverStart)
	t.Logf("before new writes: both ledgers matched; clean switch-back boundary verified; cutover=%s archive_bytes=%d", cutoverDuration.Round(time.Millisecond), archive.SizeBytes)

	applied, err = bill(newApps[0].pg, "before-cutover", 1.25, 0)
	require.NoError(t, err)
	require.False(t, applied, "backup must preserve billing deduplication")
	applied, err = bill(newApps[1].pg, "after-cutover", 0.75, 0)
	require.NoError(t, err)
	require.True(t, applied)
	newSnap := rehearsalSnapshot(t, ctx, newPG)
	require.Equal(t, oldSnap, rehearsalSnapshot(t, ctx, oldPG), "old target changed after the writer fence")
	require.NotEqual(t, oldSnap, newSnap, "rollback must block while the new ledger has additional writes")
	var logs, dedup int
	var balance, daily, quota string
	require.NoError(t, newPG.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM usage_logs),
		       (SELECT COUNT(*) FROM usage_billing_dedup),
		       (SELECT balance::text FROM users WHERE id=$1),
		       (SELECT daily_usage_usd::text FROM user_subscriptions WHERE id=$2),
		       (SELECT quota_used::text FROM api_keys WHERE id=$3)`, ids.user, ids.subscription, ids.key).Scan(&logs, &dedup, &balance, &daily, &quota))
	require.Equal(t, 3, logs)
	require.Equal(t, 3, dedup)
	require.Equal(t, "10.34000000", balance)
	require.Equal(t, "0.5000000000", daily)
	require.Equal(t, "2.50000000", quota)
	t.Logf("new Data reconciled: usage=%d dedup=%d balance=%s subscription_daily=%s key_quota=%s", logs, dedup, balance, daily, quota)

	// Demonstrate a reconciled rollback. New writers are fenced first, then
	// the authoritative new snapshot is restored back to the former Data.
	rollbackStart := time.Now()
	fenceRehearsalWriters(t, ctx, newPG, newRedis, newApps)
	backupAndRestore(t, ctx, newBackup, oldBackup)
	assertRehearsalSchema(t, ctx, oldPG)
	require.Equal(t, newSnap, rehearsalSnapshot(t, ctx, oldPG))
	rebuildRehearsalCounter(t, ctx, oldPG, oldRedis)
	rollbackApps := activateRehearsalWriters(t, ctx, oldPG, oldRedis, oldCfg.Database, secret)
	for _, instance := range rollbackApps {
		require.Equal(t, newSnap, rehearsalSnapshot(t, ctx, instance.pg))
		require.Equal(t, "3", instance.redis.Get(ctx, rehearsalCounter).Val())
	}
	t.Logf("reconciled rollback=%s; 0 of %d committed fixture usage and dedup records lost", time.Since(rollbackStart).Round(time.Millisecond), logs)
}

func rehearsalPort(t *testing.T, key string) int {
	t.Helper()
	port, err := strconv.Atoi(os.Getenv("NEXUS_REHEARSAL_" + key))
	require.NoError(t, err)
	require.Greater(t, port, 1024)
	require.LessOrEqual(t, port, 65535)
	return port
}

func rehearsalPG(t *testing.T, ctx context.Context, cfg config.DatabaseConfig, expectBlank bool) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", cfg.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.Eventually(t, func() bool {
		probe, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return db.PingContext(probe) == nil
	}, 30*time.Second, 250*time.Millisecond, "isolated Postgres did not become ready")
	if expectBlank {
		var actualDB, role string
		var populated bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT current_database(), current_user, to_regclass('public.users') IS NOT NULL`).Scan(&actualDB, &role, &populated))
		require.Equal(t, cfg.DBName, actualDB)
		require.Equal(t, rehearsalAdmin, role)
		require.False(t, populated, "refusing to use an existing database")
	}
	return db
}

func rehearsalRedis(t *testing.T, ctx context.Context, key string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + strconv.Itoa(rehearsalPort(t, key))})
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, func() bool { return client.Ping(ctx).Err() == nil }, 20*time.Second, 200*time.Millisecond)
	size, err := client.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size, "refusing to use an existing Redis database")
	return client
}

type rehearsalEntry struct {
	pg    *sql.DB
	redis *redis.Client
}

func activateRehearsalWriters(t *testing.T, ctx context.Context, db *sql.DB, rdb *redis.Client, cfg config.DatabaseConfig, password string) []rehearsalEntry {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, rehearsalWriter).Scan(&exists))
	if !exists {
		rehearsalExec(t, ctx, db, "CREATE ROLE "+rehearsalWriter+" NOLOGIN")
	}
	rehearsalExec(t, ctx, db, "ALTER ROLE "+rehearsalWriter+" LOGIN PASSWORD '"+password+"'")
	rehearsalExec(t, ctx, db, "GRANT USAGE ON SCHEMA public TO "+rehearsalWriter)
	rehearsalExec(t, ctx, db, "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO "+rehearsalWriter)
	rehearsalExec(t, ctx, db, "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "+rehearsalWriter)
	require.NoError(t, rdb.Do(ctx, "ACL", "SETUSER", rehearsalWriter, "on", ">"+password,
		"~nexus:rehearsal:*", "+@connection", "+get", "+set").Err())
	cfg.User = rehearsalWriter
	entries := make([]rehearsalEntry, 0, 2)
	for range 2 {
		r := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr, Username: rehearsalWriter, Password: password, MaxRetries: -1})
		t.Cleanup(func() { _ = r.Close() })
		require.NoError(t, r.Ping(ctx).Err())
		entries = append(entries, rehearsalEntry{pg: rehearsalPG(t, ctx, cfg, false), redis: r})
	}
	return entries
}

func fenceRehearsalWriters(t *testing.T, ctx context.Context, db *sql.DB, redisAdmin *redis.Client, entries []rehearsalEntry) {
	t.Helper()
	rehearsalExec(t, ctx, db, "ALTER ROLE "+rehearsalWriter+" NOLOGIN")
	_, err := db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=$1`, rehearsalWriter)
	require.NoError(t, err)
	require.NoError(t, redisAdmin.Do(ctx, "ACL", "DELUSER", rehearsalWriter).Err())
	for _, entry := range entries {
		_, err := entry.pg.ExecContext(ctx, `UPDATE users SET balance=balance+999`)
		require.Error(t, err, "the former writer still has PostgreSQL access")
		require.Error(t, entry.redis.Set(ctx, rehearsalCounter, "incorrect", time.Minute).Err(), "the former writer still has Redis access")
	}
}

func loadRehearsalIDs(t *testing.T, ctx context.Context, db *sql.DB) (ids struct{ user, account, key, subscription int64 }) {
	t.Helper()
	err := db.QueryRowContext(ctx, `
		SELECT u.id,a.id,k.id,s.id
		FROM users u JOIN api_keys k ON k.user_id=u.id JOIN user_subscriptions s ON s.user_id=u.id CROSS JOIN accounts a
		WHERE u.email='backup-rehearsal@example.test' AND k.name='fixture-key' AND a.name='fixture-account'`).Scan(&ids.user, &ids.account, &ids.key, &ids.subscription)
	require.NoError(t, err)
	return ids
}

func rehearsalSnapshot(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var result string
	err := db.QueryRowContext(ctx, `
		SELECT jsonb_build_object(
			'users',(SELECT jsonb_agg(to_jsonb(u) ORDER BY u.id) FROM users u),
			'api_keys',(SELECT jsonb_agg(to_jsonb(k) ORDER BY k.id) FROM api_keys k),
			'accounts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.id) FROM accounts a),
			'groups',(SELECT jsonb_agg(to_jsonb(g) ORDER BY g.id) FROM groups g),
			'subscriptions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM user_subscriptions s),
			'channels',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM channels c),
			'usage',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.id) FROM usage_logs l),
			'dedup',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id) FROM usage_billing_dedup d)
		)::text`).Scan(&result)
	require.NoError(t, err)
	return result
}

func backupAndRestore(t *testing.T, ctx context.Context, source, target *service.BackupService) *service.BackupRecord {
	t.Helper()
	start := time.Now()
	record, err := source.CreateBackup(ctx, "local-rehearsal", 1)
	require.NoError(t, err)
	require.Equal(t, "completed", record.Status)
	require.Positive(t, record.SizeBytes)
	backupDuration := time.Since(start)
	start = time.Now()
	require.NoError(t, target.RestoreBackup(ctx, record.ID))
	t.Logf("S3 archive uploaded/restored: bytes=%d backup=%s restore=%s", record.SizeBytes,
		backupDuration.Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
	return record
}

func assertRehearsalSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	applied, err := checkClusterMigrations(ctx, conn)
	require.NoError(t, err)
	require.Greater(t, applied, 315)
	t.Logf("verified %d restored migration files/checksums", applied)
}

func rebuildRehearsalCounter(t *testing.T, ctx context.Context, db *sql.DB, cache *redis.Client) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_logs`).Scan(&count))
	require.NoError(t, cache.Set(ctx, rehearsalCounter, count, time.Minute).Err())
	if count <= 0 {
		t.Fatal("cannot rebuild Redis fixture state from an empty ledger")
	}
}

func rehearsalExec(t *testing.T, ctx context.Context, db *sql.DB, query string) {
	t.Helper()
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err)
}

const rehearsalSeed = `
	INSERT INTO users (email,password_hash,balance) VALUES ('backup-rehearsal@example.test','fixture-only',12.34);
	INSERT INTO accounts (name,platform,type,credentials) VALUES ('fixture-account','openai','apikey','{"api_key":"fixture-only-upstream"}');
	INSERT INTO groups (name) VALUES ('fixture-group');
	INSERT INTO api_keys (user_id,key,name,quota)
	  SELECT id,'fictional-rehearsal-api-key','fixture-key',10 FROM users WHERE email='backup-rehearsal@example.test';
	INSERT INTO user_subscriptions (user_id,group_id,starts_at,expires_at,notes)
	  SELECT u.id,g.id,NOW(),NOW()+interval '30 days','fixture-subscription'
	  FROM users u,groups g WHERE u.email='backup-rehearsal@example.test' AND g.name='fixture-group';
	INSERT INTO channels (name) VALUES ('fixture-channel');
	INSERT INTO channel_groups (channel_id,group_id)
	  SELECT c.id,g.id FROM channels c,groups g WHERE c.name='fixture-channel' AND g.name='fixture-group';
	INSERT INTO account_groups (account_id,group_id)
	  SELECT a.id,g.id FROM accounts a,groups g WHERE a.name='fixture-account' AND g.name='fixture-group';
`
