//go:build full_chain_rehearsal

package repository

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const rehearsalGatewayKey = "sk-rehearsal-isolated-never-valid-elsewhere"

func TestLocalRehearsalChain(t *testing.T) {
	mode := os.Getenv("NEXUS_CHAIN_MODE")
	require.Contains(t, []string{"setup", "setup-capacity", "v2", "v3-local", "v3-relay", "entry-failover", "worker-failover", "entry-graceful"}, mode, "run via chain-rehearsal.ps1")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	db, err := sql.Open("postgres", "host=postgres port=5432 user=nexus_chain_fixture dbname=nexus_chain_fixture password=synthetic-fixture-db-only sslmode=disable connect_timeout=2")
	require.NoError(t, err)
	defer db.Close()
	require.Eventually(t, func() bool { return db.PingContext(ctx) == nil }, 25*time.Second, 250*time.Millisecond)
	if mode == "setup" || mode == "setup-capacity" {
		setupRealChain(t, ctx, db)
		if mode == "setup-capacity" {
			_, err := db.ExecContext(ctx, `
				UPDATE users SET concurrency=8 WHERE email='chain-fixture@example.test';
				UPDATE accounts SET concurrency=8 WHERE name='chain-fixture';`)
			require.NoError(t, err)
		}
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	if mode == "entry-failover" {
		exerciseEntryFailover(t, ctx, client)
		return
	}
	require.Eventually(t, func() bool {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		r, _ := http.NewRequestWithContext(probe, http.MethodGet, "http://entry:8080/health", nil)
		res, err := client.Do(r)
		if err != nil {
			return false
		}
		_ = res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 55*time.Second, 300*time.Millisecond, "Entry did not reach its health endpoint")
	// Allow the Entry probes to initialize before timing any variant.
	time.Sleep(3 * time.Second)
	if mode == "entry-graceful" {
		before := ledgerForChain(t, ctx, db)
		resp, input := beginFaultStream(t, ctx, client, "http://entry:8080")
		defer resp.Body.Close()
		fmt.Println("REHEARSAL_ACTIVE_NODE=entry")
		require.Eventually(t, func() bool {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://entry:8080/health", nil)
			r, err := client.Do(req)
			if err != nil {
				return true
			}
			_ = r.Body.Close()
			return false
		}, 3*time.Second, 100*time.Millisecond, "SIGTERM should close the listener")
		stoppedAccepting := time.Now()
		chunks, completed := 1, 0
		for input.Scan() {
			if strings.Contains(input.Text(), "response.output_text.delta") {
				chunks++
			}
			if strings.Contains(input.Text(), "response.completed") {
				completed++
			}
		}
		require.NoError(t, input.Err())
		require.Equal(t, 80, chunks)
		require.Equal(t, 1, completed)
		elapsed := time.Since(stoppedAccepting)
		require.Greater(t, elapsed, 5*time.Second)
		require.Eventually(t, func() bool {
			now := ledgerForChain(t, ctx, db)
			return now.usage == before.usage+1 && now.dedup == before.dedup+1 && now.balance < before.balance
		}, 8*time.Second, 100*time.Millisecond)
		t.Logf("Entry stopped accepting new connections; remaining stream completed after %s, 80 fragments and one charge", elapsed.Round(time.Millisecond))
		return
	}
	if mode == "worker-failover" {
		exerciseWorkerFailover(t, ctx, client)
		return
	}
	if concurrency := os.Getenv("NEXUS_CHAIN_CONCURRENCY"); concurrency != "" && concurrency != "0" {
		exerciseChainCapacity(t, ctx, client, db, mode, concurrency)
		return
	}

	before := ledgerForChain(t, ctx, db)
	beforeSources := chainSourceCounts(t, ctx, client)
	const calls = 3
	var ttft, duration []time.Duration
	for range calls {
		first, full := exerciseResponses(t, ctx, client, "http://entry:8080")
		ttft, duration = append(ttft, first), append(duration, full)
	}
	after := before
	require.Eventually(t, func() bool {
		after = ledgerForChain(t, ctx, db)
		return after.usage == before.usage+calls && after.dedup == before.dedup+calls
	}, 12*time.Second, 100*time.Millisecond, "usage and billing records did not settle")
	require.Less(t, after.balance, before.balance)
	require.Greater(t, after.cost, before.cost)
	require.InDelta(t, after.cost-before.cost, before.balance-after.balance, 0.00000002)

	afterSources := chainSourceCounts(t, ctx, client)
	w1 := chainSourceDelta(t, "worker-a", beforeSources, afterSources)
	w2 := chainSourceDelta(t, "worker-b", beforeSources, afterSources)
	entry := chainSourceDelta(t, "entry", beforeSources, afterSources)
	if mode == "v3-relay" {
		require.Positive(t, w1)
		require.Positive(t, w2)
		require.Equal(t, calls, w1+w2, "each request reached exactly one Worker")
		require.Zero(t, entry, "relay benchmark unexpectedly used the local fallback")
	} else {
		require.Equal(t, calls, entry)
		require.Zero(t, w1+w2)
	}
	slices.Sort(ttft)
	slices.Sort(duration)
	t.Logf("%s: requests=%d billing=%d cost=%.8f median_TTFT=%s median_stream=%s source_entry=%d source_worker_a=%d source_worker_b=%d",
		mode, calls, after.dedup-before.dedup, after.cost-before.cost, ttft[calls/2].Round(time.Millisecond),
		duration[calls/2].Round(time.Millisecond), entry, w1, w2)
}

func setupRealChain(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var empty bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT current_database()='nexus_chain_fixture' AND to_regclass('public.users') IS NULL`).Scan(&empty))
	require.True(t, empty, "refusing to seed an existing database")
	require.NoError(t, ApplyMigrations(ctx, db))
	_, err := db.ExecContext(ctx, `
		INSERT INTO users (email,password_hash,balance,concurrency) VALUES ('chain-fixture@example.test','fixture-not-login-enabled',100,4);
		INSERT INTO groups (name,platform) VALUES ('chain-fixture','openai');
		INSERT INTO proxies (name,protocol,host,port) VALUES ('chain-fixture','socks5','fixture',1080);
		INSERT INTO accounts (name,platform,type,proxy_id,concurrency,credentials,extra)
		SELECT 'chain-fixture','openai','apikey',id,4,'{"api_key":"rehearsal-upstream-token","base_url":"https://api.openai.com"}',
			'{"openai_responses_supported":true}' FROM proxies WHERE name='chain-fixture';
		INSERT INTO account_groups (account_id,group_id)
		SELECT a.id,g.id FROM accounts a,groups g WHERE a.name='chain-fixture' AND g.name='chain-fixture';
		INSERT INTO api_keys (user_id,group_id,key,name)
		SELECT u.id,g.id,'`+rehearsalGatewayKey+`','chain-fixture' FROM users u,groups g
		WHERE u.email='chain-fixture@example.test' AND g.name='chain-fixture';`)
	require.NoError(t, err)
}

func chainRequest(ctx context.Context, base, input string) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/responses",
		strings.NewReader(fmt.Sprintf(`{"model":"gpt-4o-mini","stream":true,"input":%q}`, input)))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+rehearsalGatewayKey)
	r.Header.Set("Content-Type", "application/json")
	return r, nil
}

func exerciseResponses(t *testing.T, ctx context.Context, client *http.Client, base string) (time.Duration, time.Duration) {
	t.Helper()
	r, err := chainRequest(ctx, base, "synthetic fixture")
	require.NoError(t, err)
	start := time.Now()
	res, err := client.Do(r)
	require.NoError(t, err)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 8192))
		t.Fatalf("gateway returned HTTP %d: %s", res.StatusCode, body)
	}
	scanner := bufio.NewScanner(res.Body)
	var events int
	var completed bool
	var first time.Duration
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "response.output_text.delta") {
			if events == 0 {
				first = time.Since(start)
			}
			require.Contains(t, line, fmt.Sprintf("part-%d", events))
			events++
		}
		if strings.Contains(line, "response.completed") {
			completed = true
		}
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, 8, events)
	require.True(t, completed)
	full := time.Since(start)
	require.Greater(t, full-first, 650*time.Millisecond, "response was not delivered progressively")
	return first, full
}

func chainSourceCounts(t *testing.T, ctx context.Context, client *http.Client) map[string]int {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fixture:8081/counts", nil)
	require.NoError(t, err)
	res, err := client.Do(r)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	counts := make(map[string]int)
	require.NoError(t, json.NewDecoder(res.Body).Decode(&counts))
	return counts
}

func chainSourceDelta(t *testing.T, name string, before, after map[string]int) int {
	t.Helper()
	ips, err := net.LookupIP(name)
	require.NoError(t, err)
	var total int
	for _, ip := range ips {
		key := ip.String()
		total += after[key] - before[key]
	}
	return total
}

func ledgerForChain(t *testing.T, ctx context.Context, db *sql.DB) (out struct {
	usage, dedup  int
	balance, cost float64
}) {
	t.Helper()
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM usage_logs),
		       (SELECT COUNT(*) FROM usage_billing_dedup),
		       (SELECT balance FROM users WHERE email='chain-fixture@example.test'),
		       (SELECT COALESCE(SUM(actual_cost),0) FROM usage_logs)`).Scan(&out.usage, &out.dedup, &out.balance, &out.cost))
	return out
}
