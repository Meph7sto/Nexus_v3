//go:build full_chain_rehearsal

package repository

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type chainCapacityResult struct {
	first, total time.Duration
	err          error
}

func exerciseChainCapacity(t *testing.T, ctx context.Context, client *http.Client, db *sql.DB, mode, requested string) {
	t.Helper()
	calls, err := strconv.Atoi(requested)
	require.NoError(t, err)
	require.Contains(t, []int{4, 8}, calls)

	before := ledgerForChain(t, ctx, db)
	beforeSources := chainSourceCounts(t, ctx, client)
	fmt.Println("REHEARSAL_CAPACITY_READY")
	require.Eventually(t, func() bool {
		_, err := os.Stat("/rehearsal/capacity-go")
		return err == nil
	}, 15*time.Second, 50*time.Millisecond, "host did not start the sample")

	results := make(chan chainCapacityResult, calls)
	start := make(chan struct{})
	for range calls {
		go func() {
			<-start
			results <- capacityStream(ctx, client)
		}()
	}
	close(start)

	var timings, lifetimes []time.Duration
	for range calls {
		result := <-results
		if result.err != nil {
			t.Error(result.err)
			continue
		}
		timings = append(timings, result.first)
		lifetimes = append(lifetimes, result.total)
	}
	t.Logf("capacity completions: mode=%s completed=%d offered=%d", mode, len(timings), calls)
	require.Len(t, timings, calls)
	peak := chainUpstreamPeak(t, ctx, client)
	require.Equal(t, calls, peak, "loads did not overlap at the synthetic upstream")

	after := before
	require.Eventually(t, func() bool {
		after = ledgerForChain(t, ctx, db)
		return after.usage == before.usage+calls && after.dedup == before.dedup+calls
	}, 12*time.Second, 100*time.Millisecond, "usage and billing at sampled load did not settle")
	require.Greater(t, after.cost, before.cost)
	require.InDelta(t, after.cost-before.cost, before.balance-after.balance, 0.00000002)

	afterSources := chainSourceCounts(t, ctx, client)
	w1 := chainSourceDelta(t, "worker-a", beforeSources, afterSources)
	w2 := chainSourceDelta(t, "worker-b", beforeSources, afterSources)
	entry := chainSourceDelta(t, "entry", beforeSources, afterSources)
	if mode == "v3-relay" {
		require.Positive(t, w1)
		require.Positive(t, w2)
		require.Equal(t, calls, w1+w2)
		require.Zero(t, entry)
	} else {
		require.Equal(t, calls, entry)
		require.Zero(t, w1+w2)
	}
	slices.Sort(timings)
	slices.Sort(lifetimes)
	median := func(v []time.Duration) time.Duration { return (v[calls/2-1] + v[calls/2]) / 2 }
	t.Logf("CAPACITY mode=%s offered=%d completed=%d upstream_peak=%d billing=%d median_TTFT=%s max_TTFT=%s median_stream=%s source_entry=%d source_worker_a=%d source_worker_b=%d",
		mode, calls, len(timings), peak, after.dedup-before.dedup, median(timings).Round(time.Millisecond),
		timings[calls-1].Round(time.Millisecond), median(lifetimes).Round(time.Millisecond), entry, w1, w2)
}

func capacityStream(ctx context.Context, client *http.Client) chainCapacityResult {
	r, err := chainRequest(ctx, "http://entry:8080", "nexus-rehearsal-capacity")
	if err != nil {
		return chainCapacityResult{err: err}
	}
	start := time.Now()
	res, err := client.Do(r)
	if err != nil {
		return chainCapacityResult{err: err}
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return chainCapacityResult{err: fmt.Errorf("gateway returned HTTP %d at sampled load: %s", res.StatusCode, body)}
	}
	input := bufio.NewScanner(res.Body)
	var first time.Duration
	var chunks int
	var terminal bool
	for input.Scan() {
		line := input.Text()
		if strings.Contains(line, "response.output_text.delta") {
			if !strings.Contains(line, fmt.Sprintf("part-%d ", chunks)) {
				return chainCapacityResult{err: fmt.Errorf("unexpected content fragment at index %d", chunks)}
			}
			if chunks == 0 {
				first = time.Since(start)
			}
			chunks++
		}
		terminal = terminal || strings.Contains(line, "response.completed")
	}
	if err := input.Err(); err != nil {
		return chainCapacityResult{err: err}
	}
	total := time.Since(start)
	if !terminal || chunks != 40 || total-first < 4*time.Second {
		return chainCapacityResult{err: errors.New("stream was incomplete or did not deliver progressive content")}
	}
	return chainCapacityResult{first: first, total: total}
}

func chainUpstreamPeak(t *testing.T, ctx context.Context, client *http.Client) int {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fixture:8081/capacity", nil)
	require.NoError(t, err)
	res, err := client.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	var peak int
	require.NoError(t, json.NewDecoder(res.Body).Decode(&peak))
	return peak
}
