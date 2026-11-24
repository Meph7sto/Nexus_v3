//go:build full_chain_rehearsal

package repository

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func exerciseEntryFailover(t *testing.T, ctx context.Context, client *http.Client) {
	t.Helper()
	entries := []string{"http://entry:8080", "http://entry-peer:8080"}
	healthy := func(ctx context.Context, base string) bool {
		check, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(check, http.MethodGet, base+"/health", nil)
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	require.Eventually(t, func() bool { return healthy(ctx, entries[0]) && healthy(ctx, entries[1]) },
		40*time.Second, 200*time.Millisecond, "both Entries must be healthy before the test")
	time.Sleep(2 * time.Second)

	proxies := make([]*httputil.ReverseProxy, 0, len(entries))
	for _, entry := range entries {
		target, err := url.Parse(entry)
		require.NoError(t, err)
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.FlushInterval = -1
		proxies = append(proxies, proxy)
	}
	lb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i, entry := range entries {
			if healthy(r.Context(), entry) {
				w.Header().Set("X-Rehearsal-Entry", entry)
				proxies[i].ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, "no healthy Entry", http.StatusServiceUnavailable)
	}))
	defer lb.Close()
	resp, input := beginFaultStream(t, ctx, client, lb.URL)
	defer resp.Body.Close()
	require.Equal(t, entries[0], resp.Header.Get("X-Rehearsal-Entry"))
	fmt.Println("REHEARSAL_ACTIVE_NODE=entry")
	lossObserved := awaitInterruptedStream(t, input)
	require.False(t, healthy(ctx, entries[0]), "the failed Entry still accepts health requests")

	next, err := chainRequest(ctx, lb.URL, "synthetic fixture")
	require.NoError(t, err)
	recovered, err := client.Do(next)
	require.NoError(t, err)
	defer recovered.Body.Close()
	require.Equal(t, http.StatusOK, recovered.StatusCode)
	require.Equal(t, entries[1], recovered.Header.Get("X-Rehearsal-Entry"))
	second := bufio.NewScanner(recovered.Body)
	var firstDelta time.Duration
	var terminal bool
	var chunks int
	for second.Scan() {
		line := second.Text()
		if strings.Contains(line, "response.output_text.delta") {
			if chunks == 0 {
				firstDelta = time.Since(lossObserved)
			}
			chunks++
		}
		terminal = terminal || strings.Contains(line, "response.completed")
	}
	require.NoError(t, second.Err())
	require.True(t, terminal)
	require.Equal(t, 8, chunks)
	t.Logf("Entry loss: existing SSE ended without completion; LB routed new SSE to the second Entry; loss_observed_to_first_content=%s complete_after=%s",
		firstDelta.Round(time.Millisecond), time.Since(lossObserved).Round(time.Millisecond))
}

func exerciseWorkerFailover(t *testing.T, ctx context.Context, client *http.Client) {
	t.Helper()
	const entry = "http://entry:8080"
	addresses := make(map[string][]net.IP)
	for _, node := range []string{"worker-a", "worker-b"} {
		ips, err := net.LookupIP(node)
		require.NoError(t, err)
		require.NotEmpty(t, ips)
		addresses[node] = ips
	}
	delta := func(node string, a, b map[string]int) int {
		var result int
		for _, ip := range addresses[node] {
			result += b[ip.String()] - a[ip.String()]
		}
		return result
	}
	before := chainSourceCounts(t, ctx, client)
	resp, input := beginFaultStream(t, ctx, client, entry)
	defer resp.Body.Close()
	active := chainSourceCounts(t, ctx, client)
	require.Equal(t, 1, delta("worker-a", before, active)+delta("worker-b", before, active))
	failed, healthy := "worker-a", "worker-b"
	if delta(failed, before, active) == 0 {
		failed, healthy = healthy, failed
	}
	fmt.Printf("REHEARSAL_ACTIVE_NODE=%s\n", failed)
	lossObserved := awaitInterruptedStream(t, input)
	first, total := exerciseResponses(t, ctx, client, entry)
	firstAfterLoss := time.Since(lossObserved) - total + first
	after := chainSourceCounts(t, ctx, client)
	require.Equal(t, 1, delta(failed, before, after), "aborted request was replayed on the failed Worker")
	require.Equal(t, 1, delta(healthy, before, after), "new request must be handled once by the surviving Worker")
	totalCalls := 0
	for source, count := range after {
		totalCalls += count - before[source]
	}
	require.Equal(t, 2, totalCalls, "the uncertain request was retried or fell back to Entry")
	t.Logf("Worker loss: existing SSE ended without completion; new SSE sent once through %s; observed_loss_to_first_content=%s next_TTFT=%s next_stream=%s upstream_calls=2",
		healthy, firstAfterLoss.Round(time.Millisecond), first.Round(time.Millisecond), total.Round(time.Millisecond))
}

func beginFaultStream(t *testing.T, ctx context.Context, client *http.Client, base string) (*http.Response, *bufio.Scanner) {
	t.Helper()
	req, err := chainRequest(ctx, base, "nexus-rehearsal-hold")
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	input := bufio.NewScanner(resp.Body)
	for input.Scan() {
		if strings.Contains(input.Text(), "response.output_text.delta") {
			return resp, input
		}
	}
	_ = resp.Body.Close()
	t.Fatalf("no content before the fault: %v", input.Err())
	return nil, nil
}

func awaitInterruptedStream(t *testing.T, input *bufio.Scanner) time.Time {
	t.Helper()
	for input.Scan() {
		require.NotContains(t, input.Text(), "response.completed", "the interrupted stream reported completion")
	}
	return time.Now()
}
