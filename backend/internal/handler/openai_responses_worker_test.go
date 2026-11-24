//go:build unit

package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type responsesWorkerAccounts struct {
	openAIImagesFailoverAccountRepo
	cooldowns int
}

func (r *responsesWorkerAccounts) SetTempUnschedulable(context.Context, int64, time.Time, string) error {
	r.cooldowns++
	return nil
}

type responsesWorkerTestTransport struct {
	service.HTTPUpstream
	do func(*http.Request, int64) (*http.Response, error)
}

func (u responsesWorkerTestTransport) Do(r *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	return u.do(r, accountID)
}

func responsesWorkerTestHandler(t *testing.T, upstream service.HTTPUpstream, firstOutputSeconds int) (*OpenAIGatewayHandler, *responsesWorkerAccounts, <-chan *service.UsageLog) {
	t.Helper()
	proxyID := int64(10)
	account := service.Account{
		ID: 1, Name: "worker-pilot", Platform: service.PlatformOpenAI,
		Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "example-upstream-key"},
		ProxyID:     &proxyID,
		Proxy:       &service.Proxy{ID: proxyID, Protocol: "socks5", Host: "proxy.example", Port: 1080},
	}
	other := account
	other.ID = 2
	other.Priority = 1
	accounts := &responsesWorkerAccounts{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: []service.Account{account, other}}}
	usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 4)}
	cfg := &config.Config{RunMode: config.RunModeSimple, WorkerPool: config.WorkerPoolConfig{Enabled: true}}
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = firstOutputSeconds
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	gateway := service.NewOpenAIGatewayService(
		accounts, usage, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{},
		nil, nil, nil, nil, nil, nil, nil,
	)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	h.maxAccountSwitches = 2
	return h, accounts, usage.created
}

func responsesWorkerStreamRequest(c *gin.Context) {
	payload := `{"model":"gpt-5.1","stream":true,"input":"hello"}`
	c.Request.Body = io.NopCloser(strings.NewReader(payload))
	c.Request.ContentLength = int64(len(payload))
}

func TestResponsesWorkerUnknownHeadersNeverSwitchOrCool(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var sent []int64
	h, accounts, usage := responsesWorkerTestHandler(t, responsesWorkerTestTransport{do: func(r *http.Request, id int64) (*http.Response, error) {
		require.True(t, service.DistributedHTTPUpstreamEnabled(r.Context()))
		require.Equal(t, "https://api.openai.com/v1/responses", r.URL.String())
		sent = append(sent, id)
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
			t.Fatal("header wait deadline did not release the attempt")
		}
		return nil, &service.HTTPUpstreamRelayError{Origin: service.HTTPUpstreamRelayLink, Err: errors.New("relay connection refused")}
	}}, 1)
	c, rec := newOpenAIResponsesFailoverTestContext(t, context.Background())
	responsesWorkerStreamRequest(c)
	h.Responses(c)

	require.Equal(t, []int64{1}, sent)
	require.Zero(t, accounts.cooldowns)
	require.Empty(t, usage)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "upstream_outcome_unknown")
	require.Equal(t, "false", rec.Header().Get("x-should-retry"))
}

func TestResponsesWorkerDrainAfterClientDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name     string
		complete bool
	}{
		{name: "late usage is recorded once", complete: true},
		{name: "heartbeats do not extend the drain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, cancel := context.WithCancel(context.Background())
			defer cancel()
			released := make(chan struct{})
			var sent int
			h, accounts, usage := responsesWorkerTestHandler(t, responsesWorkerTestTransport{do: func(r *http.Request, _ int64) (*http.Response, error) {
				require.True(t, service.DistributedHTTPUpstreamEnabled(r.Context()))
				sent++
				attempt, release := httputil.NewUpstreamAttemptContext(r.Context(), 100*time.Millisecond)
				reader, writer := io.Pipe()
				go func() {
					defer close(released)
					defer release()
					defer writer.Close()
					_, _ = fmt.Fprint(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
					cancel()
					if tc.complete {
						_, _ = fmt.Fprint(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_pilot\",\"model\":\"gpt-5.1\",\"usage\":{\"input_tokens\":8,\"output_tokens\":2}}}\n\n")
						return
					}
					ticker := time.NewTicker(15 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-attempt.Done():
							_ = writer.CloseWithError(attempt.Err())
							return
						case <-ticker.C:
							if _, err := fmt.Fprint(writer, ": ping\n\n"); err != nil {
								return
							}
						}
					}
				}()
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req_pilot"}},
					Body:       reader,
				}
				service.MarkDistributedHTTPUpstreamResponse(resp, r)
				return resp, nil
			}}, 0)
			c, _ := newOpenAIResponsesFailoverTestContext(t, client)
			responsesWorkerStreamRequest(c)
			h.Responses(c)
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("upstream drain did not release")
			}
			require.Equal(t, 1, sent)
			require.Zero(t, accounts.cooldowns)
			if tc.complete {
				require.Len(t, usage, 1)
				log := <-usage
				require.Equal(t, 8, log.InputTokens)
				require.Equal(t, 2, log.OutputTokens)
			} else {
				require.Empty(t, usage)
			}
		})
	}
}
