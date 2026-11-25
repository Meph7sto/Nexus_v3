//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type workerFaultUpstream struct {
	HTTPUpstream
	fault error
	calls int
}

func (u *workerFaultUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls++
	return nil, u.fault
}

func TestWorkerFaultIsNotAnAccountFailure(t *testing.T) {
	ctx := context.Background()
	account := &Account{ID: 42, Status: StatusActive, Schedulable: true}
	for _, e := range []*HTTPUpstreamRelayError{
		{Origin: HTTPUpstreamRelayWorker, Unsent: true, Persistent: true},
		{Origin: HTTPUpstreamRelayLink, Persistent: true},
		{Origin: HTTPUpstreamRelayUpstream},
	} {
		require.Same(t, e, (&GatewayService{}).handleUpstreamTransportError(ctx, nil, account, e, OpsUpstreamErrorEvent{}))
		require.Same(t, e, (&GeminiMessagesCompatService{}).handleUpstreamTransportError(ctx, nil, account, e))
		for _, platform := range []string{PlatformGrok, PlatformOpenAI} {
			account.Platform = platform
			require.Same(t, e, (&OpenAIGatewayService{}).handleOpenAIUpstreamTransportError(ctx, nil, account, e, false))
		}
		account.Platform = PlatformAntigravity
		up := &workerFaultUpstream{fault: e}
		_, err := (&AntigravityGatewayService{}).antigravityRetryLoop(antigravityRetryLoopParams{
			ctx: ctx, account: account, httpUpstream: up, action: "generateContent", accessToken: "fixture", body: []byte("{}"),
		})
		require.Same(t, e, err)
		require.Equal(t, 1, up.calls)
	}
}
