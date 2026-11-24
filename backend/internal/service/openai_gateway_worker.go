package service

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Only the native HTTP Responses forwarder calls this method. Converted,
// passthrough and probe requests must not opt in even at the same upstream URL.
func (s *OpenAIGatewayService) allowsOpenAIResponsesWorker(c *gin.Context, account *Account, req *http.Request) bool {
	if s.cfg == nil || (!s.cfg.WorkerPool.Enabled && !s.cfg.EntryNode.ClusterEnabled) ||
		account == nil || !account.IsOpenAIApiKey() || account.UsesOpenAICodexProtocol() ||
		account.IsTLSFingerprintEnabled() ||
		c == nil || c.Request == nil || c.Request.URL == nil ||
		c.Request.Method != http.MethodPost || c.Request.URL.EscapedPath() != "/v1/responses" ||
		req.Method != http.MethodPost || req.URL.Scheme != "https" ||
		!strings.EqualFold(req.URL.Host, "api.openai.com") || req.URL.EscapedPath() != "/v1/responses" ||
		req.URL.RawQuery != "" || req.Host != req.URL.Host {
		return false
	}
	return account.Proxy != nil && (account.Proxy.FallbackMode == "" || account.Proxy.FallbackMode == FallbackModeNone)
}

func openAIWorkerResponseInterrupted(err error) error {
	if IsHTTPUpstreamRelayTerminal(err) {
		return err
	}
	return &HTTPUpstreamRelayError{Origin: HTTPUpstreamRelayLink, Err: err}
}

func (s *OpenAIGatewayService) readOpenAIResponsesErrorBody(resp *http.Response) ([]byte, error) {
	if !isDistributedHTTPUpstreamResponse(resp) {
		return s.readUpstreamErrorBody(resp), nil
	}
	limit := openAIUpstreamErrorBodyReadLimitForConfig(s.cfg)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err == nil && int64(len(body)) > limit {
		err = errors.New("relayed upstream error body exceeds the read limit")
	}
	if err != nil {
		return nil, openAIWorkerResponseInterrupted(err)
	}
	return body, nil
}
