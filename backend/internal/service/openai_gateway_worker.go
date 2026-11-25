package service

import (
	"errors"
	"io"
	"net/http"
)

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
