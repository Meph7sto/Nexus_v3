// Package workerproto defines the Entry-to-Worker wire format.
package workerproto

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

const (
	HealthPath = "/healthz"
	RelayPath  = "/_nexus_worker/v1/relay"
	DrainPath  = "/_nexus_worker/v1/drain"

	HeaderPrefix             = "X-Nexus-Relay-"
	HeaderTargetURL          = HeaderPrefix + "Target-URL"
	HeaderMethod             = HeaderPrefix + "Method"
	HeaderProxyID            = HeaderPrefix + "Proxy-ID"
	HeaderAccountID          = HeaderPrefix + "Account-ID"
	HeaderAccountConcurrency = HeaderPrefix + "Account-Concurrency"
	HeaderUpstreamProfile    = HeaderPrefix + "Upstream-Profile"
	HeaderTLSProfile         = HeaderPrefix + "TLS-Profile"
	HeaderRedirectsDisabled  = HeaderPrefix + "Redirects-Disabled"
	HeaderPublicHostsOnly    = HeaderPrefix + "Public-Hosts-Only"
	HeaderUpstreamHeaders    = HeaderPrefix + "Upstream-Headers"
	HeaderUpstreamStatus     = HeaderPrefix + "Upstream-Status"
	HeaderCompletion         = HeaderPrefix + "Completion"

	DefaultProxyID     = "default"
	CompletionComplete = "complete"
	MaxEncodedJSON     = 32 << 10
)

type ErrorCode string

const (
	CodeWorkerDraining     ErrorCode = "worker_draining"
	CodeWorkerOverloaded   ErrorCode = "worker_overloaded"
	CodeInvalidRequest     ErrorCode = "invalid_request"
	CodeUnauthorized       ErrorCode = "unauthorized"
	CodeTargetDenied       ErrorCode = "target_denied"
	CodeUpstreamError      ErrorCode = "upstream_error"
	CodeUpstreamPersistent ErrorCode = "upstream_persistent_error"
)

type Outcome string

const (
	OutcomeUnsent  Outcome = "unsent"
	OutcomeUnknown Outcome = "unknown"
)

type RelayError struct {
	Code    ErrorCode `json:"code"`
	Outcome Outcome   `json:"outcome"`
}

type HealthStatus string

const (
	HealthOK       HealthStatus = "ok"
	HealthDraining HealthStatus = "draining"
)

type Health struct {
	Status      HealthStatus `json:"status"`
	NodeID      string       `json:"node_id"`
	InFlight    int          `json:"in_flight"`
	MaxInFlight int          `json:"max_in_flight"`
	MemAllocMB  float64      `json:"mem_alloc_mb"`
	UptimeSec   int64        `json:"uptime_sec"`
	Version     string       `json:"version"`
}

func EncodeJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	if base64.StdEncoding.EncodedLen(len(data)) > MaxEncodedJSON {
		return "", errors.New("relay metadata exceeds size limit")
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func DecodeJSON(encoded string, v any) error {
	if len(encoded) > MaxEncodedJSON {
		return errors.New("relay metadata exceeds size limit")
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return errors.New("invalid relay metadata encoding")
	}
	if err := json.Unmarshal(data, v); err != nil {
		return errors.New("invalid relay metadata JSON")
	}
	return nil
}

// EndToEndHeaders removes fields that cannot cross the relay transport.
// Upstream Authorization is allowed only inside the encoded request metadata.
func EndToEndHeaders(src http.Header) http.Header {
	h := make(http.Header, len(src))
	blocked := make(map[string]bool)
	for name, values := range src {
		if strings.EqualFold(name, "Connection") {
			for _, value := range values {
				for _, field := range strings.Split(value, ",") {
					blocked[http.CanonicalHeaderKey(strings.TrimSpace(field))] = true
				}
			}
		}
	}
	for name, values := range src {
		key := http.CanonicalHeaderKey(name)
		if blocked[key] || strings.HasPrefix(strings.ToLower(key), strings.ToLower(HeaderPrefix)) {
			continue
		}
		switch key {
		case "Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
			"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length", "Host":
			continue
		}
		h[key] = append(h[key], values...)
	}
	return h
}
