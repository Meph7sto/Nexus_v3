package worker

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestConfigReadsNamedSecretFilesAndDefaultsToSmallPool(t *testing.T) {
	for _, name := range []string{
		"LISTEN_ADDR", "NODE_ID", "SHARED_TOKEN", "SHARED_TOKEN_FILE",
		"PREVIOUS_SHARED_TOKEN", "PREVIOUS_SHARED_TOKEN_FILE", "PROXY_URL",
		"ALLOWED_TARGETS", "ALLOW_PRIVATE_H2C", "TLS_CERT_FILE", "TLS_KEY_FILE", "TLS_CLIENT_CA_FILE",
		"MAX_INFLIGHT", "MAX_CONNECTIONS", "DRAIN_TIMEOUT",
	} {
		t.Setenv("WORKER_"+name, "")
	}
	dir := t.TempDir()
	t.Setenv("WORKER_SHARED_TOKEN_FILE", writeFixture(t, dir, "primary", []byte(testToken+"\n")))
	t.Setenv("WORKER_PREVIOUS_SHARED_TOKEN_FILE", writeFixture(t, dir, "previous", []byte(testOldToken+"\n")))
	t.Setenv("WORKER_PROXY_URL", "socks5://127.0.0.1:2080")
	t.Setenv("WORKER_ALLOWED_TARGETS", "api.example.com, api2.example.com:444")
	t.Setenv("WORKER_ALLOW_PRIVATE_H2C", "true")
	t.Setenv("WORKER_DRAIN_TIMEOUT", "10s")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SharedToken != testToken || cfg.PreviousToken != testOldToken || cfg.DrainTimeout != 10*time.Second {
		t.Fatal("Worker did not read the expected file-based tokens or drain duration")
	}
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.addr != "127.0.0.1:9090" || s.maxInFlight != 32 || s.proxyURL != "socks5h://127.0.0.1:2080" {
		t.Fatal("Worker defaults do not match the small-pool and loopback startup settings")
	}
	t.Setenv("WORKER_SHARED_TOKEN", testToken)
	if _, err := ConfigFromEnv(); err == nil || strings.Contains(err.Error(), testToken) {
		t.Fatal("ambiguous secret configuration must be rejected without logging its value")
	}
}

func TestConfigRejectsOutOfBoundsInFlightBeforeSizingConnections(t *testing.T) {
	cfg := testConfig()
	cfg.MaxInFlight = math.MaxInt
	cfg.defaults()
	if cfg.MaxConnections != 0 {
		t.Fatal("capacity multiplication must not overflow")
	}
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("invalid Worker capacity must be rejected")
	}
}
