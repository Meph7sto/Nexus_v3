package worker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr      string
	NodeID          string
	Version         string
	SharedToken     string
	PreviousToken   string
	ProxyURL        string
	DNSMode         string
	AllowedTargets  []string
	AllowPrivateH2C bool
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	MaxInFlight     int
	MaxConnections  int
	DrainTimeout    time.Duration
}

func ConfigFromEnv() (Config, error) {
	var cfg Config
	cfg.ListenAddr = strings.TrimSpace(os.Getenv("WORKER_LISTEN_ADDR"))
	cfg.NodeID = strings.TrimSpace(os.Getenv("WORKER_NODE_ID"))
	cfg.ProxyURL = strings.TrimSpace(os.Getenv("WORKER_PROXY_URL"))
	cfg.DNSMode = strings.TrimSpace(os.Getenv("WORKER_DNS_MODE"))
	cfg.AllowedTargets = strings.FieldsFunc(os.Getenv("WORKER_ALLOWED_TARGETS"), func(r rune) bool { return r == ',' })
	cfg.TLSCertFile = strings.TrimSpace(os.Getenv("WORKER_TLS_CERT_FILE"))
	cfg.TLSKeyFile = strings.TrimSpace(os.Getenv("WORKER_TLS_KEY_FILE"))
	cfg.TLSClientCAFile = strings.TrimSpace(os.Getenv("WORKER_TLS_CLIENT_CA_FILE"))
	if raw := strings.TrimSpace(os.Getenv("WORKER_ALLOW_PRIVATE_H2C")); raw != "" {
		on, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, errors.New("WORKER_ALLOW_PRIVATE_H2C must be true or false")
		}
		cfg.AllowPrivateH2C = on
	}
	var err error
	if cfg.SharedToken, err = loadToken("WORKER_SHARED_TOKEN"); err != nil {
		return Config{}, err
	}
	if cfg.PreviousToken, err = loadToken("WORKER_PREVIOUS_SHARED_TOKEN"); err != nil {
		return Config{}, err
	}
	if cfg.MaxInFlight, err = positiveEnvInt("WORKER_MAX_INFLIGHT"); err != nil {
		return Config{}, err
	}
	if cfg.MaxConnections, err = positiveEnvInt("WORKER_MAX_CONNECTIONS"); err != nil {
		return Config{}, err
	}
	if raw := strings.TrimSpace(os.Getenv("WORKER_DRAIN_TIMEOUT")); raw != "" {
		cfg.DrainTimeout, err = time.ParseDuration(raw)
		if err != nil || cfg.DrainTimeout <= 0 {
			return Config{}, errors.New("WORKER_DRAIN_TIMEOUT must be a positive duration")
		}
	}
	return cfg, nil
}

func (cfg *Config) defaults() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:9090"
	}
	if cfg.NodeID == "" {
		cfg.NodeID = "worker"
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 32
	}
	if cfg.MaxConnections == 0 && cfg.MaxInFlight > 0 && cfg.MaxInFlight <= maxInFlight {
		cfg.MaxConnections = cfg.MaxInFlight * 2
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 2 * time.Minute
	}
}

func (cfg Config) inboundTLS() (*tls.Config, error) {
	anyTLS := cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" || cfg.TLSClientCAFile != ""
	if cfg.AllowPrivateH2C {
		if anyTLS {
			return nil, errors.New("private h2c and mTLS cannot be enabled together")
		}
		return nil, nil
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.TLSClientCAFile == "" {
		return nil, errors.New("mTLS requires the server certificate, server key and Entry client CA; private h2c requires explicit opt-in")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, errors.New("unable to load the Worker mTLS server key pair")
	}
	caPEM, err := os.ReadFile(cfg.TLSClientCAFile)
	if err != nil {
		return nil, errors.New("unable to read the Worker mTLS client CA")
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("Worker mTLS client CA has no certificates")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}

func loadToken(name string) (string, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	file := strings.TrimSpace(os.Getenv(name + "_FILE"))
	if file == "" {
		return raw, nil
	}
	if raw != "" {
		return "", fmt.Errorf("%s and %s_FILE cannot both be set", name, name)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("unable to read %s_FILE", name)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s_FILE is empty", name)
	}
	return token, nil
}

func positiveEnvInt(name string) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}
