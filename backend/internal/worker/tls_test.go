package worker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/transport/upstream"
	"github.com/Wei-Shaw/sub2api/internal/workerproto"
)

func TestWorkerRequiresMTLSAndBearerUnlessPrivateH2CExplicit(t *testing.T) {
	cfg := testConfig()
	cfg.AllowPrivateH2C = false
	f := forwardFunc(func(*http.Request, string, int64, int, upstream.RequestOptions) (*http.Response, error) {
		t.Error("healthcheck must not call an upstream")
		return nil, nil
	})
	if _, err := New(cfg, f); err == nil {
		t.Fatal("missing TLS config should not silently start h2c")
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caCert := x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caCert, &caCert, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid fixture CA")
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := tlsFixtureCert(t, ca, caKey, 2, x509.ExtKeyUsageServerAuth, []net.IP{net.IPv4(127, 0, 0, 1)})
	clientCert, clientKey := tlsFixtureCert(t, ca, caKey, 3, x509.ExtKeyUsageClientAuth, nil)
	clientPair, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg.TLSCertFile = writeFixture(t, dir, "server.pem", serverCert)
	cfg.TLSKeyFile = writeFixture(t, dir, "server-key.pem", serverKey)
	cfg.TLSClientCAFile = writeFixture(t, dir, "ca.pem", caPEM)
	s, err := New(cfg, f)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	base := "https://" + l.Addr().String()
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	withoutTLSClient := &http.Client{
		Transport: &http.Transport{
			Protocols: protocols, TLSClientConfig: &tls.Config{RootCAs: roots},
		},
		Timeout: 3 * time.Second,
	}
	defer withoutTLSClient.CloseIdleConnections()
	authenticated := func(token string) *http.Request {
		r, _ := http.NewRequest(http.MethodGet, base+workerproto.HealthPath, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	if resp, err := withoutTLSClient.Do(authenticated(testToken)); err == nil {
		_ = resp.Body.Close()
		t.Fatal("Worker accepted a TLS connection without an Entry certificate")
	}
	withTLSClient := &http.Client{
		Transport: &http.Transport{
			Protocols: protocols, TLSClientConfig: &tls.Config{
				RootCAs: roots, Certificates: []tls.Certificate{clientPair},
			},
		},
		Timeout: 3 * time.Second,
	}
	defer withTLSClient.CloseIdleConnections()
	assertRelayError(t, withTLSClient, authenticated("wrong"), http.StatusUnauthorized,
		workerproto.CodeUnauthorized, workerproto.OutcomeUnsent)
	if code, health := readHealth(t, withTLSClient, base, testToken); code != http.StatusOK || health.Status != workerproto.HealthOK {
		t.Fatalf("certified, authenticated healthcheck failed: %d %+v", code, health)
	}
}

func writeFixture(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func tlsFixtureCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serial int64, usage x509.ExtKeyUsage, ips []net.IP) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		IPAddresses: ips, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}
