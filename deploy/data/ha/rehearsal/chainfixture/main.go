// chainfixture serves a synthetic OpenAI response over TLS behind a
// single-destination SOCKS endpoint. It is used only on the rehearsal network.
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const host = "api.openai.com"
const pinnedIPv4 = "1.1.1.1"

type relayTraffic struct {
	mu             sync.Mutex
	tunnels        map[string]string
	counts         map[string]int
	capacityActive int
	capacityPeak   int
}

func main() {
	dir := flag.String("dir", "/rehearsal", "fixture certificate directory")
	generate := flag.Bool("generate", false, "create fixture-only TLS material")
	flag.Parse()
	if *generate {
		if err := generateFixtureCert(*dir); err != nil {
			log.Fatal(err)
		}
		return
	}
	traffic := &relayTraffic{tunnels: make(map[string]string), counts: make(map[string]int)}
	go func() { log.Fatal(traffic.serveSOCKS()) }()
	admin := http.NewServeMux()
	admin.HandleFunc("GET /counts", func(w http.ResponseWriter, _ *http.Request) {
		traffic.mu.Lock()
		defer traffic.mu.Unlock()
		_ = json.NewEncoder(w).Encode(traffic.counts)
	})
	admin.HandleFunc("GET /capacity", func(w http.ResponseWriter, _ *http.Request) {
		traffic.mu.Lock()
		defer traffic.mu.Unlock()
		_ = json.NewEncoder(w).Encode(traffic.capacityPeak)
	})
	go func() {
		log.Fatal((&http.Server{Addr: ":8081", Handler: admin, ReadHeaderTimeout: time.Second}).ListenAndServe())
	}()
	responses := http.NewServeMux()
	responses.HandleFunc("POST /v1/responses", traffic.respond)
	server := &http.Server{Addr: "127.0.0.1:8443", Handler: responses, ReadHeaderTimeout: time.Second}
	log.Fatal(server.ListenAndServeTLS(filepath.Join(*dir, "fixture-ca.pem"), filepath.Join(*dir, "fixture-key.pem")))
}

func (t *relayTraffic) respond(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer rehearsal-upstream-token" || (r.Host != host && r.Host != host+":443") {
		http.Error(w, "fixture authentication rejected", http.StatusUnauthorized)
		return
	}
	var input struct {
		Model  string          `json:"model"`
		Stream bool            `json:"stream"`
		Input  json.RawMessage `json:"input"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&input); err != nil ||
		input.Model != "gpt-4o-mini" || !input.Stream {
		http.Error(w, "unknown fixture input", http.StatusBadRequest)
		return
	}
	capacity := bytes.Contains(input.Input, []byte("nexus-rehearsal-capacity"))
	t.mu.Lock()
	t.counts[t.tunnels[r.RemoteAddr]]++
	if capacity {
		t.capacityActive++
		t.capacityPeak = max(t.capacityPeak, t.capacityActive)
	}
	t.mu.Unlock()
	if capacity {
		defer func() {
			t.mu.Lock()
			t.capacityActive--
			t.mu.Unlock()
		}()
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Request-ID", fmt.Sprintf("req_fixture_%d", time.Now().UnixNano()))
	flush := http.NewResponseController(w)
	time.Sleep(75 * time.Millisecond)
	chunks := 8
	if capacity {
		chunks = 40
	} else if bytes.Contains(input.Input, []byte("nexus-rehearsal-hold")) {
		chunks = 80
	}
	for i := range chunks {
		if _, err := fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_fixture\",\"output_index\":0,\"content_index\":0,\"delta\":\"part-%d \"}\n\n", i); err != nil || flush.Flush() != nil {
			return
		}
		time.Sleep(120 * time.Millisecond)
	}
	_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"gpt-4o-mini\",\"usage\":{\"input_tokens\":120,\"output_tokens\":32,\"total_tokens\":152}}}\n\n")
	_ = flush.Flush()
}

func (t *relayTraffic) serveSOCKS() error {
	listener, err := net.Listen("tcp", ":1080")
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		in, err := listener.Accept()
		if err != nil {
			return err
		}
		go t.tunnel(in)
	}
}

func (t *relayTraffic) tunnel(in net.Conn) {
	defer in.Close()
	_ = in.SetDeadline(time.Now().Add(3 * time.Second))
	destination, err := readSOCKS(in)
	if err != nil || (destination != host && destination != pinnedIPv4) {
		_, _ = in.Write([]byte{5, 2, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	out, err := net.DialTimeout("tcp", "127.0.0.1:8443", time.Second)
	if err != nil {
		return
	}
	defer out.Close()
	peer, _, _ := net.SplitHostPort(in.RemoteAddr().String())
	key := out.LocalAddr().String()
	t.mu.Lock()
	t.tunnels[key] = peer
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.tunnels, key)
		t.mu.Unlock()
	}()
	if _, err := in.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		return
	}
	_ = in.SetDeadline(time.Time{})
	go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
	_, _ = io.Copy(in, out)
}

func readSOCKS(in net.Conn) (string, error) {
	var header [4]byte
	if _, err := io.ReadFull(in, header[:2]); err != nil || header[0] != 5 {
		return "", fmt.Errorf("invalid SOCKS greeting")
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(in, methods); err != nil || !bytes.Contains(methods, []byte{0}) {
		return "", fmt.Errorf("unsupported SOCKS authentication")
	}
	if _, err := in.Write([]byte{5, 0}); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(in, header[:]); err != nil || header[0] != 5 || header[1] != 1 {
		return "", fmt.Errorf("unsupported SOCKS command")
	}
	var addr []byte
	switch header[3] {
	case 1:
		addr = make([]byte, 4)
	case 3:
		if _, err := io.ReadFull(in, header[:1]); err != nil {
			return "", err
		}
		addr = make([]byte, int(header[0]))
	default:
		return "", fmt.Errorf("unsupported SOCKS address")
	}
	addrType := header[3]
	if _, err := io.ReadFull(in, addr); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(in, header[:2]); err != nil || binary.BigEndian.Uint16(header[:2]) != 443 {
		return "", fmt.Errorf("unexpected destination port")
	}
	if addrType == 1 {
		return net.IP(addr).String(), nil
	}
	return string(addr), nil
}

func generateFixtureCert(dir string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{host}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture-ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "fixture-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600)
}
