package workerproto

import (
	"net/http"
	"strings"
	"testing"
)

func TestProxyMetadataAndCredentialBoundary(t *testing.T) {
	for _, v := range []ProxySpec{{Mode: "direct"}, {Mode: "url", URL: "socks5h://user:fixture@proxy:1080"}} {
		encoded, err := EncodeProxy(v.URL)
		if err != nil {
			t.Fatal(err)
		}
		var got ProxySpec
		if err = DecodeJSON(encoded, &got); err != nil || got != v {
			t.Fatal("proxy metadata did not round trip")
		}
	}
	for _, bad := range []string{"invalid:credential", strings.Repeat("credential", MaxEncodedJSON)} {
		var dest ProxySpec
		if err := DecodeJSON(bad, &dest); err == nil || strings.Contains(err.Error(), "credential") {
			t.Fatal("invalid metadata was not safely rejected")
		}
	}
	meta := EndToEndHeaders(http.Header{HeaderProxy: {"secret"}, "Proxy-Authorization": {"secret"}, "Authorization": {"Bearer test"}, "Connection": {"X-Local"}, "X-Local": {"test"}})
	if len(meta) != 1 || meta.Get("Authorization") != "Bearer test" {
		t.Fatal("unexpected end-to-end headers")
	}
}
