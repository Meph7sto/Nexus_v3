package service

import (
	"errors"
	"log/slog"
	"net/http"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
)

var ErrAccountProxyUnavailable = errors.New("configured account proxy unavailable")

func rejectBoundProxy(account *Account, reason string) error {
	slog.Warn("account.proxy_request_blocked", "account_id", account.ID, "proxy_id", *account.ProxyID, "reason", reason)
	return infraerrors.New(http.StatusServiceUnavailable, "ACCOUNT_PROXY_UNAVAILABLE", ErrAccountProxyUnavailable.Error()+": "+reason).WithCause(ErrAccountProxyUnavailable)
}

func checkedBoundProxyURL(account *Account, proxy *Proxy) (string, error) {
	if proxy == nil {
		return "", rejectBoundProxy(account, "missing proxy record")
	}
	if proxy.ID != *account.ProxyID {
		return "", rejectBoundProxy(account, "proxy ID mismatch")
	}
	raw := proxy.URL()
	if _, parsed, err := proxyurl.Parse(raw); err != nil || parsed == nil || proxy.Port < 1 || proxy.Port > 65535 {
		return "", rejectBoundProxy(account, "empty or invalid proxy URL")
	}
	return raw, nil
}

// Inference uses the scheduler's hydrated account. Only an unbound account can
// select the transport's empty-URL (direct) route.
func accountEgressProxyURL(account *Account) (string, error) {
	if account.ProxyID == nil {
		return "", nil
	}
	return checkedBoundProxyURL(account, account.Proxy)
}
