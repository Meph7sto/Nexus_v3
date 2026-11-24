package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestClusterDataHandlerAcceptsOnlyApprovedProfileReferences(t *testing.T) {
	data := service.NewClusterDataService(nil, nil, nil, nil)
	h := NewClusterHandler(nil, data)
	r := gin.New()
	r.POST("/preflight", h.VerifyData)
	for _, tc := range []struct {
		input string
		code  int
	}{
		{`{"profile_id":"unregistered","intent":"same_cluster"}`, http.StatusNotFound},
		{`{"profile_id":"unregistered","intent":"same_cluster","host":"localhost"}`, http.StatusBadRequest},
		{`{"profile_id":"unregistered","intent":"migration","password_file":"unapproved"}`, http.StatusBadRequest},
		{`{"profile_id":"unregistered","intent":"same_cluster"}{}`, http.StatusBadRequest},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/preflight", strings.NewReader(tc.input)))
		require.Equal(t, tc.code, w.Code)
		require.NotContains(t, w.Body.String(), "localhost")
	}
}

func TestClusterRecoveryRequiresExactFencingConfirmation(t *testing.T) {
	h := NewClusterHandler(&service.WorkerClusterService{}, nil)
	r := gin.New()
	r.POST("/entries/:id", h.RetireOfflineEntry)
	r.POST("/workers/:id", h.ForgetOfflineWorker)
	for _, tc := range []struct{ path, input string }{
		{"/entries/entry-old", `{}`},
		{"/entries/entry-old", `{"confirmed_fenced_instance":"entry-new"}`},
		{"/workers/worker", `{"confirmed_fenced_worker":"worker"}`},
		{"/workers/worker", `{"confirmed_fenced_worker":"other","expected_version":2}`},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.input)))
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
}
