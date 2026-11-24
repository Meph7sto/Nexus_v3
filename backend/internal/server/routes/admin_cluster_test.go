package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAdminClusterMutationsRequireHumanAndStepUp(t *testing.T) {
	prefix := "/api/v1/admin/cluster"
	router := gin.New()
	RegisterAdminRoutes(router.Group("/api/v1"), &handler.Handlers{Admin: &handler.AdminHandlers{}},
		middleware.AdminAuthMiddleware(func(c *gin.Context) {
			kind := middleware.PrincipalKindHuman
			if c.GetHeader("X-Test-Principal") == "machine" {
				kind = middleware.PrincipalKindAdminAPIKey
			}
			c.Set(string(middleware.ContextKeyUserRole), service.RoleSuperAdmin)
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, PrincipalKind: kind})
			c.Next()
		}), allowAllAdminPermissionsForRouteTest(),
		middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }),
		middleware.StepUpAuthMiddleware(func(c *gin.Context) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"reason": "STEP_UP_REQUIRED"})
		}), nil, nil)

	for _, endpoint := range []struct{ method, path string }{
		{http.MethodPost, "/workers"},
		{http.MethodPut, "/workers/worker-1"},
		{http.MethodDelete, "/workers/worker-1"},
		{http.MethodPost, "/workers/worker-1/probe"},
		{http.MethodPost, "/workers/worker-1/drain"},
		{http.MethodPost, "/workers/worker-1/join"},
		{http.MethodPut, "/policy"},
		{http.MethodPost, "/data/preflight"},
		{http.MethodPost, "/data/export"},
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(endpoint.method, prefix+endpoint.path, nil))
		require.Equal(t, http.StatusForbidden, w.Code, endpoint.method+endpoint.path)
		require.Contains(t, w.Body.String(), "STEP_UP_REQUIRED")
		r := httptest.NewRequest(endpoint.method, prefix+endpoint.path, nil)
		r.Header.Set("X-Test-Principal", "machine")
		w = httptest.NewRecorder()
		router.ServeHTTP(w, r)
		require.Equal(t, http.StatusForbidden, w.Code, endpoint.method+endpoint.path)
		require.Contains(t, w.Body.String(), "MACHINE_CREDENTIAL_FORBIDDEN")
	}
}
