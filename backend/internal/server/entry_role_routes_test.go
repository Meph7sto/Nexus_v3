package server

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/routes"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestEntryRoleRouteRegistration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		role             string
		control, gateway bool
	}{
		{config.EntryRoleAll, true, true},
		{config.EntryRoleGatewayOnly, false, true},
		{config.EntryRoleControlOnly, true, false},
	} {
		t.Run(tc.role, func(t *testing.T) {
			r := gin.New()
			h := &handler.Handlers{
				Admin:    &handler.AdminHandlers{Cluster: &admin.ClusterHandler{}},
				Gateway:  &handler.GatewayHandler{},
				MCPOAuth: &handler.MCPOAuthHandler{},
			}
			cfg := &config.Config{EntryNode: config.EntryNodeConfig{Role: tc.role}}
			noop := func(c *gin.Context) { c.Next() }
			permissions := func(service.AdminPermissionResource, service.AdminPermissionAction) gin.HandlerFunc { return noop }
			registerRoutes(r, h, noop, noop, noop, permissions, noop, noop, noop, nil, nil, nil, nil, nil, nil, cfg, nil)
			registered := map[string]bool{}
			for _, route := range r.Routes() {
				registered[route.Method+" "+route.Path] = true
			}
			require.True(t, registered[http.MethodGet+" /health"])
			require.Equal(t, tc.gateway, registered[http.MethodPost+" /v1/responses"])
			require.Equal(t, tc.control, registered[http.MethodGet+" /api/v1/admin/cluster"])
			require.Equal(t, tc.control, registered[http.MethodGet+" /api/v1/auth/me"])
			if tc.control {
				require.NoError(t, routes.ValidateAdminRouteManifest(r.Routes()))
			}
		})
	}
}
