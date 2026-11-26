//go:build unit

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type permissionsProxyService struct {
	service.AdminService
	proxy *service.Proxy
	err   error
	ids   []int64
}

func (s *permissionsProxyService) GetProxy(_ context.Context, id int64) (*service.Proxy, error) {
	s.ids = append(s.ids, id)
	return s.proxy, s.err
}

type refreshTokenClientRecorder struct {
	service.OpenAIOAuthClient
	proxyURLs []string
}

func (c *refreshTokenClientRecorder) RefreshTokenWithClientID(_ context.Context, _, proxyURL, _ string) (*openai.TokenResponse, error) {
	c.proxyURLs = append(c.proxyURLs, proxyURL)
	return &openai.TokenResponse{AccessToken: "test-access-token"}, nil
}

func TestOpenAIRefreshTokenProxyForAdminRoles(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const path = "/api/v1/admin/openai/refresh-token"
	const withProxy = `{"refresh_token":"TEST_SECRET","proxy_id":14}`
	valid := service.Proxy{ID: 14, Status: service.StatusActive, Protocol: "socks5", Host: "127.0.0.1", Port: 1080}
	inactive, expired, wrongID, invalid, unsupported := valid, valid, valid, valid, valid
	inactive.Status = "inactive"
	expiredAt := time.Now().Add(-time.Hour)
	expired.ExpiresAt = &expiredAt
	wrongID.ID++
	invalid.Port = 0
	unsupported.Protocol = "ftp"
	for _, role := range []struct {
		name   string
		policy adminaccess.Policy
	}{
		{"full access", adminaccess.Policy{FullAccess: true}},
		{"restricted", adminaccess.Policy{GlobalPermissions: []string{"openai.tokens.refresh"}}},
	} {
		t.Run(role.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, body string
				proxy      *service.Proxy
				err        error
				wantStatus int
				wantURLs   []string
			}{
				{"not found", withProxy, nil, service.ErrProxyNotFound, http.StatusBadRequest, nil},
				{"lookup error", withProxy, nil, errors.New("test lookup failure"), http.StatusBadRequest, nil},
				{"missing record", withProxy, nil, nil, http.StatusBadRequest, nil},
				{"inactive", withProxy, &inactive, nil, http.StatusBadRequest, nil},
				{"expired", withProxy, &expired, nil, http.StatusBadRequest, nil},
				{"wrong proxy", withProxy, &wrongID, nil, http.StatusBadRequest, nil},
				{"invalid port", withProxy, &invalid, nil, http.StatusBadRequest, nil},
				{"unsupported protocol", withProxy, &unsupported, nil, http.StatusBadRequest, nil},
				{"valid proxy", withProxy, &valid, nil, http.StatusOK, []string{valid.URL()}},
				{"no proxy", `{"refresh_token":"TEST_SECRET"}`, nil, nil, http.StatusOK, []string{""}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					admin := &permissionsProxyService{proxy: tc.proxy, err: tc.err}
					client := &refreshTokenClientRecorder{}
					oauth := service.NewOpenAIOAuthService(nil, client)
					t.Cleanup(oauth.Stop)
					h := NewOpenAIOAuthHandler(oauth, admin, nil, nil)
					r := gin.New()
					r.POST(path, h.RefreshToken)
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
					req = req.WithContext(adminaccess.WithPrincipal(req.Context(), &adminaccess.Principal{Policy: role.policy}))
					req.Header.Set("Content-Type", "application/json")
					w := httptest.NewRecorder()
					r.ServeHTTP(w, req)
					require.Equal(t, tc.wantURLs, client.proxyURLs)
					require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
					require.NotContains(t, w.Body.String(), "TEST_SECRET")
					if tc.body == withProxy {
						require.Equal(t, []int64{valid.ID}, admin.ids)
					} else {
						require.Empty(t, admin.ids)
					}
				})
			}
		})
	}
}

type scopedPermissionAccountService struct {
	scopedBulkAccountService
	listGroupID int64
}

func (s *scopedPermissionAccountService) ScopedAdminList(_ context.Context, _ pagination.PaginationParams, _, _, _, _ string, groupID int64) ([]service.Account, int64, error) {
	s.listGroupID = groupID
	return []service.Account{{ID: 1, Platform: "openai", Type: "oauth", GroupIDs: []int64{2}}}, 1, nil
}

func TestScopedAdminPermissionPaginationContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, query string
		groupID     int64
		invalid     bool
	}{
		{"无筛选", "page=1&page_size=100", 0, false},
		{"空分组", "group=", 0, false},
		{"补号前全局查重", "platform=openai&type=oauth&status=&group=&lite=1&include_scheduler_score=0&sort_by=id&sort_order=asc&page=1&page_size=100&timezone=Asia%2FShanghai", 0, false},
		{"监控分组", "platform=openai&type=oauth&status=&group=2&lite=1&include_scheduler_score=0&sort_by=id&sort_order=asc&page=1&page_size=100&timezone=Asia%2FShanghai", 2, false},
		{"查询限流账户", "platform=openai&type=oauth&status=rate_limited&group=2&lite=1&include_scheduler_score=0&sort_by=id&sort_order=asc&page=1&page_size=100&timezone=Asia%2FShanghai", 2, false},
		{"未分组", "group=-1", -1, false},
		{"非法负数", "group=-2", 0, true},
		{"非法文本", "group=invalid", 0, true},
		{"空白字符", "group=%20", 0, true},
		{"溢出数值", "group=9999999999999999999999", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			svc := &scopedPermissionAccountService{listGroupID: -99}
			h := &AccountHandler{adminService: svc}
			r.Use(func(c *gin.Context) {
				c.Request = c.Request.WithContext(adminaccess.WithPrincipal(c.Request.Context(), &adminaccess.Principal{Policy: adminaccess.Policy{Rules: []adminaccess.Rule{{Permissions: []string{"accounts.read"}, GroupIDs: []int64{2}, AccountTypes: []string{"oauth"}}}}}))
			})
			r.Use(h.ScopedAdminKeyAccounts)
			r.GET("/api/v1/admin/accounts", func(c *gin.Context) { t.Fatal("不应调用全权处理器") })
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/accounts?"+tc.query, nil))
			if tc.invalid {
				require.Equal(t, 400, w.Code, w.Body.String())
				require.Equal(t, int64(-99), svc.listGroupID, "无效筛选不能触达账户查询")
				return
			}
			require.Equal(t, 200, w.Code, "账户查询失败（HTTP %d）：%s", w.Code, w.Body.String())
			require.Equal(t, tc.groupID, svc.listGroupID)
			var body struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, float64(1), body.Data["pages"], "标准分页必须包含 pages")
			require.Equal(t, float64(1), body.Data["page"])
		})
	}
}

func (s *scopedPermissionAccountService) GetAccount(context.Context, int64) (*service.Account, error) {
	return &service.Account{ID: 1, Platform: "openai", Type: "oauth", GroupIDs: []int64{2, 3}, Credentials: map[string]any{"email": "test@example.com", "plan_type": "plus", "access_token": "SECRET"}, Extra: map[string]any{"email": "test@example.com", "codex_usage_updated_at": "2026-10-24T00:00:00Z", "codex_7d_used_percent": 100, "codex_7d_reset_at": "2026-10-30T00:00:00Z", "private": "SECRET"}}, nil
}

func TestScopedAdminPermissionIdentityAndUsageAreIndependent(t *testing.T) {
	for _, permissions := range [][]string{{"accounts.read"}, {"accounts.read", "accounts.identity.read"}, {"accounts.read", "accounts.usage.read"}, {"accounts.read", "accounts.identity.read", "accounts.usage.read"}} {
		r := gin.New()
		policy := adminaccess.Policy{Rules: []adminaccess.Rule{{Permissions: permissions, GroupIDs: []int64{2}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"}}}}
		h := &AccountHandler{adminService: &scopedPermissionAccountService{}}
		r.Use(func(c *gin.Context) {
			c.Request = c.Request.WithContext(adminaccess.WithPrincipal(c.Request.Context(), &adminaccess.Principal{Policy: policy}))
		})
		r.Use(h.ScopedAdminKeyAccounts)
		r.GET("/api/v1/admin/accounts/:id", func(c *gin.Context) { t.Fatal("不能调用全权读取") })
		r.GET("/api/v1/admin/accounts/:id/usage", func(c *gin.Context) { t.Fatal("不能绕过额度授权") })
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/accounts/1", nil))
		require.Equal(t, 200, w.Code)
		require.NotContains(t, w.Body.String(), "SECRET")
		require.Contains(t, w.Body.String(), `"group_ids":[2]`)
		a := adminaccess.Account{ID: 1, GroupIDs: []int64{2}, Type: "oauth", Platform: "openai"}
		require.Equal(t, policy.AllowsAccount("accounts.identity.read", a), strings.Contains(w.Body.String(), "test@example.com"))
		require.Equal(t, policy.AllowsAccount("accounts.usage.read", a), strings.Contains(w.Body.String(), "codex_7d_used_percent"))
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/accounts/1/usage?source=active&force=true", nil))
		want := 404
		if policy.AllowsAccount("accounts.usage.read", a) {
			want = 503
		} // 已授权才会触达尚未接线的额度服务。
		require.Equal(t, want, w.Code)
		policy.Rules[0].GroupIDs = []int64{99}
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/accounts/1/usage", nil))
		require.Equal(t, 404, w.Code)
	}
}

type scopedBasicGroupService struct{ service.AdminService }

func (*scopedBasicGroupService) GetGroup(context.Context, int64) (*service.Group, error) {
	return &service.Group{ID: 1, Name: "基本组", Platform: "openai", Status: "active"}, nil
}
func TestScopedAdminBasicGroupReadDoesNotReturnModels(t *testing.T) {
	policy := adminaccess.Policy{Rules: []adminaccess.Rule{{Permissions: []string{"groups.list.read"}, GroupIDs: []int64{1}, Platforms: []string{"openai"}}}}
	h := &GroupHandler{adminService: &scopedBasicGroupService{}}
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(adminaccess.WithPrincipal(c.Request.Context(), &adminaccess.Principal{Policy: policy}))
	})
	r.Use(h.ScopedAdminKeyGroupModels)
	r.GET("/api/v1/admin/groups/:id", func(c *gin.Context) { t.Fatal("不能调用完整处理器") })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/groups/1", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.JSONEq(t, `{"code":0,"message":"success","data":{"id":1,"name":"基本组","platform":"openai","status":"active"}}`, w.Body.String())
}

func TestScopedAdminPermissionRefreshRejectsMissingGlobalGrant(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, policy := range []adminaccess.Policy{{}, {Rules: []adminaccess.Rule{{Permissions: []string{"accounts.refresh"}, AllGroups: true, AccountTypes: []string{"oauth"}}}}} {
		h := &OpenAIOAuthHandler{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Request = c.Request.WithContext(adminaccess.WithPrincipal(c.Request.Context(), &adminaccess.Principal{Policy: policy}))
		})
		r.POST("/api/v1/admin/openai/refresh-token", h.RefreshToken)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/admin/openai/refresh-token", strings.NewReader(`{"refresh_token":"SECRET"}`)))
		require.Equal(t, 403, w.Code)
		require.NotContains(t, w.Body.String(), "SECRET")
	}
}
