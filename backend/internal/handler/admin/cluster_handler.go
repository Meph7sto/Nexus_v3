package admin

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ClusterHandler struct {
	cluster *service.WorkerClusterService
	data    *service.ClusterDataService
}

func NewClusterHandler(cluster *service.WorkerClusterService, data *service.ClusterDataService) *ClusterHandler {
	return &ClusterHandler{cluster: cluster, data: data}
}

type clusterVersionInput struct {
	ExpectedVersion *int64 `json:"expected_version"`
}

type clusterWorkerInput struct {
	NodeID      string   `json:"node_id"`
	Name        string   `json:"name"`
	URL         string   `json:"url"`
	UseH2C      bool     `json:"use_h2c"`
	Weight      int      `json:"weight"`
	MaxInFlight int      `json:"max_inflight"`
	Tags        []string `json:"tags"`
	Note        string   `json:"note"`
	Enabled     bool     `json:"enabled"`
	SecretRef   string   `json:"secret_ref"`
}

func (h *ClusterHandler) hasWorker(c *gin.Context) bool {
	if h == nil || h.cluster == nil {
		response.ErrorWithDetails(c, http.StatusServiceUnavailable, "Cluster controls are not enabled", "CLUSTER_UNAVAILABLE", nil)
		return false
	}
	return true
}

func (h *ClusterHandler) hasData(c *gin.Context) bool {
	if h == nil || h.data == nil {
		response.Error(c, http.StatusServiceUnavailable, "Data preflight is not available")
		return false
	}
	return true
}

func clusterBody(c *gin.Context, input any) bool {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(input); err != nil || decoder.Decode(new(any)) != io.EOF {
		response.BadRequest(c, "Invalid cluster request")
		return false
	}
	return true
}

func clusterResult(c *gin.Context, result any, err error) {
	if err == nil {
		response.Success(c, result)
		return
	}
	switch {
	case errors.Is(err, service.ErrWorkerClusterConflict):
		response.ErrorWithDetails(c, http.StatusConflict, "Cluster configuration changed; refresh and retry", "CLUSTER_VERSION_CONFLICT", nil)
		return
	case errors.Is(err, service.ErrWorkerNodeNotFound):
		response.NotFound(c, "Worker node not found")
		return
	case errors.Is(err, service.ErrWorkerClusterInvalid):
		response.BadRequest(c, "Invalid Worker configuration or unapproved binding")
		return
	case errors.Is(err, service.ErrWorkerClusterUnavailable), errors.Is(err, service.ErrWorkerClusterUninitialized):
		response.ErrorWithDetails(c, http.StatusServiceUnavailable, "Cluster controls are not enabled", "CLUSTER_UNAVAILABLE", nil)
		return
	}
	status, converted := infraerrors.ToHTTP(err)
	if status >= http.StatusInternalServerError {
		response.ErrorWithDetails(c, status, "Cluster request failed", converted.Reason, nil)
		return
	}
	response.ErrorWithDetails(c, status, converted.Message, converted.Reason, nil)
}

func clusterVersion(c *gin.Context, v *int64) bool {
	if v == nil || *v < 0 {
		response.BadRequest(c, "An expected cluster version is required")
		return false
	}
	return true
}

func (h *ClusterHandler) View(c *gin.Context) {
	if !h.hasWorker(c) {
		return
	}
	result, err := h.cluster.View(c.Request.Context())
	clusterResult(c, result, err)
}

func (h *ClusterHandler) SaveWorker(c *gin.Context) {
	if !h.hasWorker(c) {
		return
	}
	var body struct {
		clusterVersionInput
		Node *clusterWorkerInput `json:"node"`
	}
	if !clusterBody(c, &body) || !clusterVersion(c, body.ExpectedVersion) {
		return
	}
	if body.Node == nil {
		response.BadRequest(c, "A Worker node is required")
		return
	}
	node := body.Node
	node.NodeID = strings.TrimSpace(node.NodeID)
	pathID := c.Param("id")
	if pathID != "" && node.NodeID != pathID {
		response.BadRequest(c, "Worker node ID does not match the route")
		return
	}
	ctx := c.Request.Context()
	view, err := h.cluster.View(ctx)
	if err != nil {
		clusterResult(c, nil, err)
		return
	}
	exists := false
	for _, current := range view.Desired.Workers {
		if current.NodeID == node.NodeID {
			exists = true
			break
		}
	}
	switch {
	case view.Desired.Version != *body.ExpectedVersion:
		response.ErrorWithDetails(c, http.StatusConflict, "Cluster configuration changed; refresh and retry", "CLUSTER_VERSION_CONFLICT", nil)
		return
	case pathID == "" && exists:
		response.Error(c, http.StatusConflict, "Worker node already exists")
		return
	case pathID != "" && !exists:
		response.NotFound(c, "Worker node not found")
		return
	}
	snapshot, err := h.cluster.UpsertWorker(ctx, service.WorkerNode{
		NodeID: node.NodeID, Name: node.Name, URL: node.URL, UseH2C: node.UseH2C,
		Weight: node.Weight, MaxInFlight: node.MaxInFlight, Tags: node.Tags, Note: node.Note,
		Enabled: node.Enabled, SecretRef: node.SecretRef,
	}, *body.ExpectedVersion)
	clusterResult(c, snapshot, err)
}

func (h *ClusterHandler) DeleteWorker(c *gin.Context) {
	if !h.hasWorker(c) {
		return
	}
	version, err := strconv.ParseInt(c.Query("expected_version"), 10, 64)
	if err != nil || !clusterVersion(c, &version) {
		if err != nil {
			response.BadRequest(c, "An expected cluster version is required")
		}
		return
	}
	result, err := h.cluster.DeleteWorker(c.Request.Context(), c.Param("id"), version)
	clusterResult(c, result, err)
}

func (h *ClusterHandler) RetireOfflineEntry(c *gin.Context) {
	var body struct {
		FencedInstance string `json:"confirmed_fenced_instance"`
	}
	if !h.hasWorker(c) || !clusterBody(c, &body) {
		return
	}
	if body.FencedInstance != c.Param("id") {
		response.BadRequest(c, "Confirm the exact fenced Entry instance")
		return
	}
	err := h.cluster.RetireOfflineEntry(c.Request.Context(), body.FencedInstance)
	clusterResult(c, nil, err)
}

func (h *ClusterHandler) ForgetOfflineWorker(c *gin.Context) {
	var body struct {
		clusterVersionInput
		FencedWorker string `json:"confirmed_fenced_worker"`
	}
	if !h.hasWorker(c) || !clusterBody(c, &body) || !clusterVersion(c, body.ExpectedVersion) {
		return
	}
	if body.FencedWorker != c.Param("id") {
		response.BadRequest(c, "Confirm the exact fenced Worker")
		return
	}
	result, err := h.cluster.ForgetOfflineWorker(c.Request.Context(), body.FencedWorker, *body.ExpectedVersion)
	clusterResult(c, result, err)
}

func (h *ClusterHandler) UpdatePolicy(c *gin.Context) {
	if !h.hasWorker(c) {
		return
	}
	var body struct {
		clusterVersionInput
		Policy *service.WorkerPoolPolicy `json:"policy"`
	}
	if !clusterBody(c, &body) || !clusterVersion(c, body.ExpectedVersion) {
		return
	}
	if body.Policy == nil {
		response.BadRequest(c, "A Worker pool policy is required")
		return
	}
	result, err := h.cluster.UpdatePolicy(c.Request.Context(), *body.Policy, *body.ExpectedVersion)
	clusterResult(c, result, err)
}

func (h *ClusterHandler) DrainWorker(c *gin.Context) {
	if !h.hasWorker(c) {
		return
	}
	var body clusterVersionInput
	if !clusterBody(c, &body) || !clusterVersion(c, body.ExpectedVersion) {
		return
	}
	result, err := h.cluster.DrainWorker(c.Request.Context(), c.Param("id"), *body.ExpectedVersion)
	if err != nil {
		clusterResult(c, nil, err)
		return
	}
	response.Accepted(c, result)
}

func (h *ClusterHandler) DrainStatus(c *gin.Context) {
	if h.hasWorker(c) {
		result, err := h.cluster.DrainStatus(c.Request.Context(), c.Param("id"))
		clusterResult(c, result, err)
	}
}

func (h *ClusterHandler) ProbeWorker(c *gin.Context) {
	if h.hasWorker(c) {
		result, err := h.cluster.ProbeWorker(c.Request.Context(), c.Param("id"))
		clusterResult(c, result, err)
	}
}

func (h *ClusterHandler) JoinTemplate(c *gin.Context) {
	if h.hasWorker(c) {
		result, err := h.cluster.JoinTemplate(c.Request.Context(), c.Param("id"))
		clusterResult(c, gin.H{"template": result}, err)
	}
}

func (h *ClusterHandler) DataView(c *gin.Context) {
	if h.hasData(c) {
		response.Success(c, h.data.View(c.Request.Context()))
	}
}

func (h *ClusterHandler) VerifyData(c *gin.Context) {
	var input service.ClusterDataRequest
	if h.hasData(c) && clusterBody(c, &input) {
		result, err := h.data.Probe(c.Request.Context(), input)
		clusterResult(c, result, err)
	}
}

func (h *ClusterHandler) ExportData(c *gin.Context) {
	var input service.ClusterDataRequest
	if h.hasData(c) && clusterBody(c, &input) {
		result, err := h.data.Export(c.Request.Context(), input)
		clusterResult(c, result, err)
	}
}
