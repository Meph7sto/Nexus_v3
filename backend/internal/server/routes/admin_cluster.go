package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
)

func registerClusterRoutes(admin *gin.RouterGroup, h *handler.Handlers, stepUpAuth middleware.StepUpAuthMiddleware) {
	cluster := admin.Group("/cluster")
	step := gin.HandlerFunc(stepUpAuth)
	cluster.GET("", h.Admin.Cluster.View)
	cluster.POST("/workers", step, h.Admin.Cluster.SaveWorker)
	cluster.PUT("/workers/:id", step, h.Admin.Cluster.SaveWorker)
	cluster.DELETE("/workers/:id", step, h.Admin.Cluster.DeleteWorker)
	cluster.POST("/workers/:id/probe", step, h.Admin.Cluster.ProbeWorker)
	cluster.POST("/workers/:id/drain", step, h.Admin.Cluster.DrainWorker)
	cluster.POST("/workers/:id/forget-offline", step, h.Admin.Cluster.ForgetOfflineWorker)
	cluster.POST("/entries/:id/retire-offline", step, h.Admin.Cluster.RetireOfflineEntry)
	cluster.GET("/workers/:id/drain", h.Admin.Cluster.DrainStatus)
	cluster.POST("/workers/:id/join", step, h.Admin.Cluster.JoinTemplate)
	cluster.PUT("/policy", step, h.Admin.Cluster.UpdatePolicy)
	cluster.GET("/data", h.Admin.Cluster.DataView)
	cluster.POST("/data/preflight", step, h.Admin.Cluster.VerifyData)
	cluster.POST("/data/export", step, h.Admin.Cluster.ExportData)
}
