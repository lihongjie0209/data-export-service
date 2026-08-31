package httptransport

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lihongjie0209/data-export-service/internal/apperror"
	"github.com/lihongjie0209/data-export-service/internal/buildinfo"
	platformexport "github.com/lihongjie0209/data-export-service/internal/export"
	"github.com/lihongjie0209/data-export-service/internal/health"
)

type Handler struct {
	logger  *slog.Logger
	health  *health.Service
	exports *platformexport.Service
}

func NewHandler(healthService *health.Service, exportService *platformexport.Service, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, exports: exportService, logger: logger}
}

type MeResponseBody struct {
	Subject string `json:"subject"`
}

// Login godoc
// @Summary Issue a JWT access token
// @Tags authentication
// @Accept json
// @Produce json
// @Param request body LoginRequest true "Client credentials"
// @Success 200 {object} Response{body=LoginResponseBody}
// @Failure 400 {object} Response "Code 10001: invalid request"
// @Failure 401 {object} Response "Code 20001: invalid credentials"
// @Failure 429 {object} Response "Code 10029: rate limited"

// Live godoc
// @Summary Check process liveness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Router /live [post]
func (h *Handler) Live(c *gin.Context) { OK(c, h.health.Live()) }

// Ready godoc
// @Summary Check database and Redis readiness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Failure 503 {object} Response{body=health.Status} "Code 50003: dependency unavailable"
// @Router /ready [post]
func (h *Handler) Ready(c *gin.Context) {
	status, ready := h.health.Ready(c.Request.Context())
	if !ready {
		c.JSON(503, Response{Code: apperror.CodeDependencyUnavailable, Message: "service is not ready", Body: status, RequestID: requestID(c)})
		return
	}
	OK(c, status)
}

// Me godoc
// @Summary Return the authenticated subject
// @Tags authentication
// @Produce json
// @Security Bearer
// @Success 200 {object} Response{body=MeResponseBody}
// @Failure 401 {object} Response "Code 20001: unauthorized"
// @Router /api/v1/me [post]
func (h *Handler) Me(c *gin.Context) {
	subject, _ := c.Get("subject")
	OK(c, gin.H{"subject": subject})
}

// Version godoc
// @Summary Return build and runtime version information
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=buildinfo.Info}
// @Router /api/v1/version [post]
func (h *Handler) Version(c *gin.Context) { OK(c, buildinfo.Current()) }

type CreateExportRequest struct {
	TenantID        string          `json:"tenant_id"`
	DatasetCode     string          `json:"dataset_code"`
	ProviderService string          `json:"provider_service"`
	Format          string          `json:"format"`
	Filename        string          `json:"filename"`
	Query           json.RawMessage `json:"query" swaggertype:"object"`
	SelectedColumns []string        `json:"selected_columns"`
	IdempotencyKey  string          `json:"idempotency_key"`
}
type GetExportRequest struct {
	TenantID string `json:"tenant_id"`
	ID       string `json:"id"`
}
type ListExportsRequest struct {
	TenantID    string     `json:"tenant_id"`
	Status      string     `json:"status"`
	DatasetCode string     `json:"dataset_code"`
	CreatedFrom *time.Time `json:"created_from"`
	CreatedTo   *time.Time `json:"created_to"`
	Page        int32      `json:"page"`
	PageSize    int32      `json:"page_size"`
}
type VersionedExportRequest struct {
	TenantID string `json:"tenant_id"`
	ID       string `json:"id"`
	Version  int64  `json:"version"`
}
type RetryExportRequest struct {
	TenantID       string `json:"tenant_id"`
	ID             string `json:"id"`
	Version        int64  `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
}
type DownloadExportRequest struct {
	TenantID   string `json:"tenant_id"`
	ID         string `json:"id"`
	TTLSeconds int32  `json:"ttl_seconds"`
}

func (h *Handler) bind(c *gin.Context, target any) bool {
	if err := c.ShouldBindJSON(target); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid JSON request", err))
		return false
	}
	return true
}

// CreateExport godoc
// @Summary Create an asynchronous export job
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body CreateExportRequest true "Export definition"
// @Success 200 {object} Response
// @Router /api/v1/exports/create [post]
func (h *Handler) CreateExport(c *gin.Context) {
	var r CreateExportRequest
	if !h.bind(c, &r) {
		return
	}
	columns, _ := json.Marshal(r.SelectedColumns)
	value, duplicate, err := h.exports.Create(c.Request.Context(), platformexport.CreateInput{TenantID: r.TenantID, DatasetCode: r.DatasetCode, ProviderService: r.ProviderService, Format: r.Format, Filename: r.Filename, QueryJSON: string(r.Query), SelectedColumnsJSON: string(columns), IdempotencyKey: r.IdempotencyKey})
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"job": value, "duplicate": duplicate})
}

// GetExport godoc
// @Summary Get an export job
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body GetExportRequest true "Job selector"
// @Success 200 {object} Response
// @Router /api/v1/exports/get [post]
func (h *Handler) GetExport(c *gin.Context) {
	var r GetExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.Get(c.Request.Context(), r.TenantID, r.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, value)
}

// ListExports godoc
// @Summary Search export jobs for frontend pages
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ListExportsRequest true "Filters and pagination"
// @Success 200 {object} Response
// @Router /api/v1/exports/list [post]
func (h *Handler) ListExports(c *gin.Context) {
	var r ListExportsRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.List(c.Request.Context(), platformexport.ListFilter{TenantID: r.TenantID, Status: r.Status, DatasetCode: r.DatasetCode, CreatedFrom: r.CreatedFrom, CreatedTo: r.CreatedTo, Page: r.Page, PageSize: r.PageSize})
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	page, pageSize := r.Page, r.PageSize
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}
	OK(c, gin.H{"items": value.Items, "total": value.Total, "page": page, "page_size": pageSize})
}

// CancelExport godoc
// @Summary Cancel a queued or running export with optimistic locking
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body VersionedExportRequest true "Job and current version"
// @Success 200 {object} Response
// @Router /api/v1/exports/cancel [post]
func (h *Handler) CancelExport(c *gin.Context) {
	var r VersionedExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.Cancel(c.Request.Context(), r.TenantID, r.ID, r.Version)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, value)
}

// RetryExport godoc
// @Summary Retry a failed or canceled export
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body RetryExportRequest true "Retry request"
// @Success 200 {object} Response
// @Router /api/v1/exports/retry [post]
func (h *Handler) RetryExport(c *gin.Context) {
	var r RetryExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, duplicate, err := h.exports.Retry(c.Request.Context(), r.TenantID, r.ID, r.Version, r.IdempotencyKey)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"job": value, "duplicate": duplicate})
}

// DownloadExport godoc
// @Summary Create a short-lived download URL
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body DownloadExportRequest true "Download request"
// @Success 200 {object} Response
// @Router /api/v1/exports/download [post]
func (h *Handler) DownloadExport(c *gin.Context) {
	var r DownloadExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, expires, job, err := h.exports.CreateDownloadURL(c.Request.Context(), r.TenantID, r.ID, time.Duration(r.TTLSeconds)*time.Second)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"url": value.String(), "expires_at": expires, "filename": job.Filename, "content_type": job.ContentType, "checksum": job.Checksum})
}
