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
	catalog *platformexport.Catalog
}

func NewHandler(healthService *health.Service, exportService *platformexport.Service, catalog *platformexport.Catalog, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, exports: exportService, catalog: catalog, logger: logger}
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
	ApplicationID   string          `json:"application_id"`
	DatasetCode     string          `json:"dataset_code"`
	ProviderService string          `json:"provider_service"`
	Format          string          `json:"format"`
	Filename        string          `json:"filename"`
	Query           json.RawMessage `json:"query" swaggertype:"object"`
	SelectedColumns []string        `json:"selected_columns"`
	IdempotencyKey  string          `json:"idempotency_key"`
}
type ListExportDatasetsRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	Search        string `json:"search"`
	Page          int32  `json:"page"`
	PageSize      int32  `json:"page_size"`
}
type DescribeExportDatasetRequest struct {
	TenantID        string `json:"tenant_id"`
	ApplicationID   string `json:"application_id"`
	ProviderService string `json:"provider_service"`
	DatasetCode     string `json:"dataset_code"`
}
type ExportDatasetPageBody struct {
	Items    []ExportDatasetSummaryBody `json:"items"`
	Total    int64                      `json:"total"`
	Page     int32                      `json:"page"`
	PageSize int32                      `json:"page_size"`
}
type ExportDatasetSummaryBody struct {
	ProviderService  string   `json:"provider_service"`
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Formats          []string `json:"formats"`
	SupportsSnapshot bool     `json:"supports_snapshot"`
	HealthyInstances int32    `json:"healthy_instances"`
}
type ExportColumnBody struct {
	Key       string `json:"key"`
	Title     string `json:"title"`
	Type      string `json:"type"`
	Format    string `json:"format"`
	Sensitive bool   `json:"sensitive"`
}
type ExportQueryFieldBody struct {
	Key         string   `json:"key"`
	Title       string   `json:"title"`
	Type        string   `json:"type"`
	Format      string   `json:"format"`
	Description string   `json:"description"`
	Options     []string `json:"options"`
	Required    bool     `json:"required"`
}
type ExportDatasetDescriptorBody struct {
	Code             string                 `json:"code"`
	Title            string                 `json:"title"`
	Columns          []ExportColumnBody     `json:"columns"`
	QueryFields      []ExportQueryFieldBody `json:"query_fields"`
	Formats          []string               `json:"formats"`
	EstimatedRows    int64                  `json:"estimated_rows"`
	SupportsSnapshot bool                   `json:"supports_snapshot"`
}

// ListExportDatasets godoc
// @Summary List export datasets available to an application
// @Tags export datasets
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ListExportDatasetsRequest true "Application scope and search"
// @Success 200 {object} Response{body=ExportDatasetPageBody}
// @Router /api/v1/exports/datasets/list [post]
func (h *Handler) ListExportDatasets(c *gin.Context) {
	var r ListExportDatasetsRequest
	if !h.bind(c, &r) {
		return
	}
	items, total, page, pageSize, err := h.catalog.List(c.Request.Context(), r.TenantID, r.ApplicationID, r.Search, r.Page, r.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	body := make([]ExportDatasetSummaryBody, len(items))
	for i, item := range items {
		body[i] = ExportDatasetSummaryBody{ProviderService: item.ProviderService, Code: item.Code, Title: item.Title, Formats: item.Formats, SupportsSnapshot: item.SupportsSnapshot, HealthyInstances: item.HealthyInstances}
	}
	OK(c, ExportDatasetPageBody{Items: body, Total: total, Page: page, PageSize: pageSize})
}

// DescribeExportDataset godoc
// @Summary Describe an export dataset available to an application
// @Tags export datasets
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body DescribeExportDatasetRequest true "Dataset selector"
// @Success 200 {object} Response{body=ExportDatasetDescriptorBody}
// @Router /api/v1/exports/datasets/describe [post]
func (h *Handler) DescribeExportDataset(c *gin.Context) {
	var r DescribeExportDatasetRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.catalog.Describe(c.Request.Context(), r.TenantID, r.ApplicationID, r.ProviderService, r.DatasetCode)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	columns := make([]ExportColumnBody, len(value.Columns))
	for i, column := range value.Columns {
		columns[i] = ExportColumnBody{Key: column.Key, Title: column.Title, Type: column.Type, Format: column.Format, Sensitive: column.Sensitive}
	}
	queryFields := make([]ExportQueryFieldBody, len(value.QueryFields))
	for i, field := range value.QueryFields {
		queryFields[i] = ExportQueryFieldBody{Key: field.Key, Title: field.Title, Type: field.Type, Format: field.Format, Description: field.Description, Options: field.Options, Required: field.Required}
	}
	OK(c, ExportDatasetDescriptorBody{Code: value.Code, Title: value.Title, Columns: columns, QueryFields: queryFields, Formats: value.Formats, EstimatedRows: value.EstimatedRows, SupportsSnapshot: value.SupportsSnapshot})
}

type GetExportRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
}
type ListExportsRequest struct {
	TenantID      string     `json:"tenant_id"`
	ApplicationID string     `json:"application_id"`
	Status        string     `json:"status"`
	DatasetCode   string     `json:"dataset_code"`
	CreatedFrom   *time.Time `json:"created_from"`
	CreatedTo     *time.Time `json:"created_to"`
	Page          int32      `json:"page"`
	PageSize      int32      `json:"page_size"`
}
type VersionedExportRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
	Version       int64  `json:"version"`
}
type RetryExportRequest struct {
	TenantID       string `json:"tenant_id"`
	ApplicationID  string `json:"application_id"`
	ID             string `json:"id"`
	Version        int64  `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
}
type DownloadExportRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
	TTLSeconds    int32  `json:"ttl_seconds"`
}

type ExportJobBody struct {
	ID              string          `json:"id"`
	TenantID        string          `json:"tenant_id"`
	ApplicationID   string          `json:"application_id"`
	DatasetCode     string          `json:"dataset_code"`
	ProviderService string          `json:"provider_service"`
	Format          string          `json:"format"`
	Filename        string          `json:"filename"`
	Query           json.RawMessage `json:"query" swaggertype:"object"`
	SelectedColumns []string        `json:"selected_columns"`
	Status          string          `json:"status"`
	RowsExported    int64           `json:"rows_exported"`
	BytesWritten    int64           `json:"bytes_written"`
	ProgressPercent int32           `json:"progress_percent"`
	ObjectKey       string          `json:"object_key"`
	ContentType     string          `json:"content_type"`
	Checksum        string          `json:"checksum"`
	ErrorCode       string          `json:"error_code"`
	ErrorMessage    string          `json:"error_message"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	Version         int64           `json:"version"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	CreatedBy       string          `json:"created_by"`
	UpdatedBy       string          `json:"updated_by"`
}

type ExportMutationBody struct {
	Job       ExportJobBody `json:"job"`
	Duplicate bool          `json:"duplicate"`
}

type ExportPageBody struct {
	Items    []ExportJobBody `json:"items"`
	Total    int64           `json:"total"`
	Page     int32           `json:"page"`
	PageSize int32           `json:"page_size"`
}

type ExportDownloadBody struct {
	URL         string    `json:"url"`
	ExpiresAt   time.Time `json:"expires_at"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Checksum    string    `json:"checksum"`
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
// @Success 200 {object} Response{body=ExportMutationBody}
// @Router /api/v1/exports/create [post]
func (h *Handler) CreateExport(c *gin.Context) {
	var r CreateExportRequest
	if !h.bind(c, &r) {
		return
	}
	columns, _ := json.Marshal(r.SelectedColumns)
	value, duplicate, err := h.exports.Create(c.Request.Context(), platformexport.CreateInput{TenantID: r.TenantID, ApplicationID: r.ApplicationID, DatasetCode: r.DatasetCode, ProviderService: r.ProviderService, Format: r.Format, Filename: r.Filename, QueryJSON: string(rawObject(r.Query)), SelectedColumnsJSON: string(columns), IdempotencyKey: r.IdempotencyKey})
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"job": exportJobBody(value), "duplicate": duplicate})
}

// GetExport godoc
// @Summary Get an export job
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body GetExportRequest true "Job selector"
// @Success 200 {object} Response{body=ExportJobBody}
// @Router /api/v1/exports/get [post]
func (h *Handler) GetExport(c *gin.Context) {
	var r GetExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.Get(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, exportJobBody(value))
}

// ListExports godoc
// @Summary Search export jobs for frontend pages
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ListExportsRequest true "Filters and pagination"
// @Success 200 {object} Response{body=ExportPageBody}
// @Router /api/v1/exports/list [post]
func (h *Handler) ListExports(c *gin.Context) {
	var r ListExportsRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.List(c.Request.Context(), platformexport.ListFilter{TenantID: r.TenantID, ApplicationID: r.ApplicationID, Status: r.Status, DatasetCode: r.DatasetCode, CreatedFrom: r.CreatedFrom, CreatedTo: r.CreatedTo, Page: r.Page, PageSize: r.PageSize})
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
	items := make([]ExportJobBody, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, exportJobBody(item))
	}
	OK(c, gin.H{"items": items, "total": value.Total, "page": page, "page_size": pageSize})
}

// CancelExport godoc
// @Summary Cancel a queued or running export with optimistic locking
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body VersionedExportRequest true "Job and current version"
// @Success 200 {object} Response{body=ExportJobBody}
// @Router /api/v1/exports/cancel [post]
func (h *Handler) CancelExport(c *gin.Context) {
	var r VersionedExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.exports.Cancel(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, exportJobBody(value))
}

// RetryExport godoc
// @Summary Retry a failed or canceled export
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body RetryExportRequest true "Retry request"
// @Success 200 {object} Response{body=ExportMutationBody}
// @Router /api/v1/exports/retry [post]
func (h *Handler) RetryExport(c *gin.Context) {
	var r RetryExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, duplicate, err := h.exports.Retry(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version, r.IdempotencyKey)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"job": exportJobBody(value), "duplicate": duplicate})
}

func exportJobBody(value platformexport.Job) ExportJobBody {
	columns := []string{}
	_ = json.Unmarshal([]byte(value.SelectedColumnsJSON), &columns)
	return ExportJobBody{
		ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, DatasetCode: value.DatasetCode, ProviderService: value.ProviderService,
		Format: value.Format, Filename: value.Filename, Query: rawObject(json.RawMessage(value.QueryJSON)),
		SelectedColumns: columns, Status: value.Status, RowsExported: value.RowsExported, BytesWritten: value.BytesWritten,
		ProgressPercent: value.ProgressPercent, ObjectKey: value.ObjectKey, ContentType: value.ContentType, Checksum: value.Checksum,
		ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: value.StartedAt, CompletedAt: value.CompletedAt,
		ExpiresAt: value.ExpiresAt, Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func rawObject(value json.RawMessage) json.RawMessage {
	if len(value) > 0 && json.Valid(value) {
		if value[0] == '"' {
			var legacy string
			if json.Unmarshal(value, &legacy) == nil && json.Valid([]byte(legacy)) {
				return json.RawMessage(legacy)
			}
		}
		return value
	}
	return json.RawMessage(`{}`)
}

// DownloadExport godoc
// @Summary Create a short-lived download URL
// @Tags exports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body DownloadExportRequest true "Download request"
// @Success 200 {object} Response{body=ExportDownloadBody}
// @Router /api/v1/exports/download [post]
func (h *Handler) DownloadExport(c *gin.Context) {
	var r DownloadExportRequest
	if !h.bind(c, &r) {
		return
	}
	value, expires, job, err := h.exports.CreateDownloadURL(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, time.Duration(r.TTLSeconds)*time.Second)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"url": value.String(), "expires_at": expires, "filename": job.Filename, "content_type": job.ContentType, "checksum": job.Checksum})
}
