package grpctransport

import (
	"context"
	"errors"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/apperror"
	platformexport "github.com/lihongjie0209/data-export-service/internal/export"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type exportServer struct {
	exportv1.UnimplementedExportServiceServer
	service *platformexport.Service
}

func (s *exportServer) CreateExportJob(ctx context.Context, r *exportv1.CreateExportJobRequest) (*exportv1.CreateExportJobResponse, error) {
	value, duplicate, err := s.service.Create(ctx, platformexport.CreateInput{TenantID: r.GetTenantId(), ApplicationID: r.GetApplicationId(), DatasetCode: r.GetDatasetCode(), ProviderService: r.GetProviderService(), Format: r.GetFormat(), Filename: r.GetFilename(), QueryJSON: r.GetQueryJson(), SelectedColumnsJSON: r.GetSelectedColumnsJson(), IdempotencyKey: r.GetIdempotencyKey()})
	return &exportv1.CreateExportJobResponse{Job: platformexport.ToProto(value), Duplicate: duplicate}, exportError(err)
}
func (s *exportServer) GetExportJob(ctx context.Context, r *exportv1.GetExportJobRequest) (*exportv1.GetExportJobResponse, error) {
	value, err := s.service.Get(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetId())
	return &exportv1.GetExportJobResponse{Job: platformexport.ToProto(value)}, exportError(err)
}
func (s *exportServer) ListExportJobs(ctx context.Context, r *exportv1.ListExportJobsRequest) (*exportv1.ListExportJobsResponse, error) {
	page, size := int32(0), int32(0)
	if r.GetPage() != nil {
		page = int32(r.GetPage().GetPage())
		size = int32(r.GetPage().GetPageSize())
	}
	filter := platformexport.ListFilter{TenantID: r.GetTenantId(), ApplicationID: r.GetApplicationId(), Status: r.GetStatus(), DatasetCode: r.GetDatasetCode(), Page: page, PageSize: size}
	if r.GetCreatedFrom() != nil {
		value := r.GetCreatedFrom().AsTime()
		filter.CreatedFrom = &value
	}
	if r.GetCreatedTo() != nil {
		value := r.GetCreatedTo().AsTime()
		filter.CreatedTo = &value
	}
	result, err := s.service.List(ctx, filter)
	items := make([]*exportv1.ExportJob, len(result.Items))
	for i := range result.Items {
		items[i] = platformexport.ToProto(result.Items[i])
	}
	return &exportv1.ListExportJobsResponse{Jobs: items, Page: &commonv1.PageResult{Total: uint64(result.Total), Page: uint32(max(page, 1)), PageSize: uint32(normalizedSize(size))}}, exportError(err)
}
func (s *exportServer) CancelExportJob(ctx context.Context, r *exportv1.CancelExportJobRequest) (*exportv1.CancelExportJobResponse, error) {
	value, err := s.service.Cancel(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetId(), r.GetVersion())
	return &exportv1.CancelExportJobResponse{Job: platformexport.ToProto(value)}, exportError(err)
}
func (s *exportServer) RetryExportJob(ctx context.Context, r *exportv1.RetryExportJobRequest) (*exportv1.RetryExportJobResponse, error) {
	value, duplicate, err := s.service.Retry(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetId(), r.GetVersion(), r.GetIdempotencyKey())
	return &exportv1.RetryExportJobResponse{Job: platformexport.ToProto(value), Duplicate: duplicate}, exportError(err)
}
func (s *exportServer) CreateDownloadURL(ctx context.Context, r *exportv1.CreateDownloadURLRequest) (*exportv1.CreateDownloadURLResponse, error) {
	value, expires, job, err := s.service.CreateDownloadURL(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetId(), time.Duration(r.GetTtlSeconds())*time.Second)
	response := &exportv1.CreateDownloadURLResponse{}
	if err == nil {
		response.Url = value.String()
		response.ExpiresAt = timestamppb.New(expires)
		response.Filename = job.Filename
		response.ContentType = job.ContentType
		response.Checksum = job.Checksum
	}
	return response, exportError(err)
}
func normalizedSize(value int32) int32 {
	if value < 1 {
		return 20
	}
	if value > 200 {
		return 200
	}
	return value
}
func exportError(err error) error {
	if err == nil {
		return nil
	}
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return status.Error(codes.Internal, "internal server error")
	}
	mapping := map[int]codes.Code{apperror.CodeInvalidArgument: codes.InvalidArgument, apperror.CodeNotFound: codes.NotFound, apperror.CodeUnauthorized: codes.Unauthenticated, apperror.CodeForbidden: codes.PermissionDenied, apperror.CodeConflict: codes.Aborted, apperror.CodeDependencyUnavailable: codes.Unavailable, apperror.CodeRequestTimeout: codes.DeadlineExceeded, apperror.CodeTooManyRequests: codes.ResourceExhausted}
	code, ok := mapping[appErr.Code]
	if !ok {
		code = codes.Internal
	}
	return status.Error(code, appErr.Message)
}
