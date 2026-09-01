package export

import (
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ToProto(value Job) *exportv1.ExportJob {
	result := &exportv1.ExportJob{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, DatasetCode: value.DatasetCode, ProviderService: value.ProviderService, Format: value.Format, Filename: value.Filename, QueryJson: value.QueryJSON, SelectedColumnsJson: value.SelectedColumnsJSON, Status: value.Status, RowsExported: value.RowsExported, BytesWritten: value.BytesWritten, ProgressPercent: value.ProgressPercent, ObjectKey: value.ObjectKey, ContentType: value.ContentType, Checksum: value.Checksum, ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
	if value.StartedAt != nil {
		result.StartedAt = timestamppb.New(*value.StartedAt)
	}
	if value.CompletedAt != nil {
		result.CompletedAt = timestamppb.New(*value.CompletedAt)
	}
	if value.ExpiresAt != nil {
		result.ExpiresAt = timestamppb.New(*value.ExpiresAt)
	}
	return result
}
