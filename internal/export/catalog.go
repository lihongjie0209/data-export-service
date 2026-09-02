package export

import (
	"context"
	"errors"
	"strings"

	"github.com/lihongjie0209/data-export-service/internal/apperror"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
)

type DatasetSummary struct {
	ProviderService  string   `json:"provider_service"`
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Formats          []string `json:"formats"`
	SupportsSnapshot bool     `json:"supports_snapshot"`
	HealthyInstances int32    `json:"healthy_instances"`
}

type DatasetDescriptor struct {
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Columns          []Column `json:"columns"`
	Formats          []string `json:"formats"`
	EstimatedRows    int64    `json:"estimated_rows"`
	SupportsSnapshot bool     `json:"supports_snapshot"`
}

type CatalogProvider interface {
	ListDatasets(context.Context, string, int32, int32) ([]DatasetSummary, int64, error)
	DescribeDataset(context.Context, string, string, string, string) (DatasetDescriptor, error)
}

type Catalog struct {
	provider     CatalogProvider
	applications appaccess.Verifier
}

func NewRuntimeCatalog(provider CatalogProvider, applications appaccess.Verifier) (*Catalog, error) {
	if applications == nil {
		return nil, errors.New("application verifier is required")
	}
	return &Catalog{provider: provider, applications: applications}, nil
}

func (c *Catalog) List(ctx context.Context, tenantID, applicationID, search string, page, pageSize int32) ([]DatasetSummary, int64, int32, int32, error) {
	if err := verifyCatalogScope(ctx, c.applications, tenantID, applicationID); err != nil {
		return nil, 0, 0, 0, err
	}
	page, pageSize = normalizeCatalogPage(page, pageSize)
	values, total, err := c.provider.ListDatasets(ctx, strings.TrimSpace(search), page, pageSize)
	if err != nil {
		return nil, 0, 0, 0, apperror.Unavailable("export dataset catalog is unavailable", err)
	}
	return values, total, page, pageSize, nil
}

func (c *Catalog) Describe(ctx context.Context, tenantID, applicationID, service, dataset string) (DatasetDescriptor, error) {
	if err := verifyCatalogScope(ctx, c.applications, tenantID, applicationID); err != nil {
		return DatasetDescriptor{}, err
	}
	service, dataset = strings.ToLower(normalize(service)), strings.ToLower(normalize(dataset))
	if !codePattern.MatchString(service) || !codePattern.MatchString(dataset) {
		return DatasetDescriptor{}, apperror.Invalid("provider_service and dataset_code are required", nil)
	}
	value, err := c.provider.DescribeDataset(ctx, normalize(tenantID), normalize(applicationID), service, dataset)
	if err != nil {
		return DatasetDescriptor{}, apperror.Unavailable("export dataset descriptor is unavailable", err)
	}
	return value, nil
}

func verifyCatalogScope(ctx context.Context, verifier appaccess.Verifier, tenantID, applicationID string) error {
	if _, err := authorize(ctx, tenantID); err != nil {
		return err
	}
	tenantID, applicationID = normalize(tenantID), normalize(applicationID)
	if tenantID == "" || applicationID == "" {
		return apperror.Invalid("tenant_id and application_id are required", nil)
	}
	if err := verifier.Verify(ctx, tenantID, applicationID); err != nil {
		if errors.Is(err, appaccess.ErrNotGranted) {
			return apperror.New(apperror.CodeForbidden, "application access denied", 403, err)
		}
		return apperror.Unavailable("application authorization unavailable", err)
	}
	return nil
}
