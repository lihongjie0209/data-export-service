package export

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/data-export-service/internal/apperror"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

type catalogProviderStub struct {
	descriptor DatasetDescriptor
	err        error
	tenant     string
	app        string
}

func (p *catalogProviderStub) ListDatasets(context.Context, string, int32, int32) ([]DatasetSummary, int64, error) {
	return []DatasetSummary{{Code: "billing.invoices"}}, 1, p.err
}
func (p *catalogProviderStub) DescribeDataset(_ context.Context, tenant, app, _, _ string) (DatasetDescriptor, error) {
	p.tenant, p.app = tenant, app
	return p.descriptor, p.err
}

func catalogContext() context.Context {
	return platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
}

func TestCatalogRequiresApplicationGrant(t *testing.T) {
	t.Parallel()
	catalog, err := NewRuntimeCatalog(&catalogProviderStub{}, applicationVerifier{err: appaccess.ErrNotGranted})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, _, err = catalog.List(catalogContext(), "tenant-1", "app-1", "", 0, 0)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeForbidden {
		t.Fatalf("error = %#v", err)
	}
}

func TestCatalogNormalizesPageAndForwardsPinnedScope(t *testing.T) {
	t.Parallel()
	provider := &catalogProviderStub{descriptor: DatasetDescriptor{Code: "billing.invoices"}}
	catalog, err := NewRuntimeCatalog(provider, applicationVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	items, total, page, size, err := catalog.List(catalogContext(), "tenant-1", "app-1", "", 0, 1000)
	if err != nil || len(items) != 1 || total != 1 || page != 1 || size != 100 {
		t.Fatalf("result = %v %d %d %d, err = %v", items, total, page, size, err)
	}
	value, err := catalog.Describe(catalogContext(), " tenant-1 ", " app-1 ", "billing-service", "billing.invoices")
	if err != nil || value.Code != "billing.invoices" || provider.tenant != "tenant-1" || provider.app != "app-1" {
		t.Fatalf("value = %+v scope = %q/%q err = %v", value, provider.tenant, provider.app, err)
	}
}
