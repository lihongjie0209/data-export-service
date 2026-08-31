package export

import "testing"

func TestSupportsDatasetMetadataForms(t *testing.T) {
	for _, metadata := range []map[string]string{{"platform.export.datasets": `["billing.invoices","billing.credits"]`}, {"platform.export.datasets": `[{"code":"billing.invoices","title":"Invoices"}]`}} {
		if !supportsDataset(metadata, "billing.invoices") {
			t.Fatalf("not supported: %v", metadata)
		}
	}
	if supportsDataset(map[string]string{"platform.export.datasets": `["users"]`}, "billing.invoices") {
		t.Fatal("unregistered dataset accepted")
	}
}
func TestValidateProviderTargetUsesDNSAllowlist(t *testing.T) {
	if err := validateProviderTarget("billing-service.platform.svc.cluster.local:9090", []string{".platform.svc.cluster.local"}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"127.0.0.1:9090", "localhost:9090", "evil.example.com:9090", "missing-port"} {
		if err := validateProviderTarget(target, []string{".platform.svc.cluster.local"}); err == nil {
			t.Fatalf("target %q accepted", target)
		}
	}
}
