package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/lihongjie0209/data-export-service/internal/grpcclient"
	"github.com/lihongjie0209/data-export-service/internal/observability"
	"github.com/lihongjie0209/data-export-service/internal/outbound"
	"github.com/lihongjie0209/microservice-platform-go/exportprovider"
	"github.com/lihongjie0209/microservice-platform-go/serviceregistry"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

type providerConnection struct {
	target     string
	connection *grpc.ClientConn
}
type GRPCProvider struct {
	static             *outbound.Registry
	config             config.ProviderClient
	discovery          *serviceregistry.Discovery
	registryConnection *grpc.ClientConn
	mu                 sync.Mutex
	connections        map[string]providerConnection
	cursor             atomic.Uint64
	dial               func(grpcclient.Config) (*grpc.ClientConn, error)
	metrics            *observability.Metrics
}

func NewProvider(lifecycle fx.Lifecycle, cfg config.Config, static *outbound.Registry, metrics *observability.Metrics) (Provider, error) {
	provider := &GRPCProvider{static: static, config: cfg.ProviderClient, connections: map[string]providerConnection{}, dial: grpcclient.Dial, metrics: metrics}
	if cfg.ServiceRegistry.Enabled {
		connection, err := grpcclient.Dial(grpcclient.Config{Name: "service-registry-service", Target: cfg.ServiceRegistry.Target, Timeout: 3 * time.Second, PSK: cfg.ServiceRegistry.PSK, TLS: grpcclient.TLSConfig{Enabled: cfg.ServiceRegistry.TLS.Enabled, ServerName: cfg.ServiceRegistry.TLS.ServerName, CAFile: cfg.ServiceRegistry.TLS.CAFile, CertFile: cfg.ServiceRegistry.TLS.CertFile, KeyFile: cfg.ServiceRegistry.TLS.KeyFile, AllowInsecureToken: cfg.ServiceRegistry.AllowInsecure}})
		if err != nil {
			return nil, fmt.Errorf("dial service registry: %w", err)
		}
		provider.registryConnection = connection
		discovery, err := serviceregistry.NewDiscovery(registryv1.NewRegistryServiceClient(connection), serviceregistry.DiscoveryConfig{Selector: map[string]string{"platform.export.provider": "true"}, MaxStale: cfg.ServiceRegistry.MaxStale, SnapshotStore: serviceregistry.FileSnapshotStore{Directory: cfg.ServiceRegistry.SnapshotDirectory}})
		if err != nil {
			_ = connection.Close()
			return nil, err
		}
		provider.discovery = discovery
		var cancel context.CancelFunc
		lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
			runCtx, stop := context.WithCancel(context.Background())
			cancel = stop
			go func() { _ = discovery.Run(runCtx) }()
			return nil
		}, OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		}})
	}
	lifecycle.Append(fx.StopHook(func() error { return provider.Close() }))
	return provider, nil
}

func (p *GRPCProvider) Stream(ctx context.Context, service string, request StreamRequest, receive func(Batch) error) error {
	client, instance, err := p.client(service, request.DatasetCode)
	if err != nil {
		return err
	}
	stream, err := client.StreamRows(ctx, &exportv1.StreamRowsRequest{TenantId: request.TenantID, ApplicationId: request.ApplicationID, DatasetCode: request.DatasetCode, QueryJson: request.QueryJSON, SelectedColumns: request.SelectedColumns, BatchSize: int32(request.BatchSize), Cursor: request.Cursor, SnapshotToken: request.SnapshotToken})
	if err != nil {
		p.failure(instance)
		return err
	}
	for {
		response, recvErr := stream.Recv()
		if recvErr == io.EOF {
			p.success(instance)
			return nil
		}
		if recvErr != nil {
			p.failure(instance)
			return recvErr
		}
		columns := make([]Column, len(response.GetColumns()))
		for i, value := range response.GetColumns() {
			columns[i] = Column{Key: value.GetKey(), Title: value.GetTitle(), Type: value.GetType(), Format: value.GetFormat(), Sensitive: value.GetSensitive()}
		}
		rows := make([]map[string]any, len(response.GetRows()))
		for i, value := range response.GetRows() {
			rows[i] = value.AsMap()
		}
		if err := receive(Batch{Columns: columns, Rows: rows, NextCursor: response.GetNextCursor(), SnapshotToken: response.GetSnapshotToken(), EstimatedTotalRows: response.GetEstimatedTotalRows(), Done: response.GetDone()}); err != nil {
			return err
		}
		if response.GetDone() {
			p.success(instance)
			return nil
		}
	}
}

func (p *GRPCProvider) client(service, dataset string) (exportv1.ExportProviderServiceClient, *registryv1.ServiceInstance, error) {
	if p.discovery == nil {
		connection, ok := p.static.GRPC(service)
		if !ok {
			return nil, nil, fmt.Errorf("export provider %q is not configured", service)
		}
		return exportv1.NewExportProviderServiceClient(connection), nil, nil
	}
	instances, err := p.discovery.Instances()
	if err != nil {
		return nil, nil, err
	}
	candidates := make([]*registryv1.ServiceInstance, 0)
	for _, instance := range instances {
		if instance.GetServiceName() != service || !supportsDataset(instance.Metadata, dataset) {
			continue
		}
		candidates = append(candidates, instance)
	}
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("export provider %q with dataset %q is not registered", service, dataset)
	}
	instance := candidates[(p.cursor.Add(1)-1)%uint64(len(candidates))]
	target := strings.TrimPrefix(strings.TrimPrefix(instance.GetEndpoint(), "grpc://"), "grpcs://")
	if err := validateProviderTarget(target, p.config.AllowedDNSSuffixes); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.connections[instance.GetInstanceId()]; ok && current.target == target {
		return exportv1.NewExportProviderServiceClient(current.connection), instance, nil
	}
	if current, ok := p.connections[instance.GetInstanceId()]; ok {
		_ = current.connection.Close()
		delete(p.connections, instance.GetInstanceId())
	}
	connection, err := p.dial(grpcclient.Config{Name: service, Target: target, Timeout: p.config.Timeout, PSK: p.config.PSK, Retry: p.config.Retry, Breaker: p.config.Breaker, Metrics: p.metrics, TLS: grpcclient.TLSConfig{Enabled: p.config.TLS.Enabled, ServerName: p.config.TLS.ServerName, CAFile: p.config.TLS.CAFile, CertFile: p.config.TLS.CertFile, KeyFile: p.config.TLS.KeyFile, AllowInsecureToken: p.config.AllowInsecure}})
	if err != nil {
		return nil, nil, err
	}
	p.connections[instance.GetInstanceId()] = providerConnection{target: target, connection: connection}
	return exportv1.NewExportProviderServiceClient(connection), instance, nil
}

func supportsDataset(metadata map[string]string, dataset string) bool {
	values, err := exportprovider.ParseMetadata(metadata)
	if err != nil {
		return false
	}
	for _, value := range values {
		if value.Code == dataset {
			return true
		}
	}
	return false
}
func validateProviderTarget(target string, suffixes []string) error {
	value := strings.TrimPrefix(target, "dns:///")
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" || net.ParseIP(host) != nil || strings.EqualFold(host, "localhost") {
		return errors.New("provider target must be a DNS name with port")
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, suffix := range suffixes {
		suffix = strings.ToLower(strings.TrimSpace(suffix))
		if suffix != "" && (host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix)) {
			return nil
		}
	}
	return fmt.Errorf("provider host %q is outside allowed DNS suffixes", host)
}
func (p *GRPCProvider) failure(instance *registryv1.ServiceInstance) {
	if p.discovery != nil && instance != nil {
		p.discovery.ReportFailure(instance.GetInstanceId(), p.config.FailureCooldown)
	}
}
func (p *GRPCProvider) success(instance *registryv1.ServiceInstance) {
	if p.discovery != nil && instance != nil {
		p.discovery.ReportSuccess(instance.GetInstanceId())
	}
}
func (p *GRPCProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var result error
	for id, value := range p.connections {
		if err := value.connection.Close(); err != nil && result == nil {
			result = fmt.Errorf("close provider %s: %w", id, err)
		}
	}
	p.connections = map[string]providerConnection{}
	if p.registryConnection != nil {
		if err := p.registryConnection.Close(); err != nil && result == nil {
			result = err
		}
	}
	return result
}
