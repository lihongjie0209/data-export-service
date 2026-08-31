package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/lihongjie0209/data-export-service/internal/eventbus"
	platformexport "github.com/lihongjie0209/data-export-service/internal/export"
	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

type jobProcessor interface {
	Process(context.Context, string, string) error
}
type exportEventRuntime struct {
	cfg    config.Config
	store  *platformoutbox.SQLStore
	bus    *eventbus.Bus
	worker jobProcessor
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newExportOutboxStore(db *sqlx.DB) (*platformoutbox.SQLStore, error) {
	if db == nil {
		return nil, nil
	}
	return platformoutbox.NewSQLStore(db, "export_outbox_events")
}
func newExportEventRuntime(lifecycle fx.Lifecycle, cfg config.Config, store *platformoutbox.SQLStore, bus *eventbus.Bus, worker *platformexport.Worker, logger *slog.Logger) *exportEventRuntime {
	runtime := &exportEventRuntime{cfg: cfg, store: store, bus: bus, worker: worker, logger: logger}
	lifecycle.Append(fx.Hook{OnStart: runtime.start, OnStop: runtime.stop})
	return runtime
}
func (r *exportEventRuntime) start(context.Context) error {
	if !r.cfg.EventBus.Enabled {
		return nil
	}
	if r.store == nil || r.bus == nil {
		return errors.New("enabled event bus requires database outbox and JetStream")
	}
	dispatcher, err := platformoutbox.New(r.store, r.bus, platformoutbox.Config{BatchSize: r.cfg.EventBus.DispatchBatchSize, Lease: r.cfg.EventBus.DispatchLease, RetryDelay: r.cfg.EventBus.DispatchRetryDelay})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	cleaner, err := platformoutbox.NewRetentionCleaner(r.store, platformoutbox.RetentionConfig{Retention: r.cfg.EventBus.PublishedRetention, BatchSize: r.cfg.EventBus.CleanupBatchSize})
	if err != nil {
		cancel()
		return err
	}
	r.wg.Go(func() {
		ticker := time.NewTicker(r.cfg.EventBus.DispatchInterval)
		defer ticker.Stop()
		for {
			if _, runErr := dispatcher.RunOnce(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				r.logger.ErrorContext(ctx, "dispatch export outbox failed", "error", runErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	r.wg.Go(func() {
		ticker := time.NewTicker(r.cfg.EventBus.CleanupInterval)
		defer ticker.Stop()
		for {
			if deleted, runErr := cleaner.RunOnce(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				r.logger.ErrorContext(ctx, "clean published export outbox events", "error", runErr)
			} else if deleted > 0 {
				r.logger.InfoContext(ctx, "published export outbox events cleaned", "deleted", deleted)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	r.wg.Go(func() {
		err := r.bus.ConsumeWithOptions(ctx, platformeventbus.ConsumerOptions{Durable: "data-export-worker-v1", FilterSubject: "platform.export.job.requested.v1", Handler: r.handleRequested, OnError: func(err error) { r.logger.ErrorContext(ctx, "consume export request failed", "error", err) }})
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "export consumer stopped", "error", err)
		}
	})
	return nil
}
func (r *exportEventRuntime) handleRequested(ctx context.Context, envelope *eventbus.Envelope) error {
	payload := new(exportv1.ExportJobChangedEvent)
	if err := proto.Unmarshal(envelope.GetPayload(), payload); err != nil {
		return err
	}
	job := payload.GetJob()
	if payload.GetChangeType() != "requested" || job == nil {
		return errors.New("invalid export requested event")
	}
	return r.worker.Process(ctx, job.GetTenantId(), job.GetId())
}
func (r *exportEventRuntime) stop(context.Context) error {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
	}
	return nil
}

var ExportEventModule = fx.Module("export-event-runtime", fx.Provide(newExportOutboxStore, newExportEventRuntime), fx.Invoke(func(*exportEventRuntime) {}))
