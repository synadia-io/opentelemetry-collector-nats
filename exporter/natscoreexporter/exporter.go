// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscoreexporter // import "github.com/suckatrash/opentelemetry-collector-nats/exporter/natscoreexporter"

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"

	"github.com/suckatrash/opentelemetry-collector-nats/exporter/natscoreexporter/internal/grouper"
	"github.com/suckatrash/opentelemetry-collector-nats/exporter/natscoreexporter/internal/marshaler"
	"github.com/suckatrash/opentelemetry-collector-nats/internal/natsclient"
)

type natsCoreExporter[T any] struct {
	set       exporter.Settings
	cfg       *Config
	grouper   grouper.Grouper[T]
	marshaler marshaler.Marshaler[T]
	publisher publisher
}

func newNatsCoreExporter[T any](
	set exporter.Settings,
	cfg *Config,
	grouper grouper.Grouper[T],
	marshaler marshaler.Marshaler[T],
) *natsCoreExporter[T] {
	return &natsCoreExporter[T]{
		set:       set,
		cfg:       cfg,
		grouper:   grouper,
		marshaler: marshaler,
	}
}

// publisher abstracts how marshaled payloads are written to NATS, so the exporter
// can target either core NATS or JetStream without the export path caring which.
type publisher interface {
	publish(ctx context.Context, subject string, data []byte) error
	close()
}

// corePublisher publishes with core NATS (fire-and-forget, no delivery guarantee).
type corePublisher struct {
	conn *nats.Conn
}

func (p *corePublisher) publish(_ context.Context, subject string, data []byte) error {
	return p.conn.Publish(subject, data)
}

func (p *corePublisher) close() {
	p.conn.Close()
}

// jetStreamPublisher publishes with JetStream, blocking until the server
// acknowledges each message. This gives durable, at-least-once delivery: a
// publish error is returned to the exporter helper, which retries the batch.
//
// A stream whose subjects capture the configured signal subjects must already
// exist on the server; this exporter does not create or manage streams.
type jetStreamPublisher struct {
	conn    *nats.Conn
	js      jetstream.JetStream
	timeout time.Duration
}

func (p *jetStreamPublisher) publish(ctx context.Context, subject string, data []byte) error {
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	_, err := p.js.Publish(ctx, subject, data)
	return err
}

func (p *jetStreamPublisher) close() {
	p.conn.Close()
}

// newPublisher connects to NATS and returns a core or JetStream publisher
// according to cfg.
func newPublisher(ctx context.Context, cfg *Config) (publisher, error) {
	conn, err := natsclient.Connect(ctx, natsclient.Params{
		Endpoint: cfg.Endpoint,
		Pedantic: cfg.Pedantic,
		TLS:      cfg.TLS,
		Auth:     cfg.Auth,
	})
	if err != nil {
		return nil, err
	}

	if cfg.JetStream == nil {
		return &corePublisher{conn: conn}, nil
	}

	var js jetstream.JetStream
	if cfg.JetStream.Domain != "" {
		js, err = jetstream.NewWithDomain(conn, cfg.JetStream.Domain)
	} else {
		js, err = jetstream.New(conn)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &jetStreamPublisher{
		conn:    conn,
		js:      js,
		timeout: cfg.JetStream.PublishTimeout,
	}, nil
}

func (e *natsCoreExporter[T]) start(ctx context.Context, host component.Host) error {
	var errs error

	errs = multierr.Append(errs, e.marshaler.Resolve(host))

	pub, err := newPublisher(ctx, e.cfg)
	errs = multierr.Append(errs, err)
	e.publisher = pub

	return errs
}

func (e *natsCoreExporter[T]) export(ctx context.Context, data T) error {
	var errs error

	groups, err := e.grouper.Group(ctx, data)
	errs = multierr.Append(errs, err)

	for _, group := range groups {
		bytes, err := e.marshaler.Marshal(group.Data)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}

		err = e.publisher.publish(ctx, group.Subject, bytes)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

func (e *natsCoreExporter[T]) shutdown(_ context.Context) error {
	if e.publisher != nil {
		e.publisher.close()
	}
	return nil
}

func createResolver(cfg *SignalConfig) (marshaler.Resolver, error) {
	if cfg.EncodingExtensionName != "" {
		return marshaler.NewEncodingExtensionResolver(cfg.EncodingExtensionName)
	}
	// Built-in marshaler; an empty name defaults to otlp_proto.
	name := cfg.BuiltinMarshalerName
	if name == "" {
		name = marshaler.OtlpProtoBuiltinMarshalerName
	}
	return marshaler.NewBuiltinMarshalerResolver(name)
}

func newNatsCoreLogsExporter(set exporter.Settings, cfg *Config) (*natsCoreExporter[plog.Logs], error) {
	var errs error

	grouper, err := grouper.NewLogsGrouper(cfg.Logs.Subject, set.TelemetrySettings)
	errs = multierr.Append(errs, err)

	resolver, err := createResolver((*SignalConfig)(&cfg.Logs))
	errs = multierr.Append(errs, err)
	marshaler := marshaler.NewMarshaler(resolver, marshaler.PickMarshalLogs)

	return newNatsCoreExporter(set, cfg, grouper, marshaler), errs
}

func newNatsCoreMetricsExporter(set exporter.Settings, cfg *Config) (*natsCoreExporter[pmetric.Metrics], error) {
	var errs error

	grouper, err := grouper.NewMetricsGrouper(cfg.Metrics.Subject, set.TelemetrySettings)
	errs = multierr.Append(errs, err)

	resolver, err := createResolver((*SignalConfig)(&cfg.Metrics))
	errs = multierr.Append(errs, err)
	marshaler := marshaler.NewMarshaler(resolver, marshaler.PickMarshalMetrics)

	return newNatsCoreExporter(set, cfg, grouper, marshaler), errs
}

func newNatsCoreTracesExporter(set exporter.Settings, cfg *Config) (*natsCoreExporter[ptrace.Traces], error) {
	var errs error

	grouper, err := grouper.NewTracesGrouper(cfg.Traces.Subject, set.TelemetrySettings)
	errs = multierr.Append(errs, err)

	resolver, err := createResolver((*SignalConfig)(&cfg.Traces))
	errs = multierr.Append(errs, err)
	marshaler := marshaler.NewMarshaler(resolver, marshaler.PickMarshalTraces)

	return newNatsCoreExporter(set, cfg, grouper, marshaler), errs
}
