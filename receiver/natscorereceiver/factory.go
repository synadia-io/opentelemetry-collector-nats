// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"

	"github.com/synadia-labs/opentelemetry-collector-nats/internal/natsclient"
	"github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver/internal/metadata"
)

const (
	defaultLogsSubject    = "otel_logs"
	defaultMetricsSubject = "otel_metrics"
	defaultTracesSubject  = "otel_spans"
	defaultEncoding       = encodingOtlpProto
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint: nats.DefaultURL,
		Pedantic: true,
		TLS:      configtls.NewDefaultClientConfig(),
		Logs:     SignalConfig{Subject: defaultLogsSubject, Encoding: defaultEncoding},
		Metrics:  SignalConfig{Subject: defaultMetricsSubject, Encoding: defaultEncoding},
		Traces:   SignalConfig{Subject: defaultTracesSubject, Encoding: defaultEncoding},
		Auth:     natsclient.AuthConfig{},
	}
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Logs,
) (receiver.Logs, error) {
	c := cfg.(*Config)
	unmarshal, err := logsUnmarshaler(c.Logs.Encoding)
	if err != nil {
		return nil, err
	}
	return newNatsReceiver[plog.Logs](set, c, &c.Logs, unmarshal, next.ConsumeLogs), nil
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (receiver.Metrics, error) {
	c := cfg.(*Config)
	unmarshal, err := metricsUnmarshaler(c.Metrics.Encoding)
	if err != nil {
		return nil, err
	}
	return newNatsReceiver[pmetric.Metrics](set, c, &c.Metrics, unmarshal, next.ConsumeMetrics), nil
}

func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Traces,
) (receiver.Traces, error) {
	c := cfg.(*Config)
	unmarshal, err := tracesUnmarshaler(c.Traces.Encoding)
	if err != nil {
		return nil, err
	}
	return newNatsReceiver[ptrace.Traces](set, c, &c.Traces, unmarshal, next.ConsumeTraces), nil
}
