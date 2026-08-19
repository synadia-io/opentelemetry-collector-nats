// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsreceiver // import "github.com/synadia-io/opentelemetry-collector-nats/receiver/natsreceiver"

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

	"github.com/synadia-io/opentelemetry-collector-nats/internal/natsclient"
	"github.com/synadia-io/opentelemetry-collector-nats/receiver/natsreceiver/internal/metadata"
)

const (
	defaultLogsSubject    = "otel_logs"
	defaultMetricsSubject = "otel_metrics"
	defaultTracesSubject  = "otel_spans"
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
		Logs:     SignalConfig{Subject: defaultLogsSubject},
		Metrics:  SignalConfig{Subject: defaultMetricsSubject},
		Traces:   SignalConfig{Subject: defaultTracesSubject},
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
	return newNatsReceiver[plog.Logs](set, c, &c.Logs, logsUnmarshalerResolver(&c.Logs), next.ConsumeLogs), nil
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (receiver.Metrics, error) {
	c := cfg.(*Config)
	return newNatsReceiver[pmetric.Metrics](set, c, &c.Metrics, metricsUnmarshalerResolver(&c.Metrics), next.ConsumeMetrics), nil
}

func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Traces,
) (receiver.Traces, error) {
	c := cfg.(*Config)
	return newNatsReceiver[ptrace.Traces](set, c, &c.Traces, tracesUnmarshalerResolver(&c.Traces), next.ConsumeTraces), nil
}
