// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/synadia-io/opentelemetry-collector-nats/exporter/natscoreexporter"
	"github.com/synadia-io/opentelemetry-collector-nats/receiver/natscorereceiver"
)

// runJetStreamServer starts an embedded JetStream-enabled nats-server and returns
// its client URL.
func runJetStreamServer(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// createStream creates a stream capturing all three default signal subjects.
func createStream(t *testing.T, url string) {
	t.Helper()
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     "OTEL",
		Subjects: []string{"otel_logs", "otel_metrics", "otel_spans"},
	})
	require.NoError(t, err)
}

// newExporterConfig returns a JetStream exporter config pointed at url (default
// subjects/marshaler).
func newExporterConfig(url string) *natscoreexporter.Config {
	cfg := natscoreexporter.NewFactory().CreateDefaultConfig().(*natscoreexporter.Config)
	cfg.Endpoint = url
	cfg.JetStream = &natscoreexporter.JetStreamConfig{PublishTimeout: 5 * time.Second}
	return cfg
}

// newReceiverConfig returns a JetStream receiver config pointed at url, binding a
// durable consumer per signal to the shared OTEL stream.
func newReceiverConfig(url string) *natscorereceiver.Config {
	cfg := natscorereceiver.NewFactory().CreateDefaultConfig().(*natscorereceiver.Config)
	cfg.Endpoint = url
	cfg.JetStream = &natscorereceiver.JetStreamConfig{AckWait: 5 * time.Second}
	cfg.Logs.Subject, cfg.Logs.Stream, cfg.Logs.Durable = "otel_logs", "OTEL", "e2e_logs"
	cfg.Metrics.Subject, cfg.Metrics.Stream, cfg.Metrics.Durable = "otel_metrics", "OTEL", "e2e_metrics"
	cfg.Traces.Subject, cfg.Traces.Stream, cfg.Traces.Durable = "otel_spans", "OTEL", "e2e_traces"
	return cfg
}

func TestRoundTrip_Traces(t *testing.T) {
	t.Parallel()

	url := runJetStreamServer(t)
	createStream(t, url)
	ctx := context.Background()
	host := componenttest.NewNopHost()

	rf := natscorereceiver.NewFactory()
	sink := new(consumertest.TracesSink)
	rcv, err := rf.CreateTraces(ctx, receivertest.NewNopSettings(rf.Type()), newReceiverConfig(url), sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	ef := natscoreexporter.NewFactory()
	exp, err := ef.CreateTraces(ctx, exportertest.NewNopSettings(ef.Type()), newExporterConfig(url))
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	sent := testdata.GenerateTraces(1)
	require.NoError(t, exp.ConsumeTraces(ctx, sent))

	require.Eventually(t, func() bool { return sink.SpanCount() >= 1 }, 5*time.Second, 20*time.Millisecond)
	require.Len(t, sink.AllTraces(), 1)
	require.NoError(t, ptracetest.CompareTraces(sent, sink.AllTraces()[0]))
}

func TestRoundTrip_Logs(t *testing.T) {
	t.Parallel()

	url := runJetStreamServer(t)
	createStream(t, url)
	ctx := context.Background()
	host := componenttest.NewNopHost()

	rf := natscorereceiver.NewFactory()
	sink := new(consumertest.LogsSink)
	rcv, err := rf.CreateLogs(ctx, receivertest.NewNopSettings(rf.Type()), newReceiverConfig(url), sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	ef := natscoreexporter.NewFactory()
	exp, err := ef.CreateLogs(ctx, exportertest.NewNopSettings(ef.Type()), newExporterConfig(url))
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	sent := testdata.GenerateLogs(1)
	require.NoError(t, exp.ConsumeLogs(ctx, sent))

	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 5*time.Second, 20*time.Millisecond)
	require.Len(t, sink.AllLogs(), 1)
	require.NoError(t, plogtest.CompareLogs(sent, sink.AllLogs()[0]))
}

func TestRoundTrip_Metrics(t *testing.T) {
	t.Parallel()

	url := runJetStreamServer(t)
	createStream(t, url)
	ctx := context.Background()
	host := componenttest.NewNopHost()

	rf := natscorereceiver.NewFactory()
	sink := new(consumertest.MetricsSink)
	rcv, err := rf.CreateMetrics(ctx, receivertest.NewNopSettings(rf.Type()), newReceiverConfig(url), sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	ef := natscoreexporter.NewFactory()
	exp, err := ef.CreateMetrics(ctx, exportertest.NewNopSettings(ef.Type()), newExporterConfig(url))
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	sent := testdata.GenerateMetrics(1)
	require.NoError(t, exp.ConsumeMetrics(ctx, sent))

	require.Eventually(t, func() bool { return sink.DataPointCount() >= 1 }, 5*time.Second, 20*time.Millisecond)
	require.Len(t, sink.AllMetrics(), 1)
	require.NoError(t, pmetrictest.CompareMetrics(sent, sink.AllMetrics()[0]))
}
