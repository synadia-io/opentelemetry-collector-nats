// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsexporter

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/testdata"

	"github.com/synadia-io/opentelemetry-collector-nats/exporter/natsexporter/internal/metadata"
)

// runServer starts an embedded nats-server for a test, optionally with JetStream.
func runServer(t *testing.T, enableJetStream bool) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1 // random free port
	if enableJetStream {
		opts.JetStream = true
		opts.StoreDir = t.TempDir()
	}
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// TestExporter_JetStream verifies that, with JetStream configured, exported
// payloads are published to a stream and acknowledged (durable delivery).
func TestExporter_JetStream(t *testing.T) {
	t.Parallel()

	url := runServer(t, true)
	ctx := context.Background()

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	// A stream must exist that captures the exporter's default logs subject.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "OTEL_LOGS",
		Subjects: []string{"otel_logs"},
	})
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.JetStream = &JetStreamConfig{PublishTimeout: 5 * time.Second}

	set := exportertest.NewNopSettings(metadata.Type)
	exp, err := newNatsLogsExporter(set, cfg)
	require.NoError(t, err)

	require.NoError(t, exp.start(ctx, componenttest.NewNopHost()))
	require.NoError(t, exp.export(ctx, testdata.GenerateLogs(1)))
	require.NoError(t, exp.export(ctx, testdata.GenerateLogs(1)))
	require.NoError(t, exp.shutdown(ctx))

	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), info.State.Msgs, "both exports should be persisted and acked by the stream")
}

// TestExporter_JetStream_NoStream verifies that publishing to a subject with no
// bound stream surfaces an error (i.e. we are genuinely waiting for the ack).
func TestExporter_JetStream_NoStream(t *testing.T) {
	t.Parallel()

	url := runServer(t, true)
	ctx := context.Background()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.JetStream = &JetStreamConfig{PublishTimeout: 2 * time.Second}

	set := exportertest.NewNopSettings(metadata.Type)
	exp, err := newNatsLogsExporter(set, cfg)
	require.NoError(t, err)

	require.NoError(t, exp.start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = exp.shutdown(ctx) })

	err = exp.export(ctx, testdata.GenerateLogs(1))
	assert.Error(t, err, "publish to an uncaptured subject should not be acked")
}

// TestExporter_CoreNATS verifies the core-NATS path (JetStream unset) still
// publishes to subscribers.
func TestExporter_CoreNATS(t *testing.T) {
	t.Parallel()

	url := runServer(t, false)
	ctx := context.Background()

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync("otel_logs")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url // JetStream nil -> core path

	set := exportertest.NewNopSettings(metadata.Type)
	exp, err := newNatsLogsExporter(set, cfg)
	require.NoError(t, err)

	require.NoError(t, exp.start(ctx, componenttest.NewNopHost()))
	require.NoError(t, exp.export(ctx, testdata.GenerateLogs(1)))

	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "otel_logs", msg.Subject)
	assert.NotEmpty(t, msg.Data)

	require.NoError(t, exp.shutdown(ctx))
}
