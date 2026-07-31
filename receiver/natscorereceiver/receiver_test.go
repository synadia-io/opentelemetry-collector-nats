// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/suckatrash/opentelemetry-collector-nats/receiver/natscorereceiver/internal/metadata"
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

func otlpProtoLogs(t *testing.T) []byte {
	t.Helper()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(testdata.GenerateLogs(1))
	require.NoError(t, err)
	return data
}

// TestReceiver_JetStream verifies that a durable consumer reads OTLP payloads
// from a stream, forwards them downstream, and acknowledges them.
func TestReceiver_JetStream(t *testing.T) {
	t.Parallel()

	url := runServer(t, true)
	ctx := context.Background()

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "OTEL_LOGS",
		Subjects: []string{"otel_logs"},
	})
	require.NoError(t, err)

	payload := otlpProtoLogs(t)
	for i := 0; i < 2; i++ {
		_, err = js.Publish(ctx, "otel_logs", payload)
		require.NoError(t, err)
	}

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.JetStream = &JetStreamConfig{AckWait: 5 * time.Second}
	cfg.Logs.Stream = "OTEL_LOGS"
	cfg.Logs.Durable = "otel_logs_consumer"

	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := createLogsReceiver(ctx, set, cfg, sink)
	require.NoError(t, err)

	require.NoError(t, rcv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 20*time.Millisecond, "both stream messages should be delivered downstream")

	// All delivered messages should have been acknowledged (nothing left pending).
	require.Eventually(t, func() bool {
		info, infoErr := stream.Info(ctx)
		return infoErr == nil && info.State.Consumers == 1
	}, 5*time.Second, 50*time.Millisecond)
}

// TestReceiver_JetStream_PoisonMessage verifies that an unparseable payload is
// terminated (not redelivered forever) while valid payloads still flow.
func TestReceiver_JetStream_PoisonMessage(t *testing.T) {
	t.Parallel()

	url := runServer(t, true)
	ctx := context.Background()

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "OTEL_LOGS",
		Subjects: []string{"otel_logs"},
	})
	require.NoError(t, err)

	_, err = js.Publish(ctx, "otel_logs", []byte("not-otlp"))
	require.NoError(t, err)
	_, err = js.Publish(ctx, "otel_logs", otlpProtoLogs(t))
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.JetStream = &JetStreamConfig{AckWait: 2 * time.Second}
	cfg.Logs.Stream = "OTEL_LOGS"
	cfg.Logs.Durable = "otel_logs_consumer"

	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := createLogsReceiver(ctx, set, cfg, sink)
	require.NoError(t, err)

	require.NoError(t, rcv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 20*time.Millisecond, "the valid payload should be delivered")

	// The poison message is terminated, so the valid one is the only delivery.
	assert.Equal(t, 1, sink.LogRecordCount())
}

// TestReceiver_CoreNATS verifies the core-NATS path (JetStream unset) forwards
// subscribed payloads downstream.
func TestReceiver_CoreNATS(t *testing.T) {
	t.Parallel()

	url := runServer(t, false)
	ctx := context.Background()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url // JetStream nil -> core path

	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := createLogsReceiver(ctx, set, cfg, sink)
	require.NoError(t, err)

	require.NoError(t, rcv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	payload := otlpProtoLogs(t)
	for i := 0; i < 2; i++ {
		require.NoError(t, nc.Publish("otel_logs", payload))
	}
	require.NoError(t, nc.Flush())

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 20*time.Millisecond)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())

	bad := createDefaultConfig().(*Config)
	bad.Logs.Encoding = "protobuf" // unsupported
	assert.Error(t, bad.Validate())

	negative := createDefaultConfig().(*Config)
	negative.JetStream = &JetStreamConfig{MaxDeliver: -1}
	assert.Error(t, negative.Validate())

	both := createDefaultConfig().(*Config)
	both.Logs.Encoding = "otlp_json"
	both.Logs.EncodingExtension = "foo"
	assert.Error(t, both.Validate(), "encoding and encoding_extension are mutually exclusive")
}

// extensionHost is a component.Host that exposes a fixed set of extensions.
type extensionHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h *extensionHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

// jsonLogsExtension is a minimal encoding extension that unmarshals OTLP JSON logs.
type jsonLogsExtension struct{}

func (jsonLogsExtension) Start(context.Context, component.Host) error { return nil }
func (jsonLogsExtension) Shutdown(context.Context) error             { return nil }
func (jsonLogsExtension) UnmarshalLogs(data []byte) (plog.Logs, error) {
	u := &plog.JSONUnmarshaler{}
	return u.UnmarshalLogs(data)
}

// TestReceiver_EncodingExtension verifies that an encoding extension resolved from
// the host is used to unmarshal incoming payloads.
func TestReceiver_EncodingExtension(t *testing.T) {
	t.Parallel()

	url := runServer(t, false)
	ctx := context.Background()

	extID := component.MustNewID("fakeencoding")
	host := &extensionHost{
		Host: componenttest.NewNopHost(),
		extensions: map[component.ID]component.Component{
			extID: jsonLogsExtension{},
		},
	}

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.Logs.EncodingExtension = extID.String()

	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := createLogsReceiver(ctx, set, cfg, sink)
	require.NoError(t, err)

	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	// Publish JSON-encoded logs, which only the extension (not the default
	// otlp_proto) can decode.
	jsonMarshaler := &plog.JSONMarshaler{}
	payload, err := jsonMarshaler.MarshalLogs(testdata.GenerateLogs(1))
	require.NoError(t, err)
	require.NoError(t, nc.Publish("otel_logs", payload))
	require.NoError(t, nc.Flush())

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 20*time.Millisecond, "the JSON payload should be decoded via the extension")
}

// TestReceiver_EncodingExtension_NotFound verifies that a missing extension is a
// Start error.
func TestReceiver_EncodingExtension_NotFound(t *testing.T) {
	t.Parallel()

	url := runServer(t, false)
	ctx := context.Background()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = url
	cfg.Logs.EncodingExtension = "missing"

	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := createLogsReceiver(ctx, set, cfg, sink)
	require.NoError(t, err)

	err = rcv.Start(ctx, componenttest.NewNopHost())
	assert.Error(t, err)
}
