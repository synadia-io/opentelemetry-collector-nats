// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/synadia-labs/opentelemetry-collector-nats/internal/natsclient"
	"github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			expected: &Config{
				Endpoint: "nats://localhost:1234",
				Pedantic: true,
				TLS:      configtls.NewDefaultClientConfig(),
				JetStream: &JetStreamConfig{
					AckWait:    30 * time.Second,
					MaxDeliver: 5,
				},
				Logs: SignalConfig{
					Subject:  "otel_logs",
					Encoding: "otlp_json",
					Stream:   "OTEL_LOGS",
					Durable:  "otel_logs_consumer",
				},
				Metrics: SignalConfig{
					Subject:  "otel_metrics",
					Encoding: "otlp_json",
					Stream:   "OTEL_METRICS",
					Durable:  "otel_metrics_consumer",
				},
				Traces: SignalConfig{
					Subject:  "otel_spans",
					Encoding: "otlp_json",
					Stream:   "OTEL_TRACES",
					Durable:  "otel_traces_consumer",
				},
				Auth: natsclient.AuthConfig{
					Token: &natsclient.TokenConfig{
						Token: "token",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			require.NoError(t, cfg.Validate())
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
