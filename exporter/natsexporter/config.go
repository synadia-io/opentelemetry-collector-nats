// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsexporter // import "github.com/synadia-io/opentelemetry-collector-nats/exporter/natsexporter"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"github.com/synadia-io/opentelemetry-collector-nats/exporter/natsexporter/internal/marshaler"
	"github.com/synadia-io/opentelemetry-collector-nats/internal/natsclient"
)

// SignalConfig defines the configuration for a signal type.
type SignalConfig struct {
	// Subject is the OTTL value expression used to construct the NATS subject.
	//
	// See: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/README.md
	//
	// See: https://docs.nats.io/nats-concepts/subjects#subject-based-filtering-and-security
	Subject string `mapstructure:"subject"`

	// BuiltinMarshalerName is the name of the built-in marshaler to use when marshaling the signal type.
	//
	// Supported marshalers:
	//  - otlp_proto
	//  - otlp_json
	BuiltinMarshalerName marshaler.BuiltinMarshalerName `mapstructure:"marshaler"`
	// EncodingExtensionName is the name of the encoding extension to use when marshaling the signal type.
	//
	// See: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/encoding
	EncodingExtensionName string `mapstructure:"encoding_extension"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// LogsConfig defines the configuration for logs.
type LogsConfig SignalConfig

// MetricsConfig defines the configuration for metrics.
type MetricsConfig SignalConfig

// TracesConfig defines the configuration for traces.
type TracesConfig SignalConfig

// JetStreamConfig configures publishing via NATS JetStream (durable, acknowledged
// delivery) instead of core NATS. When present, every exported payload is published
// with JetStream and the publish blocks until the server acknowledges persistence.
//
// A stream must already exist on the server whose subjects capture the configured
// signal subjects; this exporter does not create or manage streams.
//
// See: https://docs.nats.io/nats-concepts/jetstream
type JetStreamConfig struct {
	// Domain optionally selects a JetStream domain, e.g. when publishing through a
	// leaf node to a hub. Empty uses the server's default domain.
	//
	// See: https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes
	Domain string `mapstructure:"domain"`

	// PublishTimeout bounds how long to wait for each publish acknowledgement.
	// Zero means no exporter-imposed deadline (the surrounding context still applies).
	PublishTimeout time.Duration `mapstructure:"publish_timeout"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// Config defines the configuration for the NATS core exporter.
type Config struct {
	// Endpoint is the NATS server URL.
	Endpoint string `mapstructure:"endpoint"`

	// Pedantic is the option to enable/disable NATS pedantic mode.
	Pedantic bool `mapstructure:"pedantic"`

	// TLS holds the TLS configuration for the NATS client.
	TLS configtls.ClientConfig `mapstructure:"tls"`

	// JetStream, when set, publishes via NATS JetStream (durable, acknowledged
	// delivery) instead of core NATS.
	JetStream *JetStreamConfig `mapstructure:"jetstream"`

	// Logs holds the configuration for the logs signal.
	Logs LogsConfig `mapstructure:"logs"`
	// Metrics holds the configuration for the metrics signal.
	Metrics MetricsConfig `mapstructure:"metrics"`
	// Traces holds the configuration for the traces signal.
	Traces TracesConfig `mapstructure:"traces"`

	// Auth holds the configuration for NATS auth.
	Auth natsclient.AuthConfig `mapstructure:",squash"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

func (c *SignalConfig) Validate() error {
	var errs error

	if c.BuiltinMarshalerName != "" && c.EncodingExtensionName != "" {
		errs = multierr.Append(errs, errors.New("marshaler configured more than once"))
	}

	if c.BuiltinMarshalerName != "" {
		if c.BuiltinMarshalerName != marshaler.OtlpProtoBuiltinMarshalerName &&
			c.BuiltinMarshalerName != marshaler.OtlpJSONBuiltinMarshalerName {
			errs = multierr.Append(errs, fmt.Errorf("unsupported built-in marshaler: %s", c.BuiltinMarshalerName))
		}
	}

	if c.EncodingExtensionName != "" {
		var id component.ID
		if err := id.UnmarshalText([]byte(c.EncodingExtensionName)); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("failed to unmarshal encoding extension name: %w", err))
		}
	}

	return errs
}

func (c *LogsConfig) Validate() error {
	errs := (*SignalConfig)(c).Validate()

	if c.Subject != "" {
		parser, err := ottllog.NewParser(
			ottlfuncs.StandardConverters[ottllog.TransformContext](),
			componenttest.NewNopTelemetrySettings(),
		)
		if err != nil {
			panic(fmt.Errorf("failed to create logs parser: %w", err))
		}

		if _, err = parser.ParseValueExpression(c.Subject); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("failed to parse logs subject: %w", err))
		}
	}

	return errs
}

func (c *MetricsConfig) Validate() error {
	errs := (*SignalConfig)(c).Validate()

	if c.Subject != "" {
		parser, err := ottlmetric.NewParser(
			ottlfuncs.StandardConverters[ottlmetric.TransformContext](),
			componenttest.NewNopTelemetrySettings(),
		)
		if err != nil {
			panic(fmt.Errorf("failed to create metrics parser: %w", err))
		}

		if _, err = parser.ParseValueExpression(c.Subject); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("failed to parse metrics subject: %w", err))
		}
	}

	return errs
}

func (c *TracesConfig) Validate() error {
	errs := (*SignalConfig)(c).Validate()

	if c.Subject != "" {
		parser, err := ottlspan.NewParser(
			ottlfuncs.StandardConverters[ottlspan.TransformContext](),
			componenttest.NewNopTelemetrySettings(),
		)
		if err != nil {
			panic(fmt.Errorf("failed to create traces parser: %w", err))
		}

		if _, err = parser.ParseValueExpression(c.Subject); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("failed to parse traces subject: %w", err))
		}
	}

	return errs
}

func (c *JetStreamConfig) Validate() error {
	if c.PublishTimeout < 0 {
		return errors.New("jetstream publish_timeout must not be negative")
	}
	return nil
}

func (c *Config) Validate() error {
	var errs error
	if err := c.TLS.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := c.Logs.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := c.Metrics.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := c.Traces.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := c.Auth.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if c.JetStream != nil {
		if err := c.JetStream.Validate(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}
