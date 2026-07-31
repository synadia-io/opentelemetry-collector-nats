// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-io/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/multierr"

	"github.com/synadia-io/opentelemetry-collector-nats/internal/natsclient"
)

// SignalConfig defines the per-signal receive configuration.
type SignalConfig struct {
	// Subject is the NATS subject to subscribe to (core NATS) or the filter
	// subject for the JetStream consumer.
	Subject string `mapstructure:"subject"`

	// Encoding selects a built-in unmarshaler for incoming payloads. Mutually
	// exclusive with EncodingExtension.
	//
	// Supported encodings:
	//  - otlp_proto (default)
	//  - otlp_json
	Encoding string `mapstructure:"encoding"`

	// EncodingExtension is the component ID of an encoding extension used to
	// unmarshal incoming payloads. Mutually exclusive with Encoding. The extension
	// must implement the pdata unmarshaler for the signal (plog.Unmarshaler,
	// pmetric.Unmarshaler, or ptrace.Unmarshaler).
	//
	// See: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/encoding
	EncodingExtension string `mapstructure:"encoding_extension"`

	// Stream is the JetStream stream to consume from. Required in JetStream mode.
	Stream string `mapstructure:"stream"`

	// Durable is the durable consumer name. Recommended in JetStream mode so that
	// consumption progress survives receiver restarts. Empty creates an ephemeral
	// consumer.
	Durable string `mapstructure:"durable"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

func (c *SignalConfig) Validate() error {
	if c.Encoding != "" && c.EncodingExtension != "" {
		return errors.New("encoding configured more than once")
	}

	if c.Encoding != "" {
		switch c.Encoding {
		case encodingOtlpProto, encodingOtlpJSON:
		default:
			return fmt.Errorf("unsupported encoding: %q", c.Encoding)
		}
	}

	if c.EncodingExtension != "" {
		var id component.ID
		if err := id.UnmarshalText([]byte(c.EncodingExtension)); err != nil {
			return fmt.Errorf("failed to parse encoding extension name: %w", err)
		}
	}

	return nil
}

// JetStreamConfig configures consumption via a NATS JetStream durable consumer
// (acknowledged, with redelivery on failure) instead of a core NATS subscription.
//
// See: https://docs.nats.io/nats-concepts/jetstream/consumers
type JetStreamConfig struct {
	// Domain optionally selects a JetStream domain, e.g. when consuming through a
	// leaf node from a hub. Empty uses the server's default domain.
	Domain string `mapstructure:"domain"`

	// AckWait bounds how long the server waits for an ack before redelivering a
	// message. Zero uses the server default.
	AckWait time.Duration `mapstructure:"ack_wait"`

	// MaxDeliver caps redelivery attempts. Zero uses the server default.
	MaxDeliver int `mapstructure:"max_deliver"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

func (c *JetStreamConfig) Validate() error {
	var errs error
	if c.AckWait < 0 {
		errs = multierr.Append(errs, errors.New("jetstream ack_wait must not be negative"))
	}
	if c.MaxDeliver < 0 {
		errs = multierr.Append(errs, errors.New("jetstream max_deliver must not be negative"))
	}
	return errs
}

// Config defines the configuration for the NATS core receiver.
type Config struct {
	// Endpoint is the NATS server URL.
	Endpoint string `mapstructure:"endpoint"`

	// Pedantic is the option to enable/disable NATS pedantic mode.
	Pedantic bool `mapstructure:"pedantic"`

	// TLS holds the TLS configuration for the NATS client.
	TLS configtls.ClientConfig `mapstructure:"tls"`

	// JetStream, when set, consumes via a JetStream durable consumer (acked,
	// redelivered on failure) instead of a core NATS subscription.
	JetStream *JetStreamConfig `mapstructure:"jetstream"`

	// Logs holds the configuration for the logs signal.
	Logs SignalConfig `mapstructure:"logs"`
	// Metrics holds the configuration for the metrics signal.
	Metrics SignalConfig `mapstructure:"metrics"`
	// Traces holds the configuration for the traces signal.
	Traces SignalConfig `mapstructure:"traces"`

	// Auth holds the configuration for NATS auth.
	Auth natsclient.AuthConfig `mapstructure:",squash"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

func (c *Config) Validate() error {
	var errs error
	if err := c.TLS.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}
	errs = multierr.Append(errs, c.Logs.Validate())
	errs = multierr.Append(errs, c.Metrics.Validate())
	errs = multierr.Append(errs, c.Traces.Validate())
	errs = multierr.Append(errs, c.Auth.Validate())
	if c.JetStream != nil {
		errs = multierr.Append(errs, c.JetStream.Validate())
	}
	return errs
}
