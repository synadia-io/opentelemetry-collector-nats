// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/multierr"
)

// SignalConfig defines the per-signal receive configuration.
type SignalConfig struct {
	// Subject is the NATS subject to subscribe to (core NATS) or the filter
	// subject for the JetStream consumer.
	Subject string `mapstructure:"subject"`

	// Encoding of incoming payloads.
	//
	// Supported encodings:
	//  - otlp_proto (default)
	//  - otlp_json
	Encoding string `mapstructure:"encoding"`

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
	switch c.Encoding {
	case "", encodingOtlpProto, encodingOtlpJSON:
	default:
		return fmt.Errorf("unsupported encoding: %q", c.Encoding)
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

// TokenConfig defines the configuration for token auth.
type TokenConfig struct {
	// Token is the token to use for token auth.
	Token string `mapstructure:"token"`
}

// UserConfig defines the configuration for username/password auth.
type UserConfig struct {
	// Username is the username to use for username/password auth.
	Username string `mapstructure:"username"`
	// Password is the password to use for username/password auth.
	Password string `mapstructure:"password"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyConfig defines the configuration for NKey auth.
type NkeyConfig struct {
	// PublicKey is the public key to use for NKey auth.
	PublicKey string `mapstructure:"public_key"`
	// Seed is the seed to use for NKey auth.
	Seed []byte `mapstructure:"seed"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyJWTConfig defines the configuration for NKey auth via JWT.
type NkeyJWTConfig struct {
	// JWT is the JWT to use for NKey auth via JWT.
	JWT string `mapstructure:"jwt"`
	// Seed is the seed to use for NKey auth via JWT.
	Seed []byte `mapstructure:"seed"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyUserFileConfig defines the configuration for NKey auth via user file.
type NkeyUserFileConfig struct {
	// UserFilePath is the path to the user file (credentials) to use.
	UserFilePath string `mapstructure:"user_file"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// AuthConfig defines the auth configuration for the NATS client.
//
// See: https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro
type AuthConfig struct {
	// Token holds the configuration for token auth.
	Token *TokenConfig `mapstructure:"token"`

	// User holds the configuration for username/password auth.
	User *UserConfig `mapstructure:"user"`

	// Nkey holds the configuration for NKey auth.
	Nkey *NkeyConfig `mapstructure:"nkey"`

	// NkeyJWT holds the configuration for NKey auth via JWT.
	NkeyJWT *NkeyJWTConfig `mapstructure:"nkey_jwt"`

	// NkeyUserFile holds the configuration for NKey auth via user file.
	NkeyUserFile *NkeyUserFileConfig `mapstructure:"nkey_user_file"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

func (c *TokenConfig) Validate() error {
	if c.Token == "" {
		return errors.New("incomplete token auth configuration")
	}
	return nil
}

func (c *UserConfig) Validate() error {
	if c.Username == "" || c.Password == "" {
		return errors.New("incomplete username/password auth configuration")
	}
	return nil
}

func (c *NkeyConfig) Validate() error {
	if c.PublicKey == "" || c.Seed == nil {
		return errors.New("incomplete NKey auth configuration")
	}
	return nil
}

func (c *NkeyJWTConfig) Validate() error {
	if c.JWT == "" || c.Seed == nil {
		return errors.New("incomplete NKey auth (via JWT) configuration")
	}
	return nil
}

func (c *NkeyUserFileConfig) Validate() error {
	if c.UserFilePath == "" {
		return errors.New("incomplete NKey auth (via user file) configuration")
	}
	return nil
}

func (c *AuthConfig) Validate() error {
	var errs error

	if c.Token != nil {
		errs = multierr.Append(errs, c.Token.Validate())
	}
	if c.User != nil {
		errs = multierr.Append(errs, c.User.Validate())
	}
	if c.Nkey != nil {
		errs = multierr.Append(errs, c.Nkey.Validate())
	}
	if c.NkeyJWT != nil {
		errs = multierr.Append(errs, c.NkeyJWT.Validate())
	}
	if c.NkeyUserFile != nil {
		errs = multierr.Append(errs, c.NkeyUserFile.Validate())
	}

	isConfiguredCount := 0
	for _, isConfigured := range []bool{
		c.Nkey != nil,
		c.NkeyJWT != nil,
		c.NkeyUserFile != nil,
	} {
		if isConfigured {
			isConfiguredCount++
		}
	}
	if isConfiguredCount > 1 {
		errs = multierr.Append(errs, errors.New("NKey auth configured more than once"))
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
	Auth AuthConfig `mapstructure:",squash"`

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
