// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package natsclient provides shared NATS connection and authentication
// configuration used by the NATS Collector components (exporter and receiver).
package natsclient // import "github.com/suckatrash/opentelemetry-collector-nats/internal/natsclient"

import (
	"errors"

	"go.uber.org/multierr"
)

// TokenConfig defines the configuration for token auth.
//
// See: https://pkg.go.dev/github.com/nats-io/nats.go#Token
type TokenConfig struct {
	// Token is the token to use for token auth.
	Token string `mapstructure:"token"`
}

// UserConfig defines the configuration for username/password auth.
//
// See: https://pkg.go.dev/github.com/nats-io/nats.go#UserInfo
type UserConfig struct {
	// Username is the username to use for username/password auth.
	Username string `mapstructure:"username"`
	// Password is the password to use for username/password auth.
	Password string `mapstructure:"password"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyConfig defines the configuration for NKey auth.
//
// See: https://pkg.go.dev/github.com/nats-io/nats.go#Nkey
type NkeyConfig struct {
	// PublicKey is the public key to use for NKey auth.
	PublicKey string `mapstructure:"public_key"`
	// Seed is the seed to use for NKey auth.
	Seed []byte `mapstructure:"seed"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyJWTConfig defines the configuration for NKey auth via JWT.
//
// See: https://pkg.go.dev/github.com/nats-io/nats.go#UserJWT
type NkeyJWTConfig struct {
	// JWT is the JWT to use for NKey auth via JWT.
	JWT string `mapstructure:"jwt"`
	// Seed is the seed to use for NKey auth via JWT.
	Seed []byte `mapstructure:"seed"`

	// Prevent unkeyed literal initialization
	_ struct{}
}

// NkeyUserFileConfig defines the configuration for NKey auth via user file.
//
// See: https://pkg.go.dev/github.com/nats-io/nats.go#UserCredentials
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
