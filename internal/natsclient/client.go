// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsclient // import "github.com/suckatrash/opentelemetry-collector-nats/internal/natsclient"

import (
	"context"
	"os"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/multierr"
)

// Params holds the settings needed to open a NATS connection.
type Params struct {
	// Endpoint is the NATS server URL.
	Endpoint string
	// Pedantic enables/disables NATS pedantic mode.
	Pedantic bool
	// TLS holds the TLS configuration for the NATS client.
	TLS configtls.ClientConfig
	// Auth holds the NATS authentication configuration.
	Auth AuthConfig
}

// Connect opens a NATS connection using the given params.
func Connect(ctx context.Context, p Params) (*nats.Conn, error) {
	var errs error
	options := nats.GetDefaultOptions()
	options.Url = p.Endpoint
	options.Pedantic = p.Pedantic
	errs = multierr.Append(errs, setTLSOption(&options, ctx, &p.TLS))
	errs = multierr.Append(errs, setAuthOption(&options, &p.Auth))
	if errs != nil {
		return nil, errs
	}

	conn, err := options.Connect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func setTLSOption(options *nats.Options, ctx context.Context, cfg *configtls.ClientConfig) error {
	tlsConfig, err := cfg.LoadTLSConfig(ctx)
	if err != nil {
		return err
	}
	options.TLSConfig = tlsConfig
	return nil
}

func setTokenOption(options *nats.Options, cfg *TokenConfig) {
	options.Token = cfg.Token
}

func setUserOption(options *nats.Options, cfg *UserConfig) {
	options.User = cfg.Username
	options.Password = cfg.Password
}

func setNkeyOption(options *nats.Options, cfg *NkeyConfig) error {
	keyPair, err := nkeys.FromSeed(cfg.Seed)
	if err != nil {
		return err
	}

	options.Nkey = cfg.PublicKey
	options.SignatureCB = keyPair.Sign
	return nil
}

func setNkeyJWTOption(options *nats.Options, cfg *NkeyJWTConfig) error {
	keyPair, err := nkeys.FromSeed(cfg.Seed)
	if err != nil {
		return err
	}

	options.UserJWT = func() (string, error) {
		return cfg.JWT, nil
	}
	options.SignatureCB = keyPair.Sign
	return nil
}

func setNkeyUserFileOption(options *nats.Options, cfg *NkeyUserFileConfig) error {
	var errs error
	userConfig, err := os.ReadFile(cfg.UserFilePath)
	errs = multierr.Append(errs, err)
	userJWT, err := jwt.ParseDecoratedJWT(userConfig)
	errs = multierr.Append(errs, err)
	keyPair, err := jwt.ParseDecoratedNKey(userConfig)
	errs = multierr.Append(errs, err)
	if errs != nil {
		return errs
	}

	options.UserJWT = func() (string, error) {
		return userJWT, nil
	}
	options.SignatureCB = keyPair.Sign
	return nil
}

func setAuthOption(options *nats.Options, cfg *AuthConfig) error {
	var errs error
	if cfg.User != nil {
		setUserOption(options, cfg.User)
	}
	if cfg.Token != nil {
		setTokenOption(options, cfg.Token)
	}
	if cfg.Nkey != nil {
		errs = multierr.Append(errs, setNkeyOption(options, cfg.Nkey))
	}
	if cfg.NkeyJWT != nil {
		errs = multierr.Append(errs, setNkeyJWTOption(options, cfg.NkeyJWT))
	}
	if cfg.NkeyUserFile != nil {
		errs = multierr.Append(errs, setNkeyUserFileOption(options, cfg.NkeyUserFile))
	}
	return errs
}
