// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"context"
	"os"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/multierr"
)

func setNatsTLSOption(options *nats.Options, ctx context.Context, cfg *configtls.ClientConfig) error {
	tlsConfig, err := cfg.LoadTLSConfig(ctx)
	if err != nil {
		return err
	}
	options.TLSConfig = tlsConfig
	return nil
}

func setNatsTokenOption(options *nats.Options, cfg *TokenConfig) {
	options.Token = cfg.Token
}

func setNatsUserOption(options *nats.Options, cfg *UserConfig) {
	options.User = cfg.Username
	options.Password = cfg.Password
}

func setNatsNkeyOption(options *nats.Options, cfg *NkeyConfig) error {
	keyPair, err := nkeys.FromSeed(cfg.Seed)
	if err != nil {
		return err
	}

	options.Nkey = cfg.PublicKey
	options.SignatureCB = keyPair.Sign
	return nil
}

func setNatsNkeyJWTOption(options *nats.Options, cfg *NkeyJWTConfig) error {
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

func setNatsNkeyUserFileOption(options *nats.Options, cfg *NkeyUserFileConfig) error {
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

func setNatsAuthOption(options *nats.Options, cfg *AuthConfig) error {
	var errs error
	if cfg.User != nil {
		setNatsUserOption(options, cfg.User)
	}
	if cfg.Token != nil {
		setNatsTokenOption(options, cfg.Token)
	}
	if cfg.Nkey != nil {
		errs = multierr.Append(errs, setNatsNkeyOption(options, cfg.Nkey))
	}
	if cfg.NkeyJWT != nil {
		errs = multierr.Append(errs, setNatsNkeyJWTOption(options, cfg.NkeyJWT))
	}
	if cfg.NkeyUserFile != nil {
		errs = multierr.Append(errs, setNatsNkeyUserFileOption(options, cfg.NkeyUserFile))
	}
	return errs
}

func createNats(ctx context.Context, cfg *Config) (*nats.Conn, error) {
	var errs error
	options := nats.GetDefaultOptions()
	options.Url = cfg.Endpoint
	options.Pedantic = cfg.Pedantic
	errs = multierr.Append(errs, setNatsTLSOption(&options, ctx, &cfg.TLS))
	errs = multierr.Append(errs, setNatsAuthOption(&options, &cfg.Auth))
	if errs != nil {
		return nil, errs
	}

	conn, err := options.Connect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}
