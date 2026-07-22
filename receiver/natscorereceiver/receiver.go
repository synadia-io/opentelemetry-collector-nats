// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/synadia-labs/opentelemetry-collector-nats/internal/natsclient"
)

// unmarshalFunc turns a raw NATS payload into a pdata signal value.
type unmarshalFunc[T any] func([]byte) (T, error)

// consumeFunc hands a pdata signal value to the next consumer in the pipeline.
type consumeFunc[T any] func(context.Context, T) error

// unmarshalerResolver resolves the unmarshaler for a signal given the host. It is
// invoked at Start so that encoding extensions (only reachable via the host) can
// be looked up.
type unmarshalerResolver[T any] func(host component.Host) (unmarshalFunc[T], error)

// natsReceiver consumes one signal type from NATS. It subscribes to a core NATS
// subject or, when JetStream is configured, binds a durable consumer to a stream.
type natsReceiver[T any] struct {
	cfg       *Config
	signalCfg *SignalConfig
	settings  receiver.Settings
	resolve   unmarshalerResolver[T]
	consume   consumeFunc[T]

	unmarshal  unmarshalFunc[T] // resolved in Start
	conn       *nats.Conn
	sub        *nats.Subscription       // core NATS
	consumeCtx jetstream.ConsumeContext // JetStream

	ctx    context.Context
	cancel context.CancelFunc
}

func newNatsReceiver[T any](
	settings receiver.Settings,
	cfg *Config,
	signalCfg *SignalConfig,
	resolve unmarshalerResolver[T],
	consume consumeFunc[T],
) *natsReceiver[T] {
	return &natsReceiver[T]{
		cfg:       cfg,
		signalCfg: signalCfg,
		settings:  settings,
		resolve:   resolve,
		consume:   consume,
	}
}

func (r *natsReceiver[T]) Start(ctx context.Context, host component.Host) error {
	// A long-lived context for message handling, decoupled from Start's ctx.
	r.ctx, r.cancel = context.WithCancel(context.Background())

	unmarshal, err := r.resolve(host)
	if err != nil {
		return err
	}
	r.unmarshal = unmarshal

	conn, err := natsclient.Connect(ctx, natsclient.Params{
		Endpoint: r.cfg.Endpoint,
		Pedantic: r.cfg.Pedantic,
		TLS:      r.cfg.TLS,
		Auth:     r.cfg.Auth,
	})
	if err != nil {
		return err
	}
	r.conn = conn

	if r.cfg.JetStream != nil {
		return r.startJetStream(ctx)
	}
	return r.startCore()
}

// startCore subscribes to a core NATS subject. Core NATS has no acknowledgement
// or redelivery: a payload that fails to unmarshal or that the pipeline rejects
// is logged and dropped.
func (r *natsReceiver[T]) startCore() error {
	sub, err := r.conn.Subscribe(r.signalCfg.Subject, func(msg *nats.Msg) {
		r.handle(msg.Data, nil)
	})
	if err != nil {
		return err
	}
	r.sub = sub
	return nil
}

// startJetStream binds a durable consumer to the configured stream and consumes
// messages, acknowledging each only after the pipeline accepts it.
func (r *natsReceiver[T]) startJetStream(ctx context.Context) error {
	if r.signalCfg.Stream == "" {
		return errors.New("jetstream mode requires a stream to be configured for this signal")
	}

	var js jetstream.JetStream
	var err error
	if r.cfg.JetStream.Domain != "" {
		js, err = jetstream.NewWithDomain(r.conn, r.cfg.JetStream.Domain)
	} else {
		js, err = jetstream.New(r.conn)
	}
	if err != nil {
		return err
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       r.signalCfg.Durable,
		FilterSubject: r.signalCfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}
	if r.cfg.JetStream.AckWait > 0 {
		consumerCfg.AckWait = r.cfg.JetStream.AckWait
	}
	if r.cfg.JetStream.MaxDeliver > 0 {
		consumerCfg.MaxDeliver = r.cfg.JetStream.MaxDeliver
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, r.signalCfg.Stream, consumerCfg)
	if err != nil {
		return err
	}

	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		r.handle(msg.Data(), msg)
	})
	if err != nil {
		return err
	}
	r.consumeCtx = consumeCtx
	return nil
}

// handle unmarshals a payload and forwards it downstream. When msg is non-nil
// (JetStream), it acknowledges on success, terminates the message on an
// unmarshal error (a poison payload should not be redelivered), and negatively
// acknowledges on a downstream error so the server redelivers it.
func (r *natsReceiver[T]) handle(data []byte, msg jetstream.Msg) {
	signal, err := r.unmarshal(data)
	if err != nil {
		r.settings.Logger.Error("failed to unmarshal payload; dropping", zap.Error(err))
		if msg != nil {
			_ = msg.TermWithReason("unmarshal error")
		}
		return
	}

	if err := r.consume(r.ctx, signal); err != nil {
		r.settings.Logger.Error("downstream consumer rejected data; will retry", zap.Error(err))
		if msg != nil {
			_ = msg.Nak()
		}
		return
	}

	if msg != nil {
		_ = msg.Ack()
	}
}

func (r *natsReceiver[T]) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.consumeCtx != nil {
		r.consumeCtx.Stop()
	}
	if r.sub != nil {
		_ = r.sub.Unsubscribe()
	}
	if r.conn != nil {
		r.conn.Close()
	}
	return nil
}
