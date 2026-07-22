# NATS Core Receiver

| Status        |           |
| ------------- |-----------|
| Stability     | development: metrics, logs, traces |
| Distributions | (none yet — incubating) |

Receives OTLP-encoded logs, metrics, and traces from [NATS](https://docs.nats.io/).
It can either subscribe to a core NATS subject or, when `jetstream` is configured,
bind a **durable JetStream consumer** to a stream and acknowledge each message only
after the pipeline accepts it.

This is the receive-side counterpart to the [`natscoreexporter`](../../exporter/natscoreexporter).

## Delivery semantics

- **Core NATS** (default): fire-and-forget. There is no acknowledgement or
  redelivery — a payload that fails to decode or that the pipeline rejects is
  logged and dropped.
- **JetStream** (`jetstream` set): the receiver binds a durable consumer with an
  explicit ack policy and:
  - **acks** a message once the downstream consumer accepts it,
  - **naks** it (triggering redelivery) when the downstream consumer returns an
    error,
  - **terminates** it (no redelivery) when the payload cannot be unmarshaled, so a
    poison message does not loop forever.

  A stream capturing the configured subject(s) must already exist; the receiver
  does not create or manage streams.

## Configuration

- `endpoint` (default = `nats://localhost:4222`): The NATS server URL.
- `pedantic` (default = true): Enable/disable NATS pedantic mode.
- `tls`: See [TLS Configuration Settings](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md).
- `jetstream`: When set, consume via a JetStream durable consumer instead of core NATS.
  - `domain`: Optional JetStream domain to target.
  - `ack_wait` (default = 0): How long the server waits for an ack before redelivery. `0` uses the server default.
  - `max_deliver` (default = 0): Maximum redelivery attempts. `0` uses the server default.
- `logs` / `metrics` / `traces`:
  - `subject` (defaults: `otel_logs` / `otel_metrics` / `otel_spans`): Subject to subscribe to (core) or the consumer's filter subject (JetStream).
  - `encoding` (default = `otlp_proto`): Built-in payload encoding. One of `otlp_proto`, `otlp_json`. Mutually exclusive with `encoding_extension`.
  - `encoding_extension`: Component ID of an [encoding extension](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/encoding) used to unmarshal payloads (e.g. `otlp_encoding/1`). The extension must implement the pdata unmarshaler for the signal. Mutually exclusive with `encoding`.
  - `stream`: JetStream stream to consume from. Required in JetStream mode.
  - `durable`: Durable consumer name. Recommended in JetStream mode so progress survives restarts; empty creates an ephemeral consumer.
- `auth`: Same options as the exporter — `token`, `user`, `nkey`, `nkey_jwt`, `nkey_user_file`.

## Example (JetStream durable consumer)

```yaml
receivers:
  natscore:
    endpoint: nats://localhost:4222
    jetstream:
      ack_wait: 30s
      max_deliver: 5
    logs:
      subject: otel_logs
      stream: OTEL_LOGS
      durable: otel_logs_consumer
```

## Roadmap / notes

- The NATS connection + auth code is shared with the exporter via the
  [`internal/natsclient`](../../internal/natsclient) module.
- Encoding-extension support (beyond the built-in `otlp_proto` / `otlp_json`) is a
  planned addition, to match the exporter.
