# OpenTelemetry Collector — NATS components

Incubation repo for **NATS transport components for the OpenTelemetry Collector**:
an exporter that publishes OTLP telemetry (traces, metrics, logs) onto NATS, and
(planned) a receiver that consumes it back off NATS.

The goal is to mature these components here — build, host, test, and get real usage —
and then propose them for **donation to
[opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)**,
per the current [new-components guidance](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/docs/new-components.md).

Tracking issue: [open-telemetry/opentelemetry-collector-contrib#39540](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/39540)
— *NATS as a Receiver and Exporter*.

> Status: **early / in development.** Not yet donated, not yet in any Collector distribution.

## Components

| Component | Path | Signals | Status |
|-----------|------|---------|--------|
| NATS Core exporter | [`exporter/natscoreexporter`](./exporter/natscoreexporter) | traces, metrics, logs | in development (Core NATS + JetStream) |
| NATS Core receiver | [`receiver/natscorereceiver`](./receiver/natscorereceiver) | traces, metrics, logs | in development (Core NATS + JetStream durable consumer) |

Highlights of the exporter today:

- Generic over logs / metrics / traces.
- **OTTL-based subject routing** — static or dynamic NATS subjects derived from signal content.
- Pluggable encoding: built-in `otlp_proto` / `otlp_json` marshalers, or an encoding extension.
- **JetStream publish** — optional durable, acknowledged delivery.
- Full NATS auth matrix: user/password, token, NKey, NKey+JWT, NKey user file (creds), plus TLS.

Highlights of the receiver today:

- Core NATS subscription, or a **JetStream durable consumer** that acks after the
  pipeline accepts a message, naks on downstream error (redelivery), and terminates
  poison payloads that fail to decode.
- Built-in `otlp_proto` / `otlp_json` decoding; same NATS auth matrix as the exporter.

## Roadmap

- [x] **JetStream delivery** for the exporter (durable, acked publish) — the headline reason to prefer NATS over the plain OTLP exporter.
- [x] **NATS receiver** (Core + JetStream durable consumer, ack after downstream delivery).
- [x] Extract shared NATS connection/auth into an internal `natsclient` module.
- [x] Encoding-extension support on the receiver (parity with the exporter).
- [x] Runnable `ocb` demo building a Collector with both components (`demo/`).
- [x] Cross-component E2E round-trip tests (`test/e2e/`).
- [ ] Async/batched JetStream publish for higher throughput.
- [ ] Test coverage to the ≥80% donation bar, incl. integration tests against a real server.
- [ ] Assemble ≥3 cross-company code owners and open the donation PR.

## Testing

- **Per-component** (`exporter/…`, `receiver/…`): each is tested against a real
  embedded `nats-server` — JetStream, acks, redelivery, poison-message handling,
  encoding extensions. CI needs only Go, no Docker.
- **Cross-component round trip** (`test/e2e/`): the exporter publishes to NATS and
  the receiver reads it back through the public factories, asserting fidelity with
  `pdatatest`. This is the one test that guards the two components' shared wire
  contract — a drift in encoding or subject naming fails here even when each
  component's own tests pass.
- **Full Collector** (`demo/`): `run.sh` builds a Collector with `ocb` and drives a
  trace through both components end to end.

Run the Go tests per module:

```sh
for m in internal/natsclient exporter/natscoreexporter receiver/natscorereceiver test/e2e; do
  ( cd "$m" && go test ./... )
done
```

## Building into a Collector

Add the module to an [OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/cmd/builder) (`ocb`) manifest:

```yaml
exporters:
  - gomod: github.com/synadia-io/opentelemetry-collector-nats/exporter/natscoreexporter v0.0.0
```

(Pin to a tag once one is published; until then, use a `replace` pointing at a local checkout.)

See [`demo/`](./demo) for a complete, runnable example — it builds a Collector with
both components and pushes a trace through them (`OTLP → exporter → JetStream →
receiver → console`) with a single `./run.sh`.

## Attribution

The initial `natscoreexporter` is derived from unmerged prior art contributed by
**[@EthanKim8683](https://github.com/EthanKim8683)** to opentelemetry-collector-contrib:

- [#42186](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/42186) — New component: NATS Core Exporter
- [#42304](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/42304) — Add groupers and marshalers
- [#42452](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/42452) — Implement exporter and factory

See [NOTICE](./NOTICE). All original OpenTelemetry copyright headers are preserved.

## License

[Apache License 2.0](./LICENSE).
