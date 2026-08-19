# Demo: a Collector running the NATS exporter + receiver

This builds a custom OpenTelemetry Collector containing both components and pushes
a trace through them end to end:

```
OTLP in ─▶ nats exporter ─▶ NATS JetStream (OTEL_SPANS) ─▶ nats receiver ─▶ debug (console)
```

Both directions run in a single Collector process, so one injected trace is
exported to NATS by our exporter and then read back from NATS by our receiver.

## Prerequisites

- Go 1.25+
- `curl`

Nothing else needs installing — the NATS server, the stream-setup helper, and the
[Collector Builder (`ocb`)](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/cmd/builder)
are all launched via `go run …@version`.

## Run it

```sh
./run.sh
```

It will:

1. start a local NATS server with JetStream,
2. create the `OTEL_SPANS` stream (the components don't create streams),
3. build the Collector from [`builder-config.yaml`](./builder-config.yaml) with `ocb`,
4. start it with [`config.yaml`](./config.yaml),
5. POST [`trace.json`](./trace.json) to the OTLP receiver, and
6. print the span as the **receive** side logs it — proving the round trip.

The first run compiles a full Collector, so give it a minute.

## Files

| File | Purpose |
|------|---------|
| `builder-config.yaml` | `ocb` manifest: which components to compile in (+ local `replaces`) |
| `config.yaml` | Collector config: the produce and consume pipelines |
| `trace.json` | A minimal OTLP/HTTP trace payload to inject |
| `streamsetup/` | Tiny helper that creates the JetStream stream |
| `run.sh` | Orchestrates the whole thing |

## Switching to core NATS

To try the non-durable path, drop the `jetstream` blocks from both the exporter and
receiver in `config.yaml` (and skip stream setup). Note that with core NATS the
receiver must be subscribed before the exporter publishes, so inject after startup.
