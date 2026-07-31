# Collector container image

The NATS exporter/receiver are **not in any public Collector distribution** and
Collector components are compiled in (not loaded as plugins), so running them in
Kubernetes requires a custom image built with the
[OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/cmd/builder)
(`ocb`). This directory builds one.

Components baked in: `otlp` + `natscore` receivers, `otlp` + `debug` +
`natscore` exporters, `batch` processor. See [`builder-config.yaml`](./builder-config.yaml).

## Build & push

Build from the **repo root** (the build context must include the local modules):

```sh
REG=<your-registry>                        # e.g. ghcr.io/<org> or a cloud registry
TAG=$(git rev-parse --short HEAD)          # immutable tag; don't rely on :latest

docker build -f deploy/Dockerfile -t "$REG/otelcol-nats:$TAG" .
docker push "$REG/otelcol-nats:$TAG"
```

To build for amd64 nodes from an arm64 Mac, cross-build:

```sh
docker buildx build --platform linux/amd64 -f deploy/Dockerfile \
  -t "$REG/otelcol-nats:$TAG" --push .
```

Then reference `$REG/otelcol-nats:$TAG` as the image in your Kubernetes
deployment (Deployment/DaemonSet manifest, Helm values, etc.).

## Runtime config

The image expects a Collector config at `/etc/otelcol/config.yaml` (mounted from
a ConfigMap) and, for authenticated NATS, a `.creds` file (mounted from a
Secret). A minimal two-pipeline example
(`OTLP → natscore → JetStream → natscore → Jaeger`):

```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }
  natscore:
    endpoint: tls://nats.example.com:4222
    nkey_user_file: { user_file: /etc/nats/creds/collector.creds }
    jetstream: { domain: my_domain, ack_wait: 30s, max_deliver: 5 }
    traces: { subject: otel_spans, encoding: otlp_proto, stream: OTEL_SPANS, durable: otel_spans_consumer }
exporters:
  natscore:
    endpoint: tls://nats.example.com:4222
    nkey_user_file: { user_file: /etc/nats/creds/collector.creds }
    jetstream: { domain: my_domain }
    traces: { subject: '"otel_spans"', marshaler: otlp_proto }   # OTTL literal → inner quotes
  otlp/jaeger:
    endpoint: jaeger.jaeger.svc.cluster.local:4317
    tls: { insecure: true }
processors:
  batch: {}
service:
  pipelines:
    traces/produce: { receivers: [otlp], processors: [batch], exporters: [natscore] }
    traces/consume: { receivers: [natscore], processors: [batch], exporters: [otlp/jaeger] }
```

The `OTEL_SPANS` JetStream stream (subject `otel_spans`) must exist — the
components never create streams.
