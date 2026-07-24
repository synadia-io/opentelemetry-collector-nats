# Build a Collector image with the NATS components compiled in.
#
# Build context is the REPO ROOT (so all local modules are available to the
# builder's replace directives):
#
#   docker build -f deploy/Dockerfile -t <registry>/otelcol-nats:<tag> .
#
# The runtime image expects a config mounted at /etc/otelcol/config.yaml.

# ---- build stage: run ocb ----
FROM golang:1.25 AS build
WORKDIR /src
COPY . .
# ocb version must match the collector component versions in builder-config.yaml.
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.133.0
WORKDIR /src/deploy
RUN builder --config builder-config.yaml

# ---- runtime stage: distroless ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/deploy/_build/otelcol-nats /otelcol-nats
# 4317 OTLP/gRPC, 4318 OTLP/HTTP (from applications)
EXPOSE 4317 4318
ENTRYPOINT ["/otelcol-nats"]
CMD ["--config", "/etc/otelcol/config.yaml"]
