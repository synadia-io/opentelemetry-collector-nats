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
# Build a fully static binary (no libc / dynamic linker) so it runs in the
# distroless/static base below. Without this the golang image defaults to
# CGO_ENABLED=1 and the resulting binary fails at exec with
# "no such file or directory" (missing ELF interpreter) in a static image.
ENV CGO_ENABLED=0
WORKDIR /src
COPY . .
# ocb version must match the collector component versions in builder-config.yaml.
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.133.0
WORKDIR /src/deploy
RUN builder --config builder-config.yaml
# Fail early (in build, not at runtime) if the binary isn't static.
RUN ! ldd _build/otelcol-nats 2>/dev/null | grep -q '=>' || (echo "binary is dynamically linked" && exit 1)

# ---- runtime stage: distroless ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/deploy/_build/otelcol-nats /otelcol-nats
# 4317 OTLP/gRPC, 4318 OTLP/HTTP (from applications)
EXPOSE 4317 4318
ENTRYPOINT ["/otelcol-nats"]
CMD ["--config", "/etc/otelcol/config.yaml"]
