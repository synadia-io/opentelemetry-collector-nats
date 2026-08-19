// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package e2e holds cross-component end-to-end tests: the nats exporter
// publishes signals to NATS JetStream and the nats receiver consumes them
// back, asserting the payloads survive the full round trip. This is the only
// test that exercises the two components' wire contract together.
package e2e
