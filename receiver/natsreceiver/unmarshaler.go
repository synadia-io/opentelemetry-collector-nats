// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsreceiver // import "github.com/synadia-io/opentelemetry-collector-nats/receiver/natsreceiver"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Supported built-in payload encodings.
const (
	encodingOtlpProto = "otlp_proto"
	encodingOtlpJSON  = "otlp_json"
)

// resolveExtension looks up an encoding extension by component ID from the host.
func resolveExtension(host component.Host, extensionName string) (component.Component, error) {
	var id component.ID
	if err := id.UnmarshalText([]byte(extensionName)); err != nil {
		return nil, fmt.Errorf("failed to parse encoding extension name %q: %w", extensionName, err)
	}
	ext, ok := host.GetExtensions()[id]
	if !ok {
		return nil, fmt.Errorf("encoding extension not found: %s", id)
	}
	return ext, nil
}

// logsUnmarshalerResolver returns a resolver that, given the host, produces the
// unmarshaler for logs: a configured encoding extension if set, otherwise a
// built-in encoding.
func logsUnmarshalerResolver(sc *SignalConfig) unmarshalerResolver[plog.Logs] {
	return func(host component.Host) (unmarshalFunc[plog.Logs], error) {
		if sc.EncodingExtension != "" {
			ext, err := resolveExtension(host, sc.EncodingExtension)
			if err != nil {
				return nil, err
			}
			unmarshaler, ok := ext.(plog.Unmarshaler)
			if !ok {
				return nil, fmt.Errorf("extension %q does not implement a logs unmarshaler", sc.EncodingExtension)
			}
			return unmarshaler.UnmarshalLogs, nil
		}

		switch sc.Encoding {
		case "", encodingOtlpProto:
			u := &plog.ProtoUnmarshaler{}
			return u.UnmarshalLogs, nil
		case encodingOtlpJSON:
			u := &plog.JSONUnmarshaler{}
			return u.UnmarshalLogs, nil
		default:
			return nil, fmt.Errorf("unsupported logs encoding: %q", sc.Encoding)
		}
	}
}

func metricsUnmarshalerResolver(sc *SignalConfig) unmarshalerResolver[pmetric.Metrics] {
	return func(host component.Host) (unmarshalFunc[pmetric.Metrics], error) {
		if sc.EncodingExtension != "" {
			ext, err := resolveExtension(host, sc.EncodingExtension)
			if err != nil {
				return nil, err
			}
			unmarshaler, ok := ext.(pmetric.Unmarshaler)
			if !ok {
				return nil, fmt.Errorf("extension %q does not implement a metrics unmarshaler", sc.EncodingExtension)
			}
			return unmarshaler.UnmarshalMetrics, nil
		}

		switch sc.Encoding {
		case "", encodingOtlpProto:
			u := &pmetric.ProtoUnmarshaler{}
			return u.UnmarshalMetrics, nil
		case encodingOtlpJSON:
			u := &pmetric.JSONUnmarshaler{}
			return u.UnmarshalMetrics, nil
		default:
			return nil, fmt.Errorf("unsupported metrics encoding: %q", sc.Encoding)
		}
	}
}

func tracesUnmarshalerResolver(sc *SignalConfig) unmarshalerResolver[ptrace.Traces] {
	return func(host component.Host) (unmarshalFunc[ptrace.Traces], error) {
		if sc.EncodingExtension != "" {
			ext, err := resolveExtension(host, sc.EncodingExtension)
			if err != nil {
				return nil, err
			}
			unmarshaler, ok := ext.(ptrace.Unmarshaler)
			if !ok {
				return nil, fmt.Errorf("extension %q does not implement a traces unmarshaler", sc.EncodingExtension)
			}
			return unmarshaler.UnmarshalTraces, nil
		}

		switch sc.Encoding {
		case "", encodingOtlpProto:
			u := &ptrace.ProtoUnmarshaler{}
			return u.UnmarshalTraces, nil
		case encodingOtlpJSON:
			u := &ptrace.JSONUnmarshaler{}
			return u.UnmarshalTraces, nil
		default:
			return nil, fmt.Errorf("unsupported traces encoding: %q", sc.Encoding)
		}
	}
}
