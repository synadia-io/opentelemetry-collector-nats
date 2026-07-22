// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natscorereceiver // import "github.com/synadia-labs/opentelemetry-collector-nats/receiver/natscorereceiver"

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Supported built-in payload encodings.
const (
	encodingOtlpProto = "otlp_proto"
	encodingOtlpJSON  = "otlp_json"
)

func logsUnmarshaler(encoding string) (unmarshalFunc[plog.Logs], error) {
	switch encoding {
	case "", encodingOtlpProto:
		u := &plog.ProtoUnmarshaler{}
		return u.UnmarshalLogs, nil
	case encodingOtlpJSON:
		u := &plog.JSONUnmarshaler{}
		return u.UnmarshalLogs, nil
	default:
		return nil, fmt.Errorf("unsupported logs encoding: %q", encoding)
	}
}

func metricsUnmarshaler(encoding string) (unmarshalFunc[pmetric.Metrics], error) {
	switch encoding {
	case "", encodingOtlpProto:
		u := &pmetric.ProtoUnmarshaler{}
		return u.UnmarshalMetrics, nil
	case encodingOtlpJSON:
		u := &pmetric.JSONUnmarshaler{}
		return u.UnmarshalMetrics, nil
	default:
		return nil, fmt.Errorf("unsupported metrics encoding: %q", encoding)
	}
}

func tracesUnmarshaler(encoding string) (unmarshalFunc[ptrace.Traces], error) {
	switch encoding {
	case "", encodingOtlpProto:
		u := &ptrace.ProtoUnmarshaler{}
		return u.UnmarshalTraces, nil
	case encodingOtlpJSON:
		u := &ptrace.JSONUnmarshaler{}
		return u.UnmarshalTraces, nil
	default:
		return nil, fmt.Errorf("unsupported traces encoding: %q", encoding)
	}
}
