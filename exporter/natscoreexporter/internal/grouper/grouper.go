// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package grouper // import "github.com/synadia-labs/opentelemetry-collector-nats/exporter/natscoreexporter/internal/grouper"

import (
	"context"
)

type Group[T any] struct {
	Subject string
	Data    T
}

type Grouper[T any] interface {
	Group(ctx context.Context, data T) ([]Group[T], error)
}
