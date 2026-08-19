// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package grouper // import "github.com/synadia-io/opentelemetry-collector-nats/exporter/natsexporter/internal/grouper"

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
