// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package natsclient

import (
	"context"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	conn, err := Connect(context.Background(), Params{Endpoint: srv.ClientURL()})
	require.NoError(t, err)
	defer conn.Close()

	sub, err := conn.SubscribeSync("subject")
	require.NoError(t, err)
	require.NoError(t, conn.Publish("subject", []byte("payload")))
	require.NoError(t, conn.Flush())

	msg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(msg.Data))
}

func TestConnect_BadEndpoint(t *testing.T) {
	t.Parallel()

	_, err := Connect(context.Background(), Params{Endpoint: "nats://127.0.0.1:1"})
	assert.Error(t, err)
}

func TestAuthConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		auth    AuthConfig
		wantErr string
	}{
		{
			name: "complete token",
			auth: AuthConfig{Token: &TokenConfig{Token: "token"}},
		},
		{
			name:    "incomplete user",
			auth:    AuthConfig{User: &UserConfig{Username: "user"}},
			wantErr: "incomplete username/password auth configuration",
		},
		{
			name: "nkey configured more than once",
			auth: AuthConfig{
				Nkey:    &NkeyConfig{PublicKey: "pk", Seed: []byte("seed")},
				NkeyJWT: &NkeyJWTConfig{JWT: "jwt", Seed: []byte("seed")},
			},
			wantErr: "NKey auth configured more than once",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.auth.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
