// Command streamsetup creates (or updates) the JetStream stream the demo needs.
// The exporter and receiver deliberately do not create streams, so the demo does
// it here.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatalf("connect to %s: %v", url, err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "OTEL_SPANS",
		Subjects: []string{"otel_spans"},
	}); err != nil {
		log.Fatalf("create stream: %v", err)
	}

	log.Println("stream OTEL_SPANS ready (subject: otel_spans)")
}
