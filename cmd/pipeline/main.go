// Command pipeline runs the Stage 1 happy-path loop: stream decoded changes
// from the source and apply them to the downstream.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/extrem21/watermark/pipeline"
	"github.com/extrem21/watermark/shared"
)

func main() {
	var cfg shared.Config
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer, err := pipeline.NewConsumer(ctx, &cfg)
	if err != nil {
		log.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	applier, err := pipeline.NewApplier(ctx, &cfg)
	if err != nil {
		log.Fatalf("new applier: %v", err)
	}
	defer applier.Close()

	events := make(chan shared.ChangeEvent, 100)
	go func() {
		for ev := range events {
			if err := applier.Apply(ctx, ev); err != nil {
				log.Printf("apply failed table=%s key=%s lsn=%d: %v", ev.Table, ev.Key, ev.LSN, err)
				continue
			}
			log.Printf("applied op=%d table=%s key=%s lsn=%d", ev.Operation, ev.Table, ev.Key, ev.LSN)
		}
	}()

	if err := consumer.Start(ctx, events); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer stopped: %v", err)
	}
}
