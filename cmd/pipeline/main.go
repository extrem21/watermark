// Command pipeline runs the Stage 1 happy-path loop: stream decoded changes
// from the source and apply them to the downstream.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/extrem21/watermark/pipeline"
	"github.com/extrem21/watermark/shared"
)

// metricsInterval controls how often accumulated throughput is reported.
// Per-change end-to-end lag is logged on every apply, not just at this
// interval — lag is a per-event measurement, throughput is a windowed one.
const metricsInterval = 5 * time.Second

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
		applied := 0
		windowStart := time.Now()

		for ev := range events {
			if err := applier.Apply(ctx, ev); err != nil {
				log.Printf("apply failed table=%s key=%s lsn=%d: %v", ev.Table, ev.Key, ev.LSN, err)
				continue
			}

			lag := time.Since(ev.CommitTime)
			log.Printf("applied op=%d table=%s key=%s lsn=%d lag=%s", ev.Operation, ev.Table, ev.Key, ev.LSN, lag)
			applied++

			if elapsed := time.Since(windowStart); elapsed >= metricsInterval {
				throughput := float64(applied) / elapsed.Seconds()
				log.Printf("METRICS throughput=%.2f changes/sec window=%s applied=%d last_lag=%s",
					throughput, elapsed.Round(time.Millisecond), applied, lag)
				applied = 0
				windowStart = time.Now()
			}
		}
	}()

	if err := consumer.Start(ctx, events); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer stopped: %v", err)
	}
}
