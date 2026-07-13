// Command consumer is a manual smoke-test harness for pipeline.Consumer: it
// streams decoded changes from the source and prints them. It does not apply
// anything downstream — that's apply.go, wired in separately.
package main

import (
	"context"
	"flag"
	"fmt"
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

	events := make(chan shared.ChangeEvent, 100)
	go func() {
		for ev := range events {
			fmt.Printf("%+v\n", ev)
		}
	}()

	if err := consumer.Start(ctx, events); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer stopped: %v", err)
	}
}
