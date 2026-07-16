// Command pipeline runs the Stage 1 happy-path loop: stream decoded changes
// from the source and apply them to the downstream.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/extrem21/watermark/pipeline"
	"github.com/extrem21/watermark/shared"
)

// burstIdleGap is how long the apply loop can go without a new event before
// the run of events it just processed is considered a closed "burst" and its
// throughput is reported. Reporting per burst — not per fixed wall-clock
// interval — means the number reflects only time spent actually applying:
// however long a human took to start the workload generator, or any gap
// between bursts, never dilutes it. Per-change end-to-end lag is logged on
// every apply regardless — lag is a per-event measurement, throughput is a
// per-burst one.
const burstIdleGap = 500 * time.Millisecond

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
		var burstCount int
		var burstStart, lastEvent time.Time
		var lags []time.Duration

		// idleTimer fires once burstIdleGap has passed with no new event,
		// closing out the current burst. It starts stopped: there's no
		// burst in flight yet, so nothing should fire until the first event
		// arrives and (re)arms it.
		idleTimer := time.NewTimer(burstIdleGap)
		if !idleTimer.Stop() {
			<-idleTimer.C
		}

		flush := func() {
			if burstCount == 0 {
				return
			}
			elapsed := lastEvent.Sub(burstStart)
			p50, p90, p99 := latencyPercentiles(lags)
			if elapsed <= 0 {
				// A single-event burst has no interior span to divide by;
				// reporting a rate here would be a fabricated number. The
				// lag percentiles are still meaningful — they don't need an
				// interior span, just samples.
				log.Printf("METRICS applied=%d (single event, no rate) lag_p50=%s lag_p90=%s lag_p99=%s",
					burstCount, p50.Round(time.Millisecond), p90.Round(time.Millisecond), p99.Round(time.Millisecond))
			} else {
				log.Printf("METRICS throughput=%.2f changes/sec burst=%s applied=%d lag_p50=%s lag_p90=%s lag_p99=%s",
					float64(burstCount)/elapsed.Seconds(), elapsed.Round(time.Millisecond), burstCount,
					p50.Round(time.Millisecond), p90.Round(time.Millisecond), p99.Round(time.Millisecond))
			}
			burstCount = 0
			lags = lags[:0]
		}

		for {
			select {
			case ev, ok := <-events:
				if !ok {
					flush()
					return
				}
				if err := applier.Apply(ctx, ev); err != nil {
					log.Printf("apply failed table=%s key=%s lsn=%d: %v", ev.Table, ev.Key, ev.LSN, err)
					continue
				}

				lag := time.Since(ev.CommitTime)
				log.Printf("applied op=%d table=%s key=%s lsn=%d lag=%s", ev.Operation, ev.Table, ev.Key, ev.LSN, lag)

				now := time.Now()
				if burstCount == 0 {
					burstStart = now
				}
				burstCount++
				lastEvent = now
				lags = append(lags, lag)

				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(burstIdleGap)

			case <-idleTimer.C:
				flush()

			case <-ctx.Done():
				flush()
				return
			}
		}
	}()

	if err := consumer.Start(ctx, events); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer stopped: %v", err)
	}
}

// latencyPercentiles returns the p50/p90/p99 of lags using nearest-rank
// selection on a sorted copy. Percentiles on a handful of samples (a burst
// of 1-2 events) are not meaningful — they'd just be reporting the max as
// "p99" — but that's an honest reflection of a small burst, not a bug: the
// same caveat applies to any percentile stat computed on a small N.
func latencyPercentiles(lags []time.Duration) (p50, p90, p99 time.Duration) {
	if len(lags) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), lags...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	return rank(0.50), rank(0.90), rank(0.99)
}
