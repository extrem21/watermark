// Command diffcheck runs the diff oracle once against the currently running
// source and downstream databases and asserts exact row-by-row equality.
// Exit code 0 means equal; exit code 1 means diverged, with every mismatch
// printed by id.
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/extrem21/watermark/harness"
	"github.com/extrem21/watermark/shared"
)

func main() {
	var cfg shared.Config
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	ctx := context.Background()

	result, err := harness.RunDiffOracle(ctx, &cfg)
	if err != nil {
		log.Fatalf("run diff oracle: %v", err)
	}

	if result.Equal() {
		log.Println("PASS: source and downstream are row-for-row identical")
		return
	}

	log.Printf("FAIL: %d mismatch(es)", len(result.Mismatches))
	for _, m := range result.Mismatches {
		log.Println(" -", m)
	}
	os.Exit(1)
}
