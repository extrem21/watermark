package shared

import "flag"

// Config is the CLI surface for every binary in this project. Stage 1 needs
// the connection + slot fields, plus Seed and WorkloadOps for the workload
// generator (harness/workload_gen.go) — reproducibility depends on the seed
// being explicit from the start, not stubbed out until a later stage.
// Later stages add fields here (fault rates, worker count, retention bound)
// as those stages need them.
type Config struct {
	SourceDSN       string
	DownstreamDSN   string
	SlotName        string
	PublicationName string

	Seed        int64
	WorkloadOps int
}

// RegisterFlags binds Config fields to flags on fs, so multiple binaries
// (consumer, workload generator, diff oracle) can share the same surface.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.SourceDSN, "source-dsn",
		"postgres://watermark:watermark@localhost:5433/watermark_source",
		"connection string for the source Postgres")
	fs.StringVar(&c.DownstreamDSN, "downstream-dsn",
		"postgres://watermark:watermark@localhost:5434/watermark_downstream",
		"connection string for the downstream Postgres")
	fs.StringVar(&c.SlotName, "slot-name", "watermark_slot",
		"name of the logical replication slot on the source")
	fs.StringVar(&c.PublicationName, "publication-name", "watermark_pub",
		"name of the publication on the source")
	fs.Int64Var(&c.Seed, "seed", 1,
		"seed for all randomness (workload generator, later fault injection); never time.Now()")
	fs.IntVar(&c.WorkloadOps, "workload-ops", 200,
		"number of INSERT/UPDATE/DELETE operations the workload generator issues")
}
