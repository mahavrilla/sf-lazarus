package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/mahavrilla/sf-lazarus/internal/backfill"
	"github.com/mahavrilla/sf-lazarus/internal/iceberg"
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
	"github.com/mahavrilla/sf-lazarus/internal/schema"
	"github.com/mahavrilla/sf-lazarus/internal/syncstate"
)

func main() {
	if err := run(); err != nil {
		// run() logs structured errors before returning; this is the fallback.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ── flags ────────────────────────────────────────────────────────────────
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)

	registryPath := fs.String("registry", "config/objects.yaml",
		"path to objects.yaml manifest")
	objectName := fs.String("object", "",
		"run a single named object (useful for debugging)")
	dryRun := fs.Bool("dry-run", false,
		"describe SF schema and report drift; no data is written")
	batchSize := fs.Int("batch-size", 0,
		"records per WriteBatch call (default 5000)")
	logLevel := fs.String("log-level", "info",
		"log verbosity: debug | info | warn | error")

	// Non-secret infra config can come from flags or env vars.
	glueDatabase := fs.String("glue-database", env("GLUE_DATABASE", ""),
		"Glue database (namespace) for Iceberg tables [$GLUE_DATABASE]")
	s3Bucket := fs.String("s3-bucket", env("S3_BUCKET", ""),
		"S3 bucket for Iceberg data and metadata [$S3_BUCKET]")
	s3KeyPrefix := fs.String("s3-prefix", env("S3_KEY_PREFIX", "iceberg/"),
		"S3 key prefix for all tables [$S3_KEY_PREFIX]")
	dynamoTable := fs.String("dynamo-table", env("DYNAMO_TABLE", "lazarus-watermarks"),
		"DynamoDB table for watermark state [$DYNAMO_TABLE]")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	// ── logging ───────────────────────────────────────────────────────────────
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(*logLevel),
	}))

	// ── context with graceful shutdown ────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── registry ──────────────────────────────────────────────────────────────
	reg, err := iceberg.LoadRegistry(*registryPath)
	if err != nil {
		return fmt.Errorf("load registry %q: %w", *registryPath, err)
	}

	if *objectName != "" {
		obj, ok := reg.Get(*objectName)
		if !ok {
			return fmt.Errorf("object %q not found in registry %s", *objectName, *registryPath)
		}
		reg = iceberg.NewRegistry([]iceberg.ObjectConfig{obj})
		log.Info("single-object mode", slog.String("object", *objectName))
	}

	// ── Salesforce client ─────────────────────────────────────────────────────
	sfClient, err := salesforce.NewClient(ctx, salesforce.Config{
		LoginURL:     os.Getenv("SF_LOGIN_URL"),
		ClientID:     os.Getenv("SF_CLIENT_ID"),
		ClientSecret: os.Getenv("SF_CLIENT_SECRET"),
	})
	if err != nil {
		return fmt.Errorf("init salesforce client: %w", err)
	}

	// ── AWS config ────────────────────────────────────────────────────────────
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// ── codec (shared by GlueWriter decoder and Runner encoder) ───────────────
	codec := &schema.JSONCodec{}

	// ── Iceberg / Glue writer ─────────────────────────────────────────────────
	if !*dryRun {
		if err := requireConfig("--glue-database / $GLUE_DATABASE", *glueDatabase); err != nil {
			return err
		}
		if err := requireConfig("--s3-bucket / $S3_BUCKET", *s3Bucket); err != nil {
			return err
		}
	}

	glueWriter := iceberg.NewGlueWriter(iceberg.GlueWriterConfig{
		Database:  *glueDatabase,
		Bucket:    *s3Bucket,
		KeyPrefix: *s3KeyPrefix,
		AWSConfig: awsCfg,
	}, codec, log)

	// ── dry-run path ──────────────────────────────────────────────────────────
	if *dryRun {
		log.Info("dry-run mode: no data will be written")
		return dryRunAll(ctx, reg, sfClient, glueWriter, log)
	}

	// ── full run ──────────────────────────────────────────────────────────────
	store := syncstate.NewDynamoStore(awsCfg, *dynamoTable)

	runner := backfill.NewRunner(
		backfill.Config{BatchSize: *batchSize},
		reg,
		sfClient,
		glueWriter,
		store,
		codec,
		log,
	)

	log.Info("starting backfill")
	if err := runner.RunAll(ctx); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	log.Info("backfill complete")
	return nil
}

// dryRunAll describes each object and reports schema drift without writing.
func dryRunAll(
	ctx context.Context,
	reg *iceberg.Registry,
	sf backfill.SalesforceClient,
	ice iceberg.Writer,
	log *slog.Logger,
) error {
	var firstErr error
	for _, obj := range reg.All() {
		if err := dryRunObject(ctx, obj, sf, ice, log); err != nil {
			log.Error("dry-run failed for object",
				slog.String("object", obj.Name),
				slog.Any("error", err),
			)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// dryRunObject describes one SF object, loads the live Iceberg schema (if the
// table exists), and logs a structured drift report. No writes are performed.
func dryRunObject(
	ctx context.Context,
	obj iceberg.ObjectConfig,
	sf backfill.SalesforceClient,
	ice iceberg.Writer,
	log *slog.Logger,
) error {
	log = log.With(slog.String("object", obj.Name))

	sfFields, err := sf.Describe(ctx, obj.Name)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	desired := iceberg.ColumnsFromDescribe(sfFields, obj.ExcludeFields)

	current, err := ice.GetSchema(ctx, obj.Name)
	if err != nil {
		// Table doesn't exist yet or Glue is not configured.
		log.Info("table not found in catalog — would be created on first run",
			slog.Int("desired_columns", len(desired)),
		)
		return nil
	}

	drift := iceberg.DetectDrift(current, desired)

	log.Info("dry-run result",
		slog.Int("current_columns", len(current)),
		slog.Int("desired_columns", len(desired)),
		slog.Any("new_columns", fieldNames(drift.NewColumns)),
		slog.Any("removed_columns", fieldNames(drift.RemovedColumns)),
		slog.Any("type_changes", drift.TypeChanges),
		slog.Bool("drift_detected", !drift.IsEmpty()),
	)
	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// env returns the value of the environment variable key, or fallback if unset.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// requireConfig returns an error when a required config value is empty.
func requireConfig(name, value string) error {
	if value == "" {
		return fmt.Errorf("required configuration missing: %s", name)
	}
	return nil
}

// fieldNames extracts column names for structured log output.
func fieldNames(cols []iceberg.Column) []string {
	if len(cols) == 0 {
		return nil
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
