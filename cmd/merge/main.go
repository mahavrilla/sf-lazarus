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

	"github.com/mahavrilla/sf-lazarus/internal/athena"
	"github.com/mahavrilla/sf-lazarus/internal/iceberg"
	"github.com/mahavrilla/sf-lazarus/internal/schema"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ── flags ────────────────────────────────────────────────────────────────
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)

	registryPath := fs.String("registry", "config/objects.yaml",
		"path to objects.yaml manifest")
	objectName := fs.String("object", "",
		"merge a single named object (useful for debugging)")
	logLevel := fs.String("log-level", "info",
		"log verbosity: debug | info | warn | error")

	glueDatabase := fs.String("glue-database", env("GLUE_DATABASE", ""),
		"Glue database for Iceberg tables [$GLUE_DATABASE]")
	s3Bucket := fs.String("s3-bucket", env("S3_BUCKET", ""),
		"S3 bucket for Iceberg data [$S3_BUCKET]")
	s3KeyPrefix := fs.String("s3-prefix", env("S3_KEY_PREFIX", "iceberg/"),
		"S3 key prefix for all tables [$S3_KEY_PREFIX]")
	athenaWorkgroup := fs.String("athena-workgroup", env("ATHENA_WORKGROUP", "primary"),
		"Athena workgroup [$ATHENA_WORKGROUP]")
	athenaOutput := fs.String("athena-output", env("ATHENA_OUTPUT_LOCATION", ""),
		"S3 URI for Athena result output [$ATHENA_OUTPUT_LOCATION]")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	// ── logging ───────────────────────────────────────────────────────────────
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(*logLevel),
	}))

	// ── validate required config ──────────────────────────────────────────────
	if err := requireConfig("--glue-database / $GLUE_DATABASE", *glueDatabase); err != nil {
		return err
	}
	if err := requireConfig("--s3-bucket / $S3_BUCKET", *s3Bucket); err != nil {
		return err
	}
	if err := requireConfig("--athena-output / $ATHENA_OUTPUT_LOCATION", *athenaOutput); err != nil {
		return err
	}

	// ── context with graceful shutdown ────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── AWS config ────────────────────────────────────────────────────────────
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// ── registry ──────────────────────────────────────────────────────────────
	reg, err := iceberg.LoadRegistry(*registryPath)
	if err != nil {
		return fmt.Errorf("load registry %q: %w", *registryPath, err)
	}
	if *objectName != "" {
		obj, ok := reg.Get(*objectName)
		if !ok {
			return fmt.Errorf("object %q not found in registry", *objectName)
		}
		reg = iceberg.NewRegistry([]iceberg.ObjectConfig{obj})
		log.Info("single-object mode", slog.String("object", *objectName))
	}

	// ── dependencies ──────────────────────────────────────────────────────────
	glueWriter := iceberg.NewGlueWriter(iceberg.GlueWriterConfig{
		Database:  *glueDatabase,
		Bucket:    *s3Bucket,
		KeyPrefix: *s3KeyPrefix,
		AWSConfig: awsCfg,
	}, &schema.JSONCodec{}, log)

	athenaClient := athena.NewClient(athena.Config{
		Database:       *glueDatabase,
		Workgroup:      *athenaWorkgroup,
		OutputLocation: *athenaOutput,
		AWSConfig:      awsCfg,
	})

	// ── merge all objects ─────────────────────────────────────────────────────
	var firstErr error
	for _, obj := range reg.All() {
		if err := mergeOne(ctx, obj, glueWriter, athenaClient, log); err != nil {
			log.Error("merge failed",
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

// mergeOne ensures the current-state table exists and merges latest records into it.
func mergeOne(
	ctx context.Context,
	obj iceberg.ObjectConfig,
	glue iceberg.Writer,
	ac *athena.Client,
	log *slog.Logger,
) error {
	log = log.With(slog.String("object", obj.Name))

	cols, err := glue.GetSchema(ctx, obj.Name)
	if err != nil {
		return fmt.Errorf("get schema for %s: %w", obj.Name, err)
	}

	currentName := obj.Name + "_current"
	currentObj := iceberg.ObjectConfig{
		Name:               currentName,
		PrimaryKey:         obj.PrimaryKey,
		WatermarkField:     obj.WatermarkField,
		RemovedFieldPolicy: obj.RemovedFieldPolicy,
		// No partition_by — current table is keyed by id, no time partitioning needed.
	}
	if err := glue.EnsureTable(ctx, currentObj, cols, false); err != nil {
		return fmt.Errorf("ensure current table for %s: %w", obj.Name, err)
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	if err := ac.MergeCurrentState(ctx, obj.Name, currentName, colNames); err != nil {
		return fmt.Errorf("merge current state for %s: %w", obj.Name, err)
	}

	log.Info("merge complete",
		slog.String("current_table", currentName),
		slog.Int("columns", len(cols)),
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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireConfig(name, value string) error {
	if value == "" {
		return fmt.Errorf("required configuration missing: %s", name)
	}
	return nil
}
