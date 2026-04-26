package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/mahavrilla/sf-lazarus/internal/athena"
	"github.com/mahavrilla/sf-lazarus/internal/restore"
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
	"github.com/mahavrilla/sf-lazarus/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ── flags ────────────────────────────────────────────────────────────────
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)

	addr := fs.String("addr", env("RESTORE_ADDR", ":8080"),
		"HTTP listen address [$RESTORE_ADDR]")
	logLevel := fs.String("log-level", "info",
		"log verbosity: debug | info | warn | error")

	athenaDatabase := fs.String("athena-database", env("ATHENA_DATABASE", ""),
		"Athena/Glue database for restore queries [$ATHENA_DATABASE]")
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
	if err := requireConfig("--athena-database / $ATHENA_DATABASE", *athenaDatabase); err != nil {
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

	// ── Salesforce client ─────────────────────────────────────────────────────
	sfClient, err := salesforce.NewClient(ctx, salesforce.Config{
		LoginURL:     os.Getenv("SF_LOGIN_URL"),
		ClientID:     os.Getenv("SF_CLIENT_ID"),
		ClientSecret: os.Getenv("SF_CLIENT_SECRET"),
	})
	if err != nil {
		return fmt.Errorf("init salesforce client: %w", err)
	}

	// ── Athena client + restore service ───────────────────────────────────────
	athenaClient := athena.NewClient(athena.Config{
		Database:       *athenaDatabase,
		Workgroup:      *athenaWorkgroup,
		OutputLocation: *athenaOutput,
		AWSConfig:      awsCfg,
	})

	restoreSvc := restore.NewService(athenaClient, log)

	// ── HTTP server ───────────────────────────────────────────────────────────
	h := server.NewHandler(restoreSvc, sfClient, log)
	mux := server.NewMux(h, log)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Shutdown on signal.
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Error("graceful shutdown failed", slog.Any("error", err))
		}
	}()

	log.Info("restore server listening", slog.String("addr", *addr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server: %w", err)
	}
	log.Info("restore server stopped")
	return nil
}

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
