// Command matchd is the matching engine server.
//
//	matchd -addr :8080 -data /var/lib/matchd -instruments config/instruments.json
//
// Every flag can also be set with an environment variable (MATCHD_ADDR, MATCHD_DATA, ...).
// On SIGTERM it stops accepting requests, ends the market data streams, finishes the
// requests already queued and closes the journals.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	rtmetrics "runtime/metrics"
	"syscall"
	"time"

	"github.com/davifmn/matching-engine/internal/api"
	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/journal"
	"github.com/davifmn/matching-engine/internal/metrics"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr, nil); err != nil {
		fmt.Fprintln(os.Stderr, "matchd:", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// run starts the server and blocks until ctx is done. onListen, if set, gets the address
// actually bound (useful with port 0).
func run(ctx context.Context, args []string, logOut io.Writer, onListen func(addr string)) error {
	fs := flag.NewFlagSet("matchd", flag.ContinueOnError)
	fs.SetOutput(logOut)
	addr := fs.String("addr", envOr("MATCHD_ADDR", ":8080"), "listen address")
	dataDir := fs.String("data", envOr("MATCHD_DATA", "data"), "journal directory; empty keeps the books in memory only")
	instPath := fs.String("instruments", envOr("MATCHD_INSTRUMENTS", "config/instruments.json"), "instrument definitions (JSON)")
	syncMode := fs.String("sync", envOr("MATCHD_SYNC", "commit"), `"commit": fsync once per group commit; "none": leave flushing to the OS`)
	logLevel := fs.String("log-level", envOr("MATCHD_LOG_LEVEL", "info"), "debug, info, warn or error")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintln(logOut, "matchd", version, runtime.Version())
		return nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: level}))

	var mode journal.SyncMode
	switch *syncMode {
	case "commit":
		mode = journal.SyncEveryCommit
	case "none":
		mode = journal.SyncNever
	default:
		return fmt.Errorf(`-sync must be "commit" or "none", not %q`, *syncMode)
	}
	specs, err := instrument.Load(*instPath)
	if err != nil {
		return err
	}
	symbols := make([]string, len(specs))
	for i, s := range specs {
		symbols[i] = s.Symbol
	}
	if *dataDir != "" {
		if err := os.MkdirAll(*dataDir, 0o750); err != nil {
			return err
		}
	}

	reg := metrics.NewRegistry()
	registerRuntimeMetrics(reg)
	eng, err := engine.New(engine.Config{Dir: *dataDir, Sync: mode, Metrics: reg, Logger: log}, symbols...)
	if err != nil {
		return err
	}
	a := api.New(eng, specs, log, reg)
	srv := &http.Server{
		Handler:           a.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second, // streams lift it for themselves
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	srv.RegisterOnShutdown(a.CloseStreams)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		eng.Close()
		return err
	}
	log.Info("matchd started", "version", version, "addr", ln.Addr().String(), "instruments", symbols,
		"data", *dataDir, "sync", *syncMode)
	if onListen != nil {
		onListen(ln.Addr().String())
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	select {
	case err := <-serveErr:
		eng.Close()
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "error", err)
	}
	if err := eng.Close(); err != nil {
		return fmt.Errorf("closing journals: %w", err)
	}
	log.Info("stopped")
	return nil
}

func registerRuntimeMetrics(reg *metrics.Registry) {
	start := float64(time.Now().Unix())
	reg.GaugeFunc("process_start_time_seconds", "Start time of the process, in unix seconds.", func() float64 { return start })
	reg.GaugeFunc("go_goroutines", "Number of goroutines.", func() float64 { return float64(runtime.NumGoroutine()) })
	sample := func(name string) func() float64 {
		return func() float64 {
			s := []rtmetrics.Sample{{Name: name}}
			rtmetrics.Read(s)
			if s[0].Value.Kind() == rtmetrics.KindUint64 {
				return float64(s[0].Value.Uint64())
			}
			return 0
		}
	}
	reg.GaugeFunc("go_heap_objects_bytes", "Heap memory held by objects, live or not yet swept.", sample("/memory/classes/heap/objects:bytes"))
	reg.GaugeFunc("go_gc_cycles", "Completed GC cycles since start.", sample("/gc/cycles/total:gc-cycles"))
}
