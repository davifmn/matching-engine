// Command dropcopy follows the journals of matchd and publishes every trade, with the
// accounts of both sides, to a Kafka topic for settlement (docs/adr/0005-drop-copy.md).
//
// It needs read access to matchd's journal directory: run it as a sidecar sharing the
// volume, or on the same host. It is a separate Go module so that the engine itself keeps
// no third-party dependencies.
package main

import (
	"context"
	"encoding/json"
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/davifmn/matching-engine/internal/dropcopy"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "dropcopy:", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func run(ctx context.Context, args []string, logOut io.Writer) error {
	fs := flag.NewFlagSet("dropcopy", flag.ContinueOnError)
	fs.SetOutput(logOut)
	dataDir := fs.String("data", envOr("DROPCOPY_DATA", "/var/lib/matchd"), "matchd's journal directory (read only)")
	stateDir := fs.String("state", envOr("DROPCOPY_STATE", "/var/lib/dropcopy"), "where the cursors are kept")
	instPath := fs.String("instruments", envOr("DROPCOPY_INSTRUMENTS", "/etc/matchd/instruments.json"), "instrument definitions, as given to matchd")
	symbolList := fs.String("symbols", envOr("DROPCOPY_SYMBOLS", ""), "instruments to follow, comma separated; empty = every instrument with a currency")
	brokers := fs.String("brokers", envOr("DROPCOPY_BROKERS", "localhost:9092"), "Kafka bootstrap servers, comma separated")
	topic := fs.String("topic", envOr("DROPCOPY_TOPIC", "trades.executed"), "topic for the trades (keyed by symbol)")
	addr := fs.String("addr", envOr("DROPCOPY_ADDR", ":9100"), "address for /metrics and /healthz")
	poll := fs.Duration("poll", 50*time.Millisecond, "how often to look for new journal records when idle")
	logLevel := fs.String("log-level", envOr("DROPCOPY_LOG_LEVEL", "info"), "debug, info, warn or error")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintln(logOut, "dropcopy", version, runtime.Version())
		return nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: level}))

	specs, err := chooseInstruments(*instPath, *symbolList)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*stateDir, 0o750); err != nil {
		return err
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotent writes are on by default
		kgo.ProducerLinger(2*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	reg := metrics.NewRegistry()
	m := dropcopy.NewMetrics(reg)
	var healthy sync.Map // symbol → true while its follower runs
	srv, err := serve(*addr, reg, specs, &healthy, log)
	if err != nil {
		return err
	}
	defer srv.Shutdown(context.Background())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(specs))
	var wg sync.WaitGroup
	for _, spec := range specs {
		f, err := dropcopy.NewFollower(dropcopy.Config{
			Spec: spec, Dir: *dataDir, StateDir: *stateDir, Publisher: kafkaPublisher{client},
			Poll: *poll, Metrics: m, Logger: log,
		})
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			healthy.Store(spec.Symbol, true)
			err := f.Run(ctx)
			healthy.Store(spec.Symbol, false)
			if err != nil {
				errs <- err
				cancel() // one instrument failing stops all: the orchestrator restarts the process
			}
		}()
	}
	log.Info("dropcopy started", "version", version, "instruments", len(specs), "topic", *topic)
	wg.Wait()
	close(errs)
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	return errors.Join(all...)
}

func chooseInstruments(path, list string) ([]instrument.Spec, error) {
	specs, err := instrument.Load(path)
	if err != nil {
		return nil, err
	}
	bySymbol := map[string]instrument.Spec{}
	for _, s := range specs {
		bySymbol[s.Symbol] = s
	}
	var chosen []instrument.Spec
	if list == "" {
		for _, s := range specs {
			if s.Currency != "" {
				chosen = append(chosen, s)
			}
		}
	} else {
		for _, sym := range strings.Split(list, ",") {
			s, ok := bySymbol[strings.TrimSpace(sym)]
			if !ok {
				return nil, fmt.Errorf("-symbols: %s is not in %s", sym, path)
			}
			chosen = append(chosen, s)
		}
	}
	if len(chosen) == 0 {
		return nil, errors.New("no instrument to follow: give -symbols or a currency in the instrument definitions")
	}
	return chosen, nil
}

// kafkaPublisher sends a batch keyed by symbol, so the trades of an instrument stay in
// order on one partition, and waits until every record is acknowledged by all in-sync
// replicas.
type kafkaPublisher struct{ client *kgo.Client }

func (p kafkaPublisher) Publish(ctx context.Context, trades []dropcopy.Trade) error {
	records := make([]*kgo.Record, len(trades))
	for i, t := range trades {
		value, err := json.Marshal(t)
		if err != nil {
			return err
		}
		records[i] = &kgo.Record{Key: []byte(t.Symbol), Value: value}
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

func serve(addr string, reg *metrics.Registry, specs []instrument.Spec, healthy *sync.Map, log *slog.Logger) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", reg.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		for _, s := range specs {
			v, _ := healthy.Load(s.Symbol)
			if running, _ := v.(bool); !running {
				http.Error(w, s.Symbol+" is not running", http.StatusServiceUnavailable)
				return
			}
		}
		fmt.Fprintln(w, "ok")
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	}()
	return srv, nil
}
