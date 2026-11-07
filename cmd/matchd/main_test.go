package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a log sink the server goroutine and the test can share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// start runs the server on a free port and returns its URL and a stop function that
// shuts it down like SIGTERM would.
func start(t *testing.T, args ...string) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrc := make(chan string, 1)
	done := make(chan error, 1)
	logs := &syncBuffer{}
	go func() {
		done <- run(ctx, append([]string{"-addr", "127.0.0.1:0"}, args...), logs, func(a string) { addrc <- a })
	}()
	select {
	case a := <-addrc:
		return "http://" + a, func() {
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("run: %v\n%s", err, logs)
			}
		}
	case err := <-done:
		cancel()
		t.Fatalf("server did not start: %v\n%s", err, logs)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	return "", nil
}

func writeInstruments(t *testing.T, dir string) string {
	p := filepath.Join(dir, "instruments.json")
	body := `[{"symbol":"PETR4","tick_size":"0.01","lot_size":"100"}]`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func post(t *testing.T, url, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// The whole binary path: start, trade, shut down cleanly, start again from the journal.
func TestRestartKeepsTheBook(t *testing.T) {
	dir := t.TempDir()
	inst := writeInstruments(t, dir)
	data := filepath.Join(dir, "data")

	url, stop := start(t, "-instruments", inst, "-data", data, "-sync", "commit")
	orders := url + "/v1/instruments/PETR4/orders"
	post(t, orders, `{"side":"sell","type":"limit","price":"34.20","quantity":"500","account_id":1}`)
	post(t, orders, `{"side":"sell","type":"limit","price":"34.15","quantity":"300","account_id":1}`)
	post(t, orders, `{"side":"buy","type":"limit","price":"34.00","quantity":"1000","account_id":2}`)
	if r := post(t, orders, `{"side":"buy","type":"limit","price":"34.15","quantity":"100","account_id":3}`); r["status"] != "filled" {
		t.Fatalf("aggressive buy: %v", r)
	}
	before := get(t, url+"/v1/instruments/PETR4/book?depth=50")
	stop()

	url, stop = start(t, "-instruments", inst, "-data", data)
	defer stop()
	if after := get(t, url+"/v1/instruments/PETR4/book?depth=50"); after != before {
		t.Fatalf("book changed across restart\nbefore: %s\n after: %s", before, after)
	}
	if r := post(t, url+"/v1/instruments/PETR4/orders", `{"side":"buy","type":"market","quantity":"200"}`); r["order_id"] != "5" {
		t.Fatalf("order IDs did not continue: %v", r)
	}
	if m := get(t, url+"/metrics"); !strings.Contains(m, "go_goroutines") || !strings.Contains(m, "matching_journal_bytes") {
		t.Error("metrics endpoint incomplete")
	}
}

func TestBadFlags(t *testing.T) {
	dir := t.TempDir()
	inst := writeInstruments(t, dir)
	for _, args := range [][]string{
		{"-sync", "sometimes", "-instruments", inst},
		{"-log-level", "loud", "-instruments", inst},
		{"-instruments", filepath.Join(dir, "missing.json")},
		{"-no-such-flag"},
	} {
		if err := run(context.Background(), args, io.Discard, nil); err == nil {
			t.Errorf("run(%v) succeeded", args)
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &out, nil); err != nil || !strings.HasPrefix(out.String(), "matchd dev") {
		t.Errorf("-version: %q, %v", out.String(), err)
	}
}
