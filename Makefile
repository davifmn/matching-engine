GO      ?= go
BIN     := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test race fuzz bench lint vuln e2e run docker

build: ## matchd and loadgen into bin/
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

fuzz: ## compare the book with the reference model; FUZZTIME=10m for longer
	$(GO) test -run='^$$' -fuzz=FuzzAgainstReference -fuzztime=$(or $(FUZZTIME),60s) ./internal/book

bench: ## COUNT=10 for steadier numbers; compare runs with benchstat
	$(GO) test -run='^$$' -bench=. -benchmem -count=$(or $(COUNT),5) ./internal/... | tee bench.txt

lint:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck@latest ./...

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

e2e: build
	BIN=$(BIN) ./scripts/e2e.sh

run: build
	$(BIN)/matchd -data data -instruments config/instruments.json

docker:
	docker build --build-arg VERSION=$(VERSION) -t matching-engine:$(VERSION) .
