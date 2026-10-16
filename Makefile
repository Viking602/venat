# venat developer workflow
#
# Common targets:
#   make fmt          - format Go sources (gofmt + goimports if available)
#   make fmt-check    - fail if any Go file needs reformatting
#   make vet          - run `go vet ./...`
#   make lint         - run golangci-lint (must be installed)
#   make tidy         - run `go mod tidy`
#   make tidy-check   - fail if go.mod / go.sum drift
#   make test         - run `go test ./...`
#   make test-race    - run race tests with 10m timeout
#   make build        - build all packages
#   make verify       - fmt-check + vet + tidy-check + lint + test
#   make ci-local     - local CI parity gate including race, static analysis, vuln, and architecture checks
#
# All targets are .PHONY because we don't produce real files.

GO            ?= go
GOFMT         ?= gofmt
GOIMPORTS     ?= goimports
GOLANGCI_LINT ?= golangci-lint
GOPATH_BIN    := $(shell $(GO) env GOPATH)/bin
STATICCHECK   ?= $(GOPATH_BIN)/staticcheck
GOVULNCHECK   ?= $(GOPATH_BIN)/govulncheck
SENTRUX       ?= sentrux

GO_FILES := $(shell find . -type f -name '*.go' -not -path './.git/*')

.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN{FS=":.*##"; printf "Targets:\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## Format Go sources via gofmt (and goimports if installed)
	$(GOFMT) -w $(GO_FILES)
	@if command -v $(GOIMPORTS) >/dev/null 2>&1; then \
		$(GOIMPORTS) -w -local github.com/Viking602/venat $(GO_FILES); \
	else \
		echo "goimports not installed; skipped (install: go install golang.org/x/tools/cmd/goimports@latest)"; \
	fi

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-clean
	@diff=$$($(GOFMT) -l $(GO_FILES)); \
	if [ -n "$$diff" ]; then \
		echo "gofmt issues in:"; echo "$$diff"; exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (install: brew install golangci-lint)
	$(GOLANGCI_LINT) run --timeout=5m

.PHONY: staticcheck
staticcheck: ## Run staticcheck (install: go install honnef.co/go/tools/cmd/staticcheck@latest)
	$(STATICCHECK) ./...

.PHONY: vulncheck
vulncheck: ## Run govulncheck (install: go install golang.org/x/vuln/cmd/govulncheck@latest)
	$(GOVULNCHECK) ./...

.PHONY: architecture-check
architecture-check: ## Run Sentrux and SDK architecture boundary checks
	$(SENTRUX) check .
	./scripts/check-business-words.sh
	./scripts/check-public-any.sh
	./scripts/check-import-boundaries.sh
	./scripts/check-legacy-absence.sh

.PHONY: tidy
tidy: ## Run go mod tidy
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod / go.sum would change after `go mod tidy`
	@cp go.mod go.mod.bak; cp go.sum go.sum.bak 2>/dev/null || true; \
	status=0; \
	if ! $(GO) mod tidy; then \
		echo "go mod tidy failed"; status=1; \
	elif ! diff -q go.mod go.mod.bak >/dev/null 2>&1 || ! diff -q go.sum go.sum.bak >/dev/null 2>&1; then \
		echo "go.mod / go.sum need 'go mod tidy'"; status=1; \
	fi; \
	if [ $$status -ne 0 ]; then mv go.mod.bak go.mod; mv go.sum.bak go.sum 2>/dev/null || true; fi; \
	rm -f go.mod.bak go.sum.bak; \
	exit $$status

.PHONY: test
test: ## Run unit tests
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run tests with -race
	$(GO) test -race -timeout=10m ./...

.PHONY: build
build: ## go build ./...
	$(GO) build ./...

.PHONY: verify
verify: fmt-check vet tidy-check lint test architecture-check ## Fast local gate

.PHONY: ci-local
ci-local: fmt-check tidy-check vet staticcheck vulncheck lint test test-race architecture-check ## Local CI parity gate
