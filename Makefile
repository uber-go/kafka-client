export GO15VENDOREXPERIMENT=1

BENCH_FLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS ?= ./cmd/... ./internal/... ./kafka/... .
# Many Go tools take file globs or directories as arguments instead of packages.
PKG_FILES ?= *.go kafka internal/consumer internal/backoff internal/list internal/metrics internal/util

# The linting tools evolve with each Go version, so run them only on the latest
# stable release.
GO_VERSION := $(shell go version | cut -d " " -f 3)
GO_MINOR_VERSION := $(word 2,$(subst ., ,$(GO_VERSION)))
LINTABLE_MINOR_VERSIONS := 9
ifneq ($(filter $(LINTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
SHOULD_LINT := true
endif


.PHONY: all
all: lint test

.PHONY: dependencies
dependencies:
	@echo "Installing Dep and locked dependencies..."
	command -v dep || curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
	dep ensure
	@echo "Installing test dependencies..."
	go get -u -f github.com/axw/gocov/gocov
	go get -u -f github.com/mattn/goveralls
ifdef SHOULD_LINT
	@echo "Installing golint..."
	go get -u -f github.com/golang/lint/golint
else
	@echo "Not installing golint, since we don't expect to lint on" $(GO_VERSION)
endif

# Disable printf-like invocation checking due to testify.assert.Error()
VET_RULES := -printf=false

.PHONY: lint
lint: dependencies
ifdef SHOULD_LINT
	@rm -rf lint.log
	@echo "Checking formatting..."
	@gofmt -d -s $(PKG_FILES) 2>&1 | tee lint.log
	@echo "Installing test dependencies for vet..."
	@go test -i $(PKGS)
	@echo "Checking vet..."
	@$(foreach dir,$(PKG_FILES),go tool vet $(VET_RULES) $(dir) 2>&1 | tee -a lint.log;)
	@echo "Checking lint..."
	@$(foreach dir,$(PKGS),golint $(dir) | grep -v ".pb.go" 2>&1 | tee -a lint.log;)
	@echo "Checking for unresolved FIXMEs..."
	@git grep -i fixme | grep -v -e vendor -e Makefile | tee -a lint.log
	@echo "Checking for license headers..."
	@go run ./cmd/tools/copyright/licensegen.go --verifyOnly | tee -a lint.log
	@[ ! -s lint.log ]
else
	@echo "Skipping linters on" $(GO_VERSION)
endif

.PHONY: test
test: dependencies
	go test $(PKGS)

.PHONY: cover
cover: dependencies
	./scripts/cover.sh $(PKGS)

.PHONY: bench
BENCH ?= .
bench: dependencies
	@$(foreach pkg,$(PKGS),go test -bench=$(BENCH) -run="^$$" $(BENCH_FLAGS) $(pkg);)
