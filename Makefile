SHELL := /bin/bash -o pipefail

GOLANGCI_LINT_BINARY ?= golangci-lint
APP_NAME             ?= nomad-csi-driver
VERSION_PKG           = github.com/honest-hosting/nomad-csi-driver/internal/version
# Exported so they reach localdev/preflight.sh + `go test`; a shell-exported
# NOMAD_ADDR overrides this default.
export NOMAD_ADDR         ?= https://192.168.56.51:4646
export NOMAD_SKIP_VERIFY  ?= true

# Image tags: with TAG set, tag both "<TAG>" and "latest"; otherwise just "latest".
# A line-based ifeq avoids $(if)'s comma-splitting (no $(comma) helper needed).
ifeq ($(strip $(TAG)),)
package: export PACKER_IMAGE_BUILD_TAGS ?= "latest"
else
package: export PACKER_IMAGE_BUILD_TAGS ?= "$(TAG)","latest"
endif

default: help
.PHONY: default

help: ## Display this help screen (default)
	@grep -h "##" $(MAKEFILE_LIST) | grep -v grep | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | sort
.PHONY: help

build: export BUILD_DATE       ?= $(shell date --iso-8601=ns)
build: export BUILD_COMMIT_SHA ?= $(shell git -C $(CURDIR) rev-parse HEAD 2>/dev/null || echo unknown)
build: export BUILD_VERSION    ?= $(if $(TAG),$(TAG),vLOCALDEV)
build: build-check mod-download ## Build nomad-csi-driver into ./bin/nomad-csi-driver
	@echo "Performing build for 'bin/nomad-csi-driver'"
	@go build \
		-ldflags="-X $(VERSION_PKG).Version=$(BUILD_VERSION) -X $(VERSION_PKG).CommitSHA=$(BUILD_COMMIT_SHA) -X $(VERSION_PKG).BuildDate=$(BUILD_DATE) -s -w" \
		-o="bin/nomad-csi-driver" \
		-compiler="gc" \
		./cmd/nomad-csi-driver
	@pushd bin >/dev/null ; sha256sum "nomad-csi-driver" > "nomad-csi-driver.sha256" ; popd >/dev/null
.PHONY: build

build-check:
	@if [[ -z "$(GOROOT)" ]]; then                              \
		echo "WARNING: GOROOT is not set; using Go from PATH";  \
	fi
.PHONY: build-check

install: ## go install ./cmd/nomad-csi-driver into GOBIN
	@go install ./cmd/nomad-csi-driver
.PHONY: install

package: package-preflight ## Build + push the container image via Packer: make package
	@echo "Performing Packer build for 'bin/$(APP_NAME)'"
	@packer build \
		--var='app_build_tags=[$(PACKER_IMAGE_BUILD_TAGS)]' \
		--var app_file_path="bin/$(APP_NAME)" \
		--var docker_repo="$(DOCKER_REPO)" \
		--var docker_host="$(DOCKER_HOSTNAME)" \
		--var docker_username="$(DOCKER_USERNAME)" \
		--var docker_password="$(DOCKER_PASSWORD)" \
		$(APP_NAME).pkr.hcl
.PHONY: package

package-preflight:
	@packer init $(APP_NAME).pkr.hcl
	@if [[ -z "$(DOCKER_HOSTNAME)" ]] || [[ -z "$(DOCKER_USERNAME)" ]] || [[ -z "$(DOCKER_PASSWORD)" ]] || [[ -z "$(DOCKER_REPO)" ]]; then \
		echo "ERROR: set DOCKER_HOSTNAME/DOCKER_USERNAME/DOCKER_PASSWORD/DOCKER_REPO before packaging"; \
		exit 1; \
	fi
.PHONY: package-preflight

release: release-preflight release-tag ## Cut a new release on GitHub: ENVIRONMENT=production TAG=0.0.1 make release
	@gh release create "$(TAG)"     \
		"bin/$(APP_NAME)"           \
		"bin/$(APP_NAME).sha256"    \
		--title "Release $(TAG)"    \
		--generate-notes            \
		--latest
.PHONY: release

release-tag:
	@echo "Creating GitHub tag $(TAG)..."
	@echo git tag "$(TAG)"
	@echo git push origin "$(TAG)"
.PHONY: release-tag

release-preflight:
	@test -n "$(TAG)" || (echo "ERROR: usage: ENVIRONMENT=production TAG=0.0.1 make release" && exit 1)
	@echo "$(TAG)" | grep -Eq '^v?[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$$' || (echo "ERROR: TAG must look like 0.0.1" && exit 1)
	@git diff --quiet || (echo "ERROR: working tree has unstaged changes" && exit 1)
	@git diff --cached --quiet || (echo "ERROR: index has staged but uncommitted changes" && exit 1)
	@git rev-parse "$(TAG)" >/dev/null 2>&1 && (echo "ERROR: tag $(TAG) already exists locally" && exit 1) || true
.PHONY: release-preflight

clean: test-integration-teardown ## Remove built artifacts
	@rm -f ./bin/nomad-csi-driver ./bin/nomad-csi-driver.sha256
.PHONY: clean

lint: ## Run linter against codebase
	@$(GOLANGCI_LINT_BINARY) -v run --timeout 5m
.PHONY: lint

vet: ## Run go vet
	@go vet ./...
.PHONY: vet

test: export TEST       ?= .*
test: export TEST_DIR   ?= ./...
test: export TEST_COUNT ?= 1
test: test-setup ## Run unit tests (integration excluded by build tag): TEST=.* TEST_DIR=./... TEST_COUNT=1 make test
	@go test -v -race -count=$(TEST_COUNT) -run "$(TEST)" $(TEST_DIR) 2>&1 | tee /tmp/nomad-csi-test.log
	@echo "UnitTest completed, see /tmp/nomad-csi-test.log for details"
.PHONY: test

test-stress: export TEST       ?= TestConcurrent
test-stress: export TEST_DIR   ?= ./...
test-stress: export TEST_COUNT ?= 100
test-stress: test-setup ## Run stress tests: TEST=TestConcurrent TEST_DIR=./... TEST_COUNT=100 make test-stress
	@go test -v -race -count=$(TEST_COUNT) -run "$(TEST)" $(TEST_DIR) 2>&1 | tee /tmp/nomad-csi-stress.log
	@echo "StressTest completed, see /tmp/nomad-csi-stress.log for details"
.PHONY: test-stress

test-integration: export TEST              ?= ^TestIntegration
test-integration: export TEST_DIR          ?= ./...
test-integration: test-integration-preflight test-integration-deploy test-setup ## E2E volume/placement suite against the deployed plugins (run test-integration-deploy first). See localdev/README.md.
	@go test -v -race -p 1 -tags=integration -timeout=30m -run "$(TEST)" $(TEST_DIR) 2>&1 | tee /tmp/nomad-csi-integration.log
	@echo "IntegrationTest completed, see /tmp/nomad-csi-integration.log for details"
.PHONY: test-integration

test-integration-preflight: ## Check the external cluster is reachable (NOMAD_ADDR set, nomad up); bails if not
	@bash localdev/preflight.sh
.PHONY: test-integration-preflight

test-integration-deploy: ## Deploy the CSI plugin jobs (local + qnap) to NOMAD_ADDR and wait healthy; run before test-integration
	@bash localdev/deploy.sh
.PHONY: test-integration-deploy

test-integration-teardown: test-integration-preflight ## Purge the CSI plugin jobs + e2e volumes/consumers from NOMAD_ADDR
	@bash localdev/teardown.sh
.PHONY: test-integration-teardown

test-setup:
	@go clean -testcache
.PHONY: test-setup

mod-download: ## Download go modules
	@go mod download
.PHONY: mod-download

mod-tidy: ## Make sure go modules are tidy
	@go mod tidy
.PHONY: mod-tidy
