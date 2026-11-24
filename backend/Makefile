.PHONY: build build-entry build-worker check-worker-deps generate test test-unit test-integration test-e2e

VERSION ?= $(shell ./scripts/resolve-version.sh)
LDFLAGS ?= -s -w -X main.Version=$(VERSION)
BIN_DIR ?= bin

build:
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -trimpath -o bin/server ./cmd/server

build-entry:
	@test -s internal/web/dist/index.html || { printf '%s\n' 'Missing frontend build: run make build-entry from the repository root.' >&2; exit 1; }
	CGO_ENABLED=0 go build -tags embed -ldflags="$(LDFLAGS)" -trimpath -o "$(BIN_DIR)/nexus-entry" ./cmd/entry

build-worker: check-worker-deps
	CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o "$(BIN_DIR)/nexus-worker" ./cmd/worker

check-worker-deps:
	@deps=$$(CGO_ENABLED=0 go list -deps ./cmd/worker) || exit $$?; \
		printf '%s\n' "$$deps" | awk ' \
			/^(entgo[.]io\/ent|github[.]com\/(redis\/go-redis|lib\/pq|gin-gonic\/gin|aws\/aws-sdk-go-v2))(\/|$$)/ || \
			/^github[.]com\/Wei-Shaw\/sub2api\/(ent|internal\/(service|repository|web))(\/|$$)/ { \
				print "forbidden Worker dependency: " $$0; blocked = 1 \
			} \
			END { exit blocked }' >&2

generate:
	go generate ./ent
	go generate ./cmd/server

test:
	go test ./...
	golangci-lint run ./...

test-unit:
	go test -tags=unit ./...

test-integration:
	go test -tags=integration ./...

test-e2e:
	./scripts/e2e-test.sh

test-e2e-local:
	go test -tags=e2e -v -timeout=300s ./internal/integration/...
