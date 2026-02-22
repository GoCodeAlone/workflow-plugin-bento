BINARY_NAME = workflow-plugin-bento
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS = -ldflags "-X main.version=$(VERSION)"
PLATFORMS = linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.PHONY: build clean test lint install cross-build

build:
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/$(BINARY_NAME)

install: build
	mkdir -p $(DESTDIR)/data/plugins/$(BINARY_NAME)
	cp bin/$(BINARY_NAME) $(DESTDIR)/data/plugins/$(BINARY_NAME)/
	cp plugin.json $(DESTDIR)/data/plugins/$(BINARY_NAME)/

clean:
	rm -rf bin/

test:
	go test ./... -v -race

lint:
	go fmt ./...
	golangci-lint run

cross-build:
	@for platform in $(PLATFORMS); do \
		os=$${platform%%/*}; \
		arch=$${platform##*/}; \
		output=bin/$(BINARY_NAME)-$${os}-$${arch}; \
		echo "Building $${output}..."; \
		GOOS=$${os} GOARCH=$${arch} go build $(LDFLAGS) -o $${output} ./cmd/$(BINARY_NAME); \
	done
