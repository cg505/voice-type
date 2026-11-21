BINARY  := voice-type
PKG     := ./cmd/voice-type
VERSION ?= $(shell cat VERSION)
LDFLAGS := -s -w -X main.version=$(VERSION)

# CGO stays off everywhere: all three dependencies are pure Go, and a static
# binary is the whole point of the rewrite.
export CGO_ENABLED = 0

.PHONY: all build test test-live vet fmt check clean dist run install

all: build

build:
	go build -ldflags="$(LDFLAGS)" -o $(BINARY) $(PKG)

test:
	go test ./...

# Requires a 16 kHz mono s16le file in VOICE_TYPE_PCM, plus OPENROUTER_API_KEY
# for the batch test and/or a streaming key for the streaming one: XAI_API_KEY,
# or AI_GATEWAY_API_KEY with VOICE_TYPE_MODEL=mai-transcribe-2-streaming. The
# streaming test also takes a WAV, and VOICE_TYPE_STOP_AT=<seconds> to stop
# mid-recording.
test-live:
	VOICE_TYPE_LIVE=1 go test -run TestLiveTranscribe -v ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

check: fmt vet test

run: build
	./$(BINARY)

# Installs from the working tree, skipping the release download.
install:
	./install.sh --local

dist:
	mkdir -p dist
	GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-x64 $(PKG)
	GOOS=linux GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-arm64 $(PKG)
	@ls -lh dist/

clean:
	rm -rf $(BINARY) dist
