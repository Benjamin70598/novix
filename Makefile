.PHONY: build test crashtest clean

build:
	mkdir -p bin
	go build -o bin/novixd  ./cmd/novixd
	go build -o bin/posgen  ./cmd/posgen
	go build -o bin/kds     ./cmd/kds
	go build -o bin/telegen ./cmd/telegen

test:
	go test ./... -count=1

crashtest: build
	bash test/crashtest.sh

# Shorter variant for quick iteration.
crashtest-quick: build
	COUNT=600 RATE=300 KILL_EVERY=1 bash test/crashtest.sh

clean:
	rm -rf bin
