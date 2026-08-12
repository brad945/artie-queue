.PHONY: build test demo clean

build:
	go build -o artie-queue ./cmd/artie-queue

test:
	go test ./... -race -count=1

demo: build
	go run ./cmd/jobrunner

clean:
	rm -rf artie-queue data
