build:
	go build -o bin/server ./cmd/server
	go build -o bin/cli ./cmd/cli
test:
	go test ./...
lint:
	golangci-lint run
proto:
	buf generate