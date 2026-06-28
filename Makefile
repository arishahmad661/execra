build:
	go build -o bin/server ./cmd/server
	go build -o bin/cli ./cmd/cli
test:
	go test ./...
lint:
	golangci-lint run
proto:
	buf generate

run-server:
	go run ./cmd/server
run-compose:
	docker compose up --build
down:
	docker compose down