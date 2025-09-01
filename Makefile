.PHONY: build run test docker-up docker-down clean

build:
	go build -o bin/api ./cmd/api
	go build -o bin/consumer ./cmd/consumer

run-api:
	go run ./cmd/api

run-consumer:
	go run ./cmd/consumer

test:
	go test ./...

docker-up:
	cd docker
	docker-compose up -d

docker-down:
	cd docker
	docker-compose down

clean:
	rm -rf bin/