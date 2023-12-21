.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-elasticsearch.version=${VERSION}'" -o conduit-connector-elasticsearch cmd/connector/main.go

# Run required docker containers, execute integration tests, stop containers after tests
test:
	# Tests that does not require Docker services to be running
	go test -race $(go list ./... | grep -Fv '/test/v')

	# Elasticsearch v5
	docker compose -f test/docker-compose.v5.yml -p test-v5 up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./test/v5; ret=$$?; \
	  	docker compose -f test/docker-compose.v5.yml -p test-v5 down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

	# Elasticsearch v6
	docker compose -f test/docker-compose.v6.yml -p test-v6 up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./test/v6; ret=$$?; \
	  	docker compose -f test/docker-compose.v6.yml -p test-v6 down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

	# Elasticsearch v7
	docker compose -f test/docker-compose.v7.yml -p test-v7 up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./test/v7; ret=$$?; \
	  	docker compose -f test/docker-compose.v7.yml -p test-v7 down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

	# Elasticsearch v8
	docker compose -f test/docker-compose.v8.yml -p test-v8 up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./test/v8; ret=$$?; \
	  	docker compose -f test/docker-compose.v8.yml -p test-v8 down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

lint:
	golangci-lint run -v

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
