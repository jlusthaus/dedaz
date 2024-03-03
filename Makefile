.PHONY: all
all: build

.PHONY: build
build:
	@go build 
	
.PHONY: lint
lint:
	go mod tidy
	golangci-lint run -v