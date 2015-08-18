# Makefile for Cassabon

TARGET = cassabon

run:
	go run -race $(TARGET).go $(ARGS)

test:
	go test -race -v ./...
