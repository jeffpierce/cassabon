# Makefile for Cassabon

TARGET = cassabon

run:
	go run -race $(TARGET).go

test:
	go test -race -v ./...
