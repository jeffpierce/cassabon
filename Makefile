# Makefile for Cassabon

TARGET = cassabon

BUILDDIR = build

SOURCES = $(TARGET).go api/*go config/*go datastore/*go listener/*go logging/*go middleware/*go
TESTS = . ./api ./config ./datastore ./listener ./logging ./middleware ./pearson

UNAME = $(shell uname)

all: build

clean:
	rm -rf $(BUILDDIR)

fetch:
	go get -t -d -v ./...

test:
	go test -race $(TESTS)

build: $(BUILDDIR)/$(TARGET)

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/$(TARGET): $(SOURCES)
	go build -race -o $(BUILDDIR)/$(TARGET) $(TARGET).go
