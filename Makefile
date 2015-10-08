# Makefile for Cassabon

TARGET = cassabon

BUILDDIR = build

SOURCES = $(TARGET).go api/*go config/*go datastore/*go listener/*go logging/*go middleware/*go
TESTS = . ./api ./config ./datastore ./listener ./logging ./middleware ./pearson

VERSION = $(shell cat VERSION)

all: build

clean:
	rm -rf $(BUILDDIR)

fetch:
	go get -t -d -v ./...

test:
	go test -race $(TESTS)

config/version.go: VERSION
	@echo "Updating version.go to $(VERSION)"
	$(shell sed s/XXXXXXXXXX/$(VERSION)/ config/version.go.template > config/version.go)

build: $(BUILDDIR)/$(TARGET)

$(BUILDDIR)/$(TARGET): $(SOURCES) config/version.go
	go build -race -o $(BUILDDIR)/$(TARGET) $(TARGET).go
