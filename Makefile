# Makefile for Cassabon

TARGET = cassabon

BUILDDIR = build

SOURCES = $(TARGET).go api/*go config/*go datastore/*go listener/*go logging/*go middleware/*go
PACKAGES = . ./api ./config ./datastore ./listener ./middleware ./pearson

VERSION = $(shell cat VERSION)

all: build

clean:
	rm -rf $(BUILDDIR)

fetch:
	go get -t -d -v ./...

vendorize:
	godep save $(PACKAGES)

# Logging currently shows a data race that only shows in testing, not in build or run.
test:
	go test -race $(PACKAGES)
	go test ./logging

config/version.go: VERSION
	@echo "Updating version.go to $(VERSION)"
	$(shell sed s/XXXXXXXXXX/$(VERSION)/ config/version.go.template > config/version.go)

build: $(BUILDDIR)/$(TARGET)

$(BUILDDIR)/$(TARGET): $(SOURCES) config/version.go
	go build -race -o $(BUILDDIR)/$(TARGET) $(TARGET).go
