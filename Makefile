# Makefile for Cassabon

TARGET = cassabon

BUILDDIR = build
RELEASE = $(shell git rev-parse HEAD)
DESTDIR = $(BUILDDIR)/$(RELEASE)
TARBALL = $(BUILDDIR)/$(TARGET)-$(RELEASE).tar.gz

SOURCES = $(TARGET).go api/*go config/*go datastore/*go engine/*go listener/*go logging/*go middleware/*go
TESTS = . ./api ./config ./datastore ./engine ./listener ./logging ./middleware ./pearson

UNAME = $(shell uname)

all: $(DESTDIR) $(TARBALL)

clean:
	rm -rf $(BUILDDIR)

fetch:
	go get -t -d -v ./...

test:
	go test -race $(TESTS)

build: $(DESTDIR)/$(TARGET)

$(DESTDIR):
	mkdir -p $(DESTDIR)

$(DESTDIR)/$(TARGET): $(SOURCES)
	go build -race -o $(DESTDIR)/$(TARGET) $(TARGET).go

$(TARBALL): $(DESTDIR)/$(TARGET)
	tar czf $(TARBALL) -C $(DESTDIR) .

upload: $(TARBALL)
ifeq ($(UNAME), Darwin)
	$(error $(UNAME) binaries are not valid in production; refusing to upload to S3)
else
	$(error Upload to S3 not implemented)
endif
