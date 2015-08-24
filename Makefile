# Makefile for Cassabon

TARGET = cassabon
BUILDDIR = build
SOURCES = $(TARGET).go api/*go config/*go datastore/*go engine/*go listener/*go logging/*go middleware/*go

$(BUILDDIR)/$(TARGET): $(SOURCES)
	mkdir -p $(BUILDDIR)
	go build -race -o $(BUILDDIR)/$(TARGET) $(TARGET).go

build: $(BUILDDIR)/$(TARGET)

test:
	go test -race -v ./...

clean:
	rm -rf $(BUILDDIR)
