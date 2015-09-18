# Makefile for the stresstest executable.

SRCS = stresstest.go echostats.go

TARGETS = $(patsubst %.go,%,$(SRCS))

all: $(TARGETS)

%: %.go
	go build -o $@ $<

clean:
	rm -f $(TARGETS)
