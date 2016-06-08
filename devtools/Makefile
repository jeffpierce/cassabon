# Makefile for the stresstest executable.

SRCS = echostats.go pathdistribution.go stresstest.go

TARGETS = $(patsubst %.go,%,$(SRCS))

all: $(TARGETS)

%: %.go
	go build -o $@ $<

clean:
	rm -f $(TARGETS)
