# Makefile for the stresstest executable.

TARGET = stresstest

all: $(TARGET)

$(TARGET): $(TARGET).go
	go build -o $(TARGET) $<

clean:
	rm -f $(TARGET)
