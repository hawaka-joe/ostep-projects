CC = gcc
CFLAGS = -Wall -Werror -pthread -O
TARGET = psort
SOURCE = psort.c

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) -o $(TARGET) $(SOURCE)

clean:
	rm -f $(TARGET)

.PHONY: all clean

