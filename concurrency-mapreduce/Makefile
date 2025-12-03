CC = gcc
CFLAGS = -Wall -Werror -pthread -O
TARGET_LIB = mapreduce.o
SOURCE = mapreduce.c

all: $(TARGET_LIB)

$(TARGET_LIB): $(SOURCE)
	$(CC) $(CFLAGS) -c $(SOURCE) -o $(TARGET_LIB)

test: test_wordcount
	./test_wordcount test1.txt test2.txt

test_wordcount: test_wordcount.c mapreduce.c
	$(CC) $(CFLAGS) -o test_wordcount test_wordcount.c mapreduce.c

clean:
	rm -f $(TARGET_LIB) test_wordcount test*.txt *.o

.PHONY: all test clean

