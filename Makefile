warmdents_cflags:=-std=c11 -Wall -Wextra -pedantic -D_DEFAULT_SOURCE $(CFLAGS)

all: warmdents

warmdents: warmdents.c
	$(CC) $(warmdents_cflags) warmdents.c -o warmdents

clean:
	rm -f warmdents

.PHONY: clean
