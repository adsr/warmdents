all: warmdents

warmdents: warmdents.c
	clang -g -Wall -Wextra -pedantic warmdents.c -o warmdents

clean:
	rm -f warmdents

.PHONY: clean
