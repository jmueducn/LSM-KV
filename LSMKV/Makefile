
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence test

correctness: kvstore.o correctness.o

persistence: kvstore.o persistence.o

test: kvstore.o test.o

clean:
	-rm -f correctness persistence test*.o
