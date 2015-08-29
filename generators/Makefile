CC = g++
CFLAGS = -std=c++0x -DNDEBUG -O3 -Wall -Drestrict=__restrict__ -ffast-math -L/usr/local/lib
#CFLAGS = -std=c++0x -g -O3 -Wall -Drestrict=__restrict__ -ffast-math -L/usr/local/lib
CFLAGS += -DVERTEX_TYPE_32
CFLAGS += -DVALUE_TYPE_32
CFLAGS += -DNOISE
CFLAGS += -DWEIGHT
LDFLAGS = -lboost_program_options -lboost_thread -lboost_system

PRNG_SOURCES = splittable_mrg.c utils.c
GEN_SOURCES = options.cpp output.cpp

.PHONY: all clean

all: erdos-renyi rmat

erdos-renyi: random.cpp $(addprefix prng/,$(PRNG_SOURCES)) $(GEN_SOURCES)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

rmat: rmat.cpp $(addprefix prng/,$(PRNG_SOURCES)) $(GEN_SOURCES)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f erdos-renyi rmat
