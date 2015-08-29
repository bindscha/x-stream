OBJECT_DIR= object_files
UTILS_SRC= utils.o
LIBS_SRC= core.o $(UTILS_SRC)
LIBS= $(addprefix $(OBJECT_DIR)/, $(LIBS_SRC))
UTILS= $(addprefix $(OBJECT_DIR)/, $(UTILS_SRC))
PROGS_SRC= driver.o file_splitter.o mem_speed_sequential.o sort_edges.o\
mem_speed_random.o feeder.o
PROGS= $(addprefix $(OBJECT_DIR)/, $(PROGS_SRC))
TARGETS= bin/benchmark_driver bin/mem_speed_sequential bin/sort_edges\
bin/mem_speed_random bin/zpipe bin/feeder


all: $(LIBS) $(TARGETS) generators

CXX?= g++

CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -L/usr/local/lib
#CXXFLAGS?= -O3 -g -Wall -Wno-unused-function
#CXXFLAGS?= -g
CXXFLAGS += -Wfatal-errors
EXTRA_INCLUDES=-include core/types.h
EXTRA_INCLUDES+=-I/usr/local/include/boost-numeric-bindings

#Graph size limits
CXXFLAGS += -DCOMPACT_GRAPH

#ZLIB SPEEDS
#CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_DEFAULT_COMPRESSION
CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_BEST_SPEED
#CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_BEST_COMPRESSION

#ISA extensions
CXXFLAGS += -msse4.2
#Note avx2 is untested
#CXXFLAGS += -mavx2

#System libraries
SYSLIBS = -lboost_system -lboost_program_options -lboost_thread -lz -lrt
# Uncomment for ALS
#SYSLIBS += -llapack

#Python support (uncomment following lines)
#CXXFLAGS += -DPYTHON_SUPPORT
#SYSLIBS += -lpython3.2mu
#EXTRA_INCLUDES+=-L/usr/lib/python3.2/config
#EXTRA_INCLUDES+=-I/usr/include/python3.2




-include $(LIBS:.o=.d) $(PROGS:.o=.d) 

$(OBJECT_DIR)/core.o:core/core.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/core.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/utils.o:utils/utils.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/utils.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/driver.o:benchmarks/driver.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/driver.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/sort_edges.o:utils/sort_edges.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/sort_edges.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/feeder.o:utils/feeder.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/feeder.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/mem_speed_sequential.o:utils/mem_speed_sequential.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/mem_speed_sequential.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/mem_speed_random.o:utils/mem_speed_random.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/mem_speed_random.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/zpipe.o:utils/zpipe.c 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/zpipe.o' $< > $(@:.o=.d)

bin/benchmark_driver:$(OBJECT_DIR)/driver.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/mem_speed_sequential:$(OBJECT_DIR)/mem_speed_sequential.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/mem_speed_random:$(OBJECT_DIR)/mem_speed_random.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/zpipe:$(OBJECT_DIR)/zpipe.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/sort_edges:$(OBJECT_DIR)/sort_edges.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/feeder:$(OBJECT_DIR)/feeder.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)


.PHONY: generators
generators:
	$(MAKE) -C generators

clean:
	rm -f $(TARGETS) $(LIBS) $(OBJECT_DIR)/*
	$(MAKE) -C generators clean

prefix = /usr/local
bindir = $(prefix)/bin
.PHONY: install uninstall
install: all
	cp bin/benchmark_driver $(bindir)
	cp generators/rmat $(bindir)
	cp generators/erdos-renyi $(bindir)

uninstall: 
	rm -f $(bindir)/benchmark_driver
	rm -f $(bindir)/rmat
	rm -f $(bindir)/erdos-renyi
