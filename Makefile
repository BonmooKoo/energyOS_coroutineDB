# Compiler and flags
CXX = g++
CXXFLAGS = -std=gnu++17 -O3 \
    -I/usr/local/include \
    -I./
LDFLAGS = \
    -L/usr/local/lib \
    -Wl,-rpath,/usr/local/lib \
    -lboost_coroutine \
    -lboost_context \
    -lboost_system \
    -libverbs \
    -lmemcached \
    -lpthread

# Common sources/objects
SRCS_COMMON = keeper.cpp rdma_verb.cpp rdma_common.cpp zipf.cpp
OBJS_COMMON = $(SRCS_COMMON:.cpp=.o)

# Targets
all: client server

# one_sided_consol acts as client
client: $(OBJS_COMMON) one_sided_consol.o
	$(CXX) -g $(CXXFLAGS) -o client $(OBJS_COMMON) one_sided_consol.o $(LDFLAGS)

# server as before
server: $(OBJS_COMMON) rdma_server.o
	$(CXX) $(CXXFLAGS) -o server rdma_common.o rdma_server.o keeper.o $(LDFLAGS)

# Compile .cpp to .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f *.o client server

