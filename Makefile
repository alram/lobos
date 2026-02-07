CXX = g++
SPDK_ROOT = $(shell pwd)/src/spdk
BOOST_DIR = src/boost_1_90_0
PROTO_DIR = src/controlplane
PROTOGEN_DIR = $(PROTO_DIR)/protos

# Boost
BOOST_LIBS = -L$(BOOST_DIR)/stage/lib -lboost_program_options -lboost_filesystem -lboost_url

# Spdk
PKG_CONFIG = PKG_CONFIG_PATH=$(SPDK_ROOT)/build/lib/pkgconfig
SPDK_COMPONENTS = spdk_syslibs spdk_env_dpdk spdk_bdev spdk_event_bdev spdk_bdev_malloc spdk_bdev_gpt spdk_jsonrpc spdk_bdev_raid spdk_event spdk_blob spdk_blob_bdev spdk_log
SPDK_CFLAGS := $(shell $(PKG_CONFIG) pkg-config --cflags $(SPDK_COMPONENTS))
SPDK_LIBS   := $(shell $(PKG_CONFIG) pkg-config --libs --static $(SPDK_COMPONENTS))

# Prometheus
PROM_LDFLAGS = -lprometheus-cpp-pull -lprometheus-cpp-core -lz

# gRPC/Protobuf
GRPC_CFLAGS := $(shell pkg-config --cflags grpc++ protobuf)
GRPC_LIBS   := $(shell pkg-config --libs grpc++ protobuf)
PROTOC = /usr/bin/protoc
GRPC_PLUGIN = /usr/bin/grpc_cpp_plugin

COMMON_FLAGS = -std=c++20 -Wall -Wextra -I$(BOOST_DIR) -I$(PROTO_DIR) $(SPDK_CFLAGS) $(GRPC_CFLAGS) -MMD -MP
DEBUG_FLAGS = -O0 -g -fno-omit-frame-pointer
RELEASE_FLAGS = \
    -O3 -DNDEBUG \
    -march=native -mtune=native \
    -fomit-frame-pointer \
    -flto \
    -falign-functions=32 -falign-loops=32

CXXFLAGS += $(COMMON_FLAGS) $(RELEASE_FLAGS)
# CXXFLAGS += $(COMMON_FLAGS) $(DEBUG_FLAGS)
LDFLAGS += -flto -Wl,-O1

SRC = src/lobos.cpp src/s3http/server.cpp src/s3http/s3_op_handler.cpp \
	  src/s3http/s3_bucket.cpp \
	  src/common/common.cpp src/index/index.cpp src/store/spdk_store.cpp \
	  src/store/spdk_stats.cpp src/store/fs_store.cpp

# Proto files
PROTO_SRC = $(PROTO_DIR)/loboscontrol.proto
PROTO_GEN = $(PROTOGEN_DIR)/loboscontrol.pb.cc $(PROTOGEN_DIR)/loboscontrol.grpc.pb.cc
PROTO_HDR = $(PROTOGEN_DIR)/loboscontrol.pb.h $(PROTOGEN_DIR)/loboscontrol.grpc.pb.h

# Control plane sources
CONTROL_SRC = $(PROTO_DIR)/loboscontrol_server.cc $(PROTO_GEN)

OBJ = $(SRC:.cpp=.o)
CONTROL_OBJ = $(CONTROL_SRC:.cc=.o)

TARGET = lobos

all: $(TARGET)

# Compile .cc files (for generated proto files)
$(PROTOGEN_DIR)/%.o: $(PROTOGEN_DIR)/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET): $(OBJ) $(CONTROL_OBJ)
	$(CXX) $(OBJ) $(CONTROL_OBJ) -o $@ \
		$(LDFLAGS) \
		$(GRPC_LIBS) \
		$(PROM_LDFLAGS) \
	    $(BOOST_LIBS) \
	    -Wl,--start-group \
		-Wl,--no-as-needed \
		-Wl,--whole-archive \
	        $(SPDK_LIBS) \
		-Wl,--no-whole-archive \
	    -Wl,--end-group \
		-Wl,--disable-new-dtags \
		-Wl,-rpath,$(SPDK_ROOT)/dpdk/build/lib \
		-Wl,-rpath,$(BOOST_DIR)/stage/lib 

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

proto:
	$(PROTOC) -I$(PROTO_DIR) --cpp_out=$(PROTOGEN_DIR) --grpc_out=$(PROTOGEN_DIR) --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) $(PROTO_DIR)/loboscontrol.proto

clean:
	rm -f $(OBJ) $(CONTROL_OBJ) $(TARGET) $(OBJ:.o=.d) $(CONTROL_OBJ:.o=.d)
	rm -f $(PROTO_GEN) $(PROTO_HDR)

.PHONY: all clean

# Dependencies
-include $(OBJ:.o=.d)
-include $(CONTROL_OBJ:.o=.d)
