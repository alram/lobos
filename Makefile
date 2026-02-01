CXX = g++
SPDK_ROOT = $(shell pwd)/src/spdk
BOOST_DIR = src/boost_1_90_0
BOOST_LIBS = -L$(BOOST_DIR)/stage/lib -lboost_program_options -lboost_filesystem -lboost_url

PKG_CONFIG = PKG_CONFIG_PATH=$(SPDK_ROOT)/build/lib/pkgconfig
SPDK_COMPONENTS = spdk_syslibs spdk_env_dpdk spdk_bdev spdk_event_bdev spdk_bdev_malloc spdk_bdev_gpt spdk_jsonrpc spdk_bdev_raid spdk_event spdk_blob spdk_blob_bdev spdk_log
SPDK_CFLAGS := $(shell $(PKG_CONFIG) pkg-config --cflags $(SPDK_COMPONENTS))
SPDK_LIBS   := $(shell $(PKG_CONFIG) pkg-config --libs --static $(SPDK_COMPONENTS))
PROM_LDFLAGS = -lprometheus-cpp-pull -lprometheus-cpp-core -lz

COMMON_FLAGS = -std=c++20 -Wall -Wextra -I$(BOOST_DIR) $(SPDK_CFLAGS) -MMD -MP
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

SRC = src/lobos.cpp src/s3http/server.cpp src/s3http/s3_op_handler.cpp src/common/common.cpp src/index/index.cpp src/store/spdk_store.cpp src/store/spdk_stats.cpp src/store/fs_store.cpp
OBJ = $(SRC:.cpp=.o)
TARGET = lobos

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(OBJ) -o $@ \
		$(LDFLAGS) \
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

clean:
	rm -f $(OBJ) $(TARGET) $(OBJ:.o=.d)

.PHONY: all clean

-include $(OBJ:.o=.d)
