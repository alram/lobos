cmake_minimum_required(VERSION 3.16)
project(lobos LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# For Intellisense
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(ENABLE_SPDK "Build with SPDK support" ON)

# Build type defaults
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native \
    -fomit-frame-pointer -flto -falign-functions=32 -falign-loops=32")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-flto -Wl,-O1")

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer")

# Protobuf / gRPC code generation
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/src/controlplane)
set(PROTOGEN_DIR ${PROTO_DIR}/protos)
set(PROTO_FILE ${PROTO_DIR}/loboscontrol.proto)

find_program(PROTOC protoc REQUIRED)
find_program(GRPC_PLUGIN grpc_cpp_plugin REQUIRED)

set(PROTO_GEN_SRCS
    ${PROTOGEN_DIR}/loboscontrol.pb.cc
    ${PROTOGEN_DIR}/loboscontrol.grpc.pb.cc
)
set(PROTO_GEN_HDRS
    ${PROTOGEN_DIR}/loboscontrol.pb.h
    ${PROTOGEN_DIR}/loboscontrol.grpc.pb.h
)

add_custom_command(
    OUTPUT ${PROTO_GEN_SRCS} ${PROTO_GEN_HDRS}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${PROTOGEN_DIR}
    COMMAND ${PROTOC}
        -I${PROTO_DIR}
        --cpp_out=${PROTOGEN_DIR}
        --grpc_out=${PROTOGEN_DIR}
        --plugin=protoc-gen-grpc=${GRPC_PLUGIN}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf/gRPC sources"
)

# Sources
set(COMMON_SRCS
    src/lobos.cpp
    src/s3http/server.cpp
    src/s3http/s3_op_handler.cpp
    src/s3http/s3_bucket.cpp
    src/common/common.cpp
    src/store/fs_store.cpp
)

set(CONTROL_SRCS
    ${PROTO_DIR}/loboscontrol_server.cc
    ${PROTO_GEN_SRCS}
)

set(ALL_SRCS ${COMMON_SRCS} ${CONTROL_SRCS})

if(ENABLE_SPDK)
    list(APPEND ALL_SRCS
        src/index/spdk_index.cpp
        src/store/spdk_store.cpp
        src/store/spdk_stats.cpp
    )
endif()

add_executable(${PROJECT_NAME} ${ALL_SRCS})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${PROTO_DIR}
)

find_package(PkgConfig REQUIRED)

# gRPC / Protobuf
pkg_check_modules(GRPC REQUIRED grpc++ protobuf)
target_include_directories(${PROJECT_NAME} PRIVATE ${GRPC_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} PRIVATE ${GRPC_LIBRARIES})

# Boost
find_package(Boost REQUIRED COMPONENTS program_options filesystem url)
target_link_libraries(${PROJECT_NAME} PRIVATE Boost::program_options Boost::filesystem Boost::url)

# SPDK
if(ENABLE_SPDK)

    # Prometheus
    target_link_libraries(${PROJECT_NAME} PRIVATE prometheus-cpp-pull prometheus-cpp-core z)

    set(SPDK_ROOT ${CMAKE_SOURCE_DIR}/src/spdk)
    set(SPDK_PKG_CONFIG_PATH "${SPDK_ROOT}/build/lib/pkgconfig")

    set(SPDK_COMPONENTS
        spdk_syslibs spdk_env_dpdk spdk_bdev spdk_event_bdev
        spdk_bdev_malloc spdk_bdev_gpt spdk_jsonrpc spdk_bdev_raid
        spdk_event spdk_blob spdk_blob_bdev spdk_log
    )

    # Get SPDK compile flags via pkg-config --cflags
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env "PKG_CONFIG_PATH=${SPDK_PKG_CONFIG_PATH}"
                pkg-config --cflags ${SPDK_COMPONENTS}
        OUTPUT_VARIABLE SPDK_CFLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE SPDK_CFLAGS_RESULT
    )
    if(NOT SPDK_CFLAGS_RESULT EQUAL 0)
        message(FATAL_ERROR "pkg-config --cflags failed for SPDK components")
    endif()

    # Get the full raw linker flags via pkg-config --libs --static.
    # Somehow cmake's builtin didn't return pthread and isa-l libs
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env "PKG_CONFIG_PATH=${SPDK_PKG_CONFIG_PATH}"
                pkg-config --libs --static ${SPDK_COMPONENTS}
        OUTPUT_VARIABLE SPDK_LDFLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE SPDK_LDFLAGS_RESULT
    )
    if(NOT SPDK_LDFLAGS_RESULT EQUAL 0)
        message(FATAL_ERROR "pkg-config --libs --static failed for SPDK components")
    endif()

    target_compile_definitions(${PROJECT_NAME} PRIVATE ENABLE_SPDK)

    # Apply SPDK compile flags
    separate_arguments(SPDK_CFLAGS_LIST UNIX_COMMAND "${SPDK_CFLAGS}")
    target_compile_options(${PROJECT_NAME} PRIVATE ${SPDK_CFLAGS_LIST})

    # Link SPDK: wrap the raw pkg-config output in --whole-archive / --start-group
    # so that all static archive symbols (including ISA-L constructors) are kept
    # and circular deps between SPDK libs are resolved.
    separate_arguments(SPDK_LDFLAGS_LIST UNIX_COMMAND "${SPDK_LDFLAGS}")
    target_link_libraries(${PROJECT_NAME} PRIVATE
        "-Wl,--start-group"
        "-Wl,--no-as-needed"
        "-Wl,--whole-archive"
        ${SPDK_LDFLAGS_LIST}
        "-Wl,--no-whole-archive"
        "-Wl,--end-group"
        "-Wl,--disable-new-dtags"
        "-Wl,-rpath,${SPDK_ROOT}/dpdk/build/lib"
    )
else()
    message(STATUS "SPDK support disabled")
endif()


