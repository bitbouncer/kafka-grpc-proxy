cmake_minimum_required(VERSION 2.8.11)
project(kafka-grpc-proxy)

SET(CMAKE_CXX_STANDARD 17)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

#GLOBAL DEFINITIONS
add_definitions(-DRAPIDJSON_HAS_STDSTRING)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wpedantic -Wreturn-type -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wpedantic -Wreturn-type -g -O0")

#find_package(Boost COMPONENTS program_options system date_time REQUIRED)
#set(BOOST_LIBS
#        ${Boost_PROGRAM_OPTIONS_LIBRARY}
#        ${Boost_SYSTEM_LIBRARY}
#        ${Boost_DATE_TIME}
#        boost_date_time
#        pthread
#        rt
#        c
#        )
set(BOOST_LIBS boost_program_options boost_iostreams boost_regex boost_system z bz2 pthread rt c)

# avro + schema registry
find_package(rapidjson REQUIRED)
find_package(AvroCPP REQUIRED)

find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")
find_package(GRPC REQUIRED)

SET(PROTOS 
  ./proto/bb_streaming.proto
)

SET(PROMETHEUS_PROTOS
        ./proto/remote.proto
        ./proto/types.proto
        ./proto/gogo.proto)

set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR})

PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTOS} ${PROMETHEUS_PROTOS})

# Print path to generated files
message("PROTO_SRCS = ${PROTO_SRCS}")
message("PROTO_HDRS = ${PROTO_HDRS}")

grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_SRC_DIR} ${PROTOS})
message("GRPC_SRCS = ${GRPC_SRCS}")
message("GRPC_HDRS = ${GRPC_HDRS}")


find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})


ADD_LIBRARY(kafka-grpc-proxy-proto_s STATIC ${PROTO_SRCS} ${GRPC_SRCS})
INSTALL(TARGETS kafka-grpc-proxy-proto_s
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
)

#include protobuf generated files
include_directories(${CMAKE_CURRENT_BINARY_DIR})

SET(EXT_INCLUDE_PATH
        ${Boost_INCLUDE_DIR}
        ${RAPIDJSON_INCLUDE_DIR}
        ${AVRO_INCLUDE_DIRS}
        )
SET(DEV_LIBRARY_PATH
        )

SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-Wno-deprecated)
add_definitions(-g)

SET(AVRO_LIBS avrocpp curl snappy)
SET(LIBRDKAFKA_LIBS rdkafka++ rdkafka)
SET(PROMETHEUS_CPP_LIBS prometheus-cpp-core prometheus-cpp-push)
SET(BB_GRPC_LIBS_STATIC kafka-grpc-proxy-proto_s gRPC::grpc++_reflection gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
SET(EXT_LIBS kspp_s ${AVRO_LIBS} ${LIBRDKAFKA_LIBS} ${PROMETHEUS_CPP_LIBS} ${BOOST_LIBS} ${BB_GRPC_LIBS_STATIC} cares glog gflags crypto ssl stdc++fs dl)
SET(ALL_LIBS kafka-grpc-proxy-utils_s ${EXT_LIBS})

include_directories(${EXT_INCLUDE_PATH} ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/include)
link_directories(${DEV_LIBRARY_PATH})

add_subdirectory(utils)
add_subdirectory(programs)






