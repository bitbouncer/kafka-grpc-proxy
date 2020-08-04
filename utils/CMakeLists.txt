FILE(GLOB LIB_SRCS
        *.cpp
        *.h
        )

ADD_LIBRARY(kafka-grpc-proxy-utils_s STATIC ${LIB_SRCS})
INSTALL(TARGETS kafka-grpc-proxy-utils_s
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        )



