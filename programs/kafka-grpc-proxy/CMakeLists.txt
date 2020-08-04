add_executable(kafka-grpc-proxy main.cpp)

target_link_libraries(kafka-grpc-proxy ${ALL_LIBS})

INSTALL(TARGETS kafka-grpc-proxy RUNTIME DESTINATION bin)
