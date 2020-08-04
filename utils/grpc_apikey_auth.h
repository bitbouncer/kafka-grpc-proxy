#include <grpcpp/grpcpp.h>
#include "grpc_topic_authorizer.h"

#pragma once

std::string get_api_key(grpc::ServerContext &server_context);

std::string get_api_secret(grpc::ServerContext &server_context);

std::string get_metadata(grpc::ServerContext &server_context);

std::string get_forwarded_for(grpc::ServerContext &server_context);