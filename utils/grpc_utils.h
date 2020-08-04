#include <grpcpp/grpcpp.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/map.h>

#pragma once

void bb_set_channel_args(grpc::ChannelArguments &channelArgs);

void bb_set_channel_args(grpc::ServerBuilder &serverBuilder);
