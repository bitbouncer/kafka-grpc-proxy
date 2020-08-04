#include "grpc_utils.h"

void bb_set_channel_args(grpc::ChannelArguments &channelArgs) {
  channelArgs.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, 1);

  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 20000);
  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 20000);
  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

  channelArgs.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0); // unlimited
  //channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000); // not applicable for client
  channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 20000);
}

void bb_set_channel_args(grpc::ServerBuilder &serverBuilder) {
  serverBuilder.AddChannelArgument(GRPC_ARG_HTTP2_BDP_PROBE, 1);
  serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 20000);
  //serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 3600000);  // 1h
  serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 20000); // wait 10 s for reply
  serverBuilder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

  serverBuilder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0); // unlimited
  serverBuilder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
  serverBuilder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
  serverBuilder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0); // unlimited

  //serverBuilder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_MED);
}
