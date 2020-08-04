#include <string>
#include <memory>
#include <map>
#include <mutex>
#include "grpc_kafka_consumer.h"

#pragma once

class shared_kafka_consumer_factory {
public:
  shared_kafka_consumer_factory(std::shared_ptr<kspp::cluster_config> cluster_config);

  ~shared_kafka_consumer_factory();

  std::shared_ptr<bb::grpc_kafka_consumer> get_consumer(std::string topic, int partition, int64_t offset);

private:
  void bg_thread();

  mutable kspp::spinlock lock_;
  std::shared_ptr<kspp::cluster_config> cluster_config_;
  std::multimap<std::string, std::shared_ptr<bb::grpc_kafka_consumer>> cache_;
  std::thread bg_thread_;
};