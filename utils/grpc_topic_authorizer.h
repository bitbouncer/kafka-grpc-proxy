#include <kspp/utils/spinlock.h>
#include <kspp/cluster_config.h>
#include <set>
#include <map>
#include <string>
#include <prometheus/registry.h>
#include <prometheus/gateway.h>
#include <kspp/metrics/metrics.h>

#pragma once

class grpc_topic_authorizer {
public:
  grpc_topic_authorizer(std::shared_ptr<kspp::cluster_config> config, std::string auth_topic);

  ~grpc_topic_authorizer();

  std::pair<std::string, int>
  authorize_read(std::string api_key, std::string secret_access_key, std::string topic) const;

  std::pair<std::string, int>
  authorize_write(std::string api_key, std::string secret_access_key, std::string topic) const;

  bool authorize_api(std::string api_key) const;

private:
  void bg_thread();

  void pushgatateway_thread();

  mutable kspp::spinlock _lock;

  struct auth_data {
    int32_t id = -1;
    int32_t tenant_id = -1;
    std::string api_key;
    std::string secret_access_key_hash;
    bool read_access = false;
    bool write_access = false;
    std::string topic_name;
    std::string kafka_topic;
  };

  std::shared_ptr<kspp::cluster_config> config_;
  std::map<int32_t, auth_data> authdata_;
  std::map<std::string, int32_t> apikey2id_;
  std::map<std::string, std::shared_ptr<kspp::metric_counter>> read_metrics_;
  bool run_;
  const std::string auth_topic_;
  // we need global consumer metrics
  std::shared_ptr<prometheus::Registry> consumer_metrics_registry_;
  prometheus::Gateway gateway_;
  std::thread bg_thread_;
  std::thread pushgatateway_thread_;
};
