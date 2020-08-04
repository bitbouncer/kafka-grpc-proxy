#include "grpc_topic_authorizer.h"
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/visitor.h>
#include <kspp/utils/kafka_utils.h>
#include "md5.h"

using namespace std::chrono_literals;

static std::string pushgateway_hostname_part(std::string s) {
  return s.substr(0, s.find(":"));
}

static std::string pushgateway_port_part(std::string s) {
  auto i = s.find(":");
  if (i == std::string::npos)
    return "9091";
  return s.substr(i + 1);
}

grpc_topic_authorizer::grpc_topic_authorizer(std::shared_ptr<kspp::cluster_config> config, std::string auth_topic)
    : config_(config), auth_topic_(auth_topic), run_(false),
      consumer_metrics_registry_(std::make_shared<prometheus::Registry>()),
      gateway_(pushgateway_hostname_part(config->get_pushgateway_uri()),
               pushgateway_port_part(config->get_pushgateway_uri()), "bb_monitor"),
      bg_thread_([this]() { bg_thread(); }), pushgatateway_thread_([this]() { pushgatateway_thread(); }) {
  run_ = true;
}

grpc_topic_authorizer::~grpc_topic_authorizer() {
  run_ = false;
  bg_thread_.join();
  pushgatateway_thread_.join();
}

std::pair<std::string, int>
grpc_topic_authorizer::authorize_read(std::string api_key, std::string secret_access_key, std::string topic) const {
  std::string secret_access_key_hash = md5(secret_access_key);
  auto hash = md5(topic + "#" + api_key + "#" + secret_access_key_hash);

  kspp::spinlock::scoped_lock xxx(_lock);
  {
    auto item = apikey2id_.find(hash);
    if (item == apikey2id_.end()) {
      LOG(INFO) << "auth failed for apikey: " << api_key << ", topic:" << topic;
      return std::pair<std::string, int>("", 0);
    }

    auto item2 = authdata_.find(item->second);
    if (item2 == authdata_.end()) {
      LOG(ERROR) << "auth lookup failed for apikey: " << api_key << " should never happen";
      return std::pair<std::string, int>("", 0);
    }

    // we can have an old auth in index table so we need to double check that the secret is valid
    if (item2->second.secret_access_key_hash.size() > 0 &&
        item2->second.secret_access_key_hash != secret_access_key_hash) {
      LOG(ERROR) << "wrong api secret: " << secret_access_key;
      return std::pair<std::string, int>("", 0);
    }

    if (topic != item2->second.topic_name) {
      LOG(ERROR) << "found the wrong topic";
      return std::pair<std::string, int>("", 0);
    }

    if (!item2->second.read_access) {
      LOG(ERROR) << "no read access for apikey: " << api_key << ", topic:" << topic;
      return std::pair<std::string, int>("", 0);
    }

    return std::pair<std::string, int>(item2->second.kafka_topic, item2->second.tenant_id);
  }
}

std::pair<std::string, int>
grpc_topic_authorizer::authorize_write(std::string api_key, std::string secret_access_key, std::string topic) const {
  std::string secret_access_key_hash = md5(secret_access_key);
  auto hash = md5(topic + "#" + api_key + "#" + secret_access_key_hash);

  kspp::spinlock::scoped_lock xxx(_lock);
  {
    auto item = apikey2id_.find(hash);
    if (item == apikey2id_.end()) {
      LOG(INFO) << "auth failed for apikey: " << api_key << ", topic:" << topic;
      return std::pair<std::string, int>("", 0);
    }

    auto item2 = authdata_.find(item->second);
    if (item2 == authdata_.end()) {
      LOG(ERROR) << "auth lookup failed for apikey: " << api_key << " should never happen";
      return std::pair<std::string, int>("", 0);
    }

    // we can have an old auth in index table so we need to double check that the secret is valid
    if (item2->second.secret_access_key_hash.size() > 0 &&
        item2->second.secret_access_key_hash != secret_access_key_hash) {
      LOG(ERROR) << "wrong api secret: " << secret_access_key;
      return std::pair<std::string, int>("", 0);
    }

    if (topic != item2->second.topic_name) {
      LOG(ERROR) << "found the wrong topic";
      return std::pair<std::string, int>("", 0);
    }

    if (!item2->second.write_access) {
      LOG(ERROR) << "no write access for apikey: " << api_key << ", topic:" << topic;
      return std::pair<std::string, int>("", 0);
    }

    return std::pair<std::string, int>(item2->second.kafka_topic, item2->second.tenant_id);
  }
}

bool grpc_topic_authorizer::authorize_api(std::string api_key) const {
  return true;
}

void grpc_topic_authorizer::bg_thread() {
  kspp::topology_builder builder(config_);
  auto partitions = kspp::kafka::get_number_partitions(config_, auth_topic_);
  auto partition_list = kspp::get_partition_list(partitions);

  auto topology = builder.create_topology();
  auto sources0 = topology->create_processors<kspp::kafka_source<void, kspp::generic_avro, void, kspp::avro_serdes>>(
      partition_list, auth_topic_, config_->avro_serdes());
  auto parsed0 = topology->create_processors<kspp::visitor<void, kspp::generic_avro>>(sources0, [this](
      const kspp::krecord<void, kspp::generic_avro> &in) {
    try {
      auto record = in.value()->record();
      auto id = record.get_optional<int32_t>("id");
      auto tenant_id = record.get_optional<int32_t>("tenant_id");
      auto is_disabled = record.get_optional<bool>("is_disabled");
      auto is_deleted = record.get_optional<bool>("is_deleted");
      auto api_key = record.get_optional<std::string>("api_key");
      auto secret_access_key_hash = record.get_optional<std::string>("secret_access_key_hash");
      auto read_access = record.get_optional<bool>("read_access");
      auto write_access = record.get_optional<bool>("write_access");
      auto topic_name = record.get_optional<std::string>("topic_name");
      auto kafka_topic = record.get_optional<std::string>("kafka_topic");

      if (!id || !tenant_id || !is_disabled || !is_deleted || !api_key || !secret_access_key_hash)
        return;

      bool delete_me = false;
      if (is_disabled && *is_disabled)
        delete_me = true;
      if (is_deleted && *is_deleted)
        delete_me = true;

      auto hash = md5(*topic_name + "#" + *api_key + "#" + *secret_access_key_hash);

      kspp::spinlock::scoped_lock xxx(_lock);

      if (delete_me) {
        auto item = authdata_.find(*id);
        if (item != authdata_.end()) {
          LOG(WARNING) << "accound disabled but existing connections not killed id:" << *id;
          std::string topic_and_api_key = item->second.topic_name + "#" + item->second.api_key;
          apikey2id_.erase(topic_and_api_key);
          authdata_.erase(item);
          // TODO kill active connections
        }

        return;
      }

      auth_data ad;
      ad.id = *id;
      ad.tenant_id = *tenant_id;
      ad.api_key = *api_key;
      ad.secret_access_key_hash = *secret_access_key_hash;
      ad.read_access = *read_access;
      ad.write_access = *write_access;
      if (topic_name)
        ad.topic_name = *topic_name;
      if (kafka_topic)
        ad.kafka_topic = *kafka_topic;

      std::string access;
      if (ad.read_access)
        access += "r";
      if (ad.write_access)
        access += "w";

      //LOG(INFO) << "tenant: " << ad.tenant_id << ad.api_key << " " << ad.topic_name << " -> " << ad.kafka_topic << ", access: " << access;

      authdata_[*id] = ad;
      apikey2id_[hash] = *id;

      {
        std::string id = std::to_string(*tenant_id) + "#" + *topic_name;
        auto m = read_metrics_.find(id);
        if (m == read_metrics_.end()) {
          std::map<std::string, std::string> labels = {
              {"processor", "SOURCE"},
              {"client_id", std::to_string(*tenant_id)}
          };
          auto sm = std::make_shared<kspp::metric_counter>("processed", "msg", labels, consumer_metrics_registry_);
          //_gateway.RegisterCollectable(sm, sm->_labels)
        }
      }
    }
    catch (std::exception &e) {
      LOG(ERROR) << "not my kind of avro" << e.what();
    }
  });

  topology->start(kspp::OFFSET_BEGINNING);
  topology->flush();

  LOG(INFO) << "auth data loaded " << apikey2id_.size() << " topics/apikeys active";

  while (run_) {
    auto c = topology->process_1s();
    if (!c)
      std::this_thread::sleep_for(1000ms);
  }
}

void grpc_topic_authorizer::pushgatateway_thread() {
  int64_t next_time_to_send = kspp::milliseconds_since_epoch() + 10 * 1000;
  while (run_) {
    //time for report
    if (next_time_to_send <= kspp::milliseconds_since_epoch()) {
      gateway_.Push();
      //schedule nex reporting event
      next_time_to_send += 10000;
      // if we are reaaly out of sync lets sleep at least 10 more seconds
      if (next_time_to_send <= kspp::milliseconds_since_epoch())
        next_time_to_send = kspp::milliseconds_since_epoch() + 10000;
    }
    std::this_thread::sleep_for(500ms);
  } // while
}
