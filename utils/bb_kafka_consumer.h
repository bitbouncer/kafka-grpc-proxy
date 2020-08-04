#include <chrono>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include <kspp/cluster_config.h>

#pragma once

namespace bb {
  // consumes many partitions to same bb_kafka_source
  class bb_kafka_consumer {
  public:
    bb_kafka_consumer(std::shared_ptr<kspp::cluster_config> config, std::string topic, std::vector<int32_t> partitions,
                      std::string consumer_group, bool check_cluster = true);

    bb_kafka_consumer(std::shared_ptr<kspp::cluster_config> config, std::string topic, std::string partitions,
                      std::string consumer_group, bool check_cluster = true);

    ~bb_kafka_consumer();

    void close();

    std::unique_ptr<RdKafka::Message> consume(int librdkafka_timeout = 0);

    inline bool eof() const {
      bool eof = true;
      for (const auto &p : partition_data_)
        eof &= p.eof;
      return eof;
    }

    inline std::string topic() const {
      return topic_;
    }

    void start(int64_t offset);

    void stop();

    //int32_t commit(int64_t offset, bool flush = false);

    /*inline int64_t commited() const {
      return _can_be_committed;
    }
    */

    int update_eof();

    bool consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const;

  private:
    class MyEventCb : public RdKafka::EventCb {
    public:
      void event_cb(RdKafka::Event &event);
    };

    struct partition_data {
      int32_t partition_id = -1;
      int32_t partition_index = -1;
      int64_t can_be_committed = 0;
      int64_t last_committed = 0;
      size_t max_pending_commits = 0;
      bool eof = true;
      int64_t low = 0;
      int64_t high = 0;
    };

    std::shared_ptr<kspp::cluster_config> config_;
    const std::string topic_;
    std::vector<partition_data> partition_data_;
    std::vector<int32_t> partition2index_;
    const std::string partition_string_;
    const std::string consumer_group_;
    std::vector<RdKafka::TopicPartition *> topic_partitions_;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    //std::vector<int64_t>                    _can_be_committed;
    //std::vector<int64_t>                    _last_committed;
    //size_t                                  _max_pending_commits;
    uint64_t msg_cnt_;    // TODO move to metrics
    uint64_t msg_bytes_;  // TODO move to metrics
    bool closed_;
    MyEventCb event_cb_;
  };
}

