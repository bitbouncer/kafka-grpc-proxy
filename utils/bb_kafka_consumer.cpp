#include "bb_kafka_consumer.h"
#include <librdkafka/rdkafka.h> // for stuff that only exists in c code (in rdkafka), must be included first
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/internal/rd_kafka_utils.h>
#include <kspp/kspp.h>
#include <kspp/cluster_metadata.h>

using namespace std::chrono_literals;
namespace bb {
  void bb_kafka_consumer::MyEventCb::event_cb(RdKafka::Event &event) {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        LOG(ERROR) << RdKafka::err2str(event.err()) << " " << event.str();
        //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) TODO
        //  run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        LOG(INFO) << "STATS: " << event.str();
        break;

      case RdKafka::Event::EVENT_LOG:
        LOG(INFO) << event.fac() << ", " << event.str();
        break;

      default:
        LOG(INFO) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str();
        break;
    }
  }

  bb_kafka_consumer::bb_kafka_consumer(std::shared_ptr<kspp::cluster_config> config, std::string topic,
                                       std::vector<int32_t> partitions, std::string consumer_group, bool check_cluster)
      : config_(config), topic_(topic), partition_string_(kspp::partition_list_to_string(partitions)),
        consumer_group_(consumer_group), msg_cnt_(0), msg_bytes_(0), closed_(false) {
    int32_t max_partition = 0;
    auto max_item = std::max_element(partitions.begin(), partitions.end());
    if (max_item != partitions.end())
      max_partition = *max_item;

    partition2index_.resize(max_partition + 1, -1);

    int index = 0;
    for (auto i : partitions) {
      partition_data_.push_back({i, index, 0, 0, 0, 0, 0});
      partition2index_[index] = i;
      index++;
    }

    // really try to make sure the partition & group exist before we continue

    if (check_cluster) {
      for (const auto &i : partition_data_)
        LOG_IF(FATAL, config->get_cluster_metadata()->wait_for_topic_partition(topic, i.partition_id,
                                                                               config->get_cluster_state_timeout()) ==
                      false) << "failed to wait for topic leaders, topic:" << topic << ":" << i.partition_id;
      //wait_for_partition(_consumer.get(), _topic, _partition);
      //kspp::kafka::wait_for_group(brokers, consumer_group); something seems to wrong in rdkafka master.... TODO
    }

    for (const auto &i : partition_data_)
      topic_partitions_.push_back(RdKafka::TopicPartition::create(topic_, i.partition_id));

    /*
     * Create configuration objects
    */
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    /*
    * Set configuration properties
    */
    try {
      set_broker_config(conf.get(), config_.get());

      //set_config(conf.get(), "api.version.request", "true");
      set_config(conf.get(), "socket.nagle.disable", "true");
      set_config(conf.get(), "fetch.wait.max.ms", std::to_string(config_->get_consumer_buffering_time().count()));
      //set_config(conf.get(), "queue.buffering.max.ms", "100");
      //set_config(conf.get(), "socket.blocking.max.ms", "100");
      set_config(conf.get(), "enable.auto.commit", "false");
      set_config(conf.get(), "auto.commit.interval.ms", "5000"); // probably not needed
      set_config(conf.get(), "enable.auto.offset.store", "false");
      set_config(conf.get(), "group.id", consumer_group_);
      set_config(conf.get(), "enable.partition.eof", "true");
      set_config(conf.get(), "log.connection.close", "false");
      set_config(conf.get(), "max.poll.interval.ms", "86400000"); // max poll interval before leaving consumer group
      set_config(conf.get(), "event_cb", &event_cb_);
      //set_config(conf.get(), "socket.max.fails", "1000000");
      //set_config(conf.get(), "message.send.max.retries", "1000000");// probably not needed

      // following are topic configs but they will be passed in default_topic_conf to broker config.
      std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
      set_config(tconf.get(), "auto.offset.reset", "earliest");

      set_config(conf.get(), "default_topic_conf", tconf.get());
    }
    catch (std::invalid_argument &e) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", bad config " << e.what();
    }

    /*
    * Create consumer using accumulated global configuration.
    */
    std::string errstr;
    consumer_ = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_string_
                 << ", failed to create consumer, reason: " << errstr;
    }
    LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", created";
  }

  bb_kafka_consumer::bb_kafka_consumer(std::shared_ptr<kspp::cluster_config> config, std::string topic,
                                       std::string partitions, std::string consumer_group, bool check_cluster) :
      bb_kafka_consumer(config, topic, kspp::kafka::get_partition_list(config, topic, partitions), consumer_group,
                        check_cluster) {
  }

  bb_kafka_consumer::~bb_kafka_consumer() {
    if (!closed_)
      close();
    for (auto i : topic_partitions_) // should be exactly 1
      delete i;
    LOG(INFO) << "consumer deleted";
  }

  void bb_kafka_consumer::close() {
    if (closed_)
      return;
    closed_ = true;
    if (consumer_) {
      consumer_->close();
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", closed - consumed " << msg_cnt_
                << " messages (" << msg_bytes_ << " bytes)";
    }
    consumer_.reset(nullptr);
  }

  void bb_kafka_consumer::start(int64_t offset) {
    if (offset == kspp::OFFSET_STORED) {
      //just make shure we're not in for any surprises since this is a runtime variable in rdkafka...
      assert(kspp::OFFSET_STORED == RdKafka::Topic::OFFSET_STORED);

      if (consumer_group_exists(consumer_group_, 5s)) {
        DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_string_ << " consumer group: "
                   << consumer_group_ << " starting from OFFSET_STORED";
      } else {
        //non existing consumer group means start from beginning
        LOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_string_ << " consumer group: "
                  << consumer_group_ << " missing -> starting from OFFSET_BEGINNING";
        offset = kspp::OFFSET_BEGINNING;
      }
    } else if (offset == kspp::OFFSET_BEGINNING) {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_string_ << " consumer group: "
                 << consumer_group_ << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_string_ << " consumer group: "
                 << consumer_group_ << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_string_ << " consumer group: "
                 << consumer_group_ << " starting from fixed offset: " << offset;
    }

    /*
    * Subscribe to topics
    */
    for (auto &i : topic_partitions_)
      i->set_offset(offset);

    RdKafka::ErrorCode err0 = consumer_->assign(topic_partitions_);
    if (err0) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", failed to subscribe, reason:"
                 << RdKafka::err2str(err0);
    }

    update_eof();
  }

  void bb_kafka_consumer::stop() {
    if (consumer_) {
      RdKafka::ErrorCode err = consumer_->unassign();
      if (err) {
        LOG(FATAL) << "kafka_consumer::stop topic:"
                   << topic_
                   << ":"
                   << partition_string_
                   << ", failed to stop, reason:"
                   << RdKafka::err2str(err);
      }
    }
  }

  int bb_kafka_consumer::update_eof() {
    for (auto &i : partition_data_) {
      i.eof = false;
      i.low = 0;
      i.high = 0;
      RdKafka::ErrorCode ec = consumer_->query_watermark_offsets(topic_, i.partition_id, &i.low, &i.high, 1000);
      if (ec) {
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << i.partition_id
                   << ", consumer.query_watermark_offsets failed, reason:" << RdKafka::err2str(ec);
        return ec;
      }
      if (i.low == i.high) {
        i.eof = true;
        LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << i.partition_id << " [empty], eof at:" << i.high;
      }
    }

    auto ec = consumer_->position(topic_partitions_);
    if (ec) {
      LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_string_
                 << ", consumer.position failed, reason:" << RdKafka::err2str(ec);
      return ec;
    }

    for (auto &i : partition_data_) {
      auto cursor = topic_partitions_[i.partition_id]->offset();
      i.eof = (cursor + 1 == i.high);
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << i.partition_id << " cursor: " << cursor << ", eof at:"
                << i.high;
    }

    return 0;
  }

  std::unique_ptr<RdKafka::Message> bb_kafka_consumer::consume(int librdkafka_timeout) {
    if (closed_ || consumer_ == nullptr) {
      LOG(ERROR) << "topic:" << topic_ << ":" << partition_string_ << ", consume failed: closed()";
      return nullptr; // already closed
    }

    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(librdkafka_timeout));

    switch (msg->err()) {
      case RdKafka::ERR_NO_ERROR: {
        int32_t partition = msg->partition();
        assert(partition < partition2index_.size());
        auto index = partition2index_[partition];
        partition_data_[index].eof = false;
        msg_cnt_++;
        msg_bytes_ += msg->len() + msg->key_len();
      }
        return msg;


      case RdKafka::ERR__TIMED_OUT:
        break;

      case RdKafka::ERR__PARTITION_EOF: {
        int32_t partition = msg->partition();
        assert(partition < partition2index_.size());
        auto index = partition2index_[partition];
        partition_data_[index].eof = false;
      }
        break;

      case RdKafka::ERR__UNKNOWN_TOPIC:
      case RdKafka::ERR__UNKNOWN_PARTITION:
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", consume failed: "
                   << msg->errstr();
        break;

      default:
        /* Errors */
        //_eof = true;
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_string_ << ", consume failed: "
                   << msg->errstr();
    }
    return nullptr;
  }

  // TBD add time based autocommit
  /*
   * int32_t bb_kafka_consumer::commit(int64_t offset, bool flush) {
    if (offset < 0) // not valid
      return 0;

    // you should actually write offset + 1, since a new consumer will start at offset.
    offset = offset + 1;

    if (offset <= _last_committed) // already done
    {
      return 0;
    }

    if (_closed || _consumer == nullptr) {
      LOG(ERROR) << "kafka_consumer topic:" << _topic << ":" << _partition << ", consumer group: " << _consumer_group << ", commit on closed consumer, lost " << offset - _last_committed << " messsages";
      return -1; // already closed
    }

    _can_be_committed = offset;
    RdKafka::ErrorCode ec = RdKafka::ERR_NO_ERROR;
    if (flush) {
      LOG(INFO) << "kafka_consumer topic:" << _topic << ":" << _partition << ", consumer group: " << _consumer_group << ", commiting(flush) offset:" << _can_be_committed;
      _topic_partition[0]->set_offset(_can_be_committed);
      ec = _consumer->commitSync(_topic_partition);
      if (ec == RdKafka::ERR_NO_ERROR) {
        _last_committed = _can_be_committed;
      } else {
        LOG(ERROR) << "kafka_consumer topic:" << _topic << ":" << _partition << ", consumer group: " << _consumer_group <<  ", failed to commit, reason:" << RdKafka::err2str(ec);
      }
    } else if ((_last_committed + _max_pending_commits) < _can_be_committed) {
      DLOG(INFO) << "kafka_consumer topic:" << _topic << ":" << _partition << ", consumer group: " << _consumer_group << ", lazy commit: offset:" << _can_be_committed;
      _topic_partition[0]->set_offset(_can_be_committed);
      ec = _consumer->commitAsync(_topic_partition);
      if (ec == RdKafka::ERR_NO_ERROR) {
        _last_committed = _can_be_committed; // not done yet but promised to be written on close...
      } else {
        LOG(ERROR) << "kafka_consumer topic:" << _topic << ":" << _partition << ", consumer group: " << _consumer_group << ", failed to commit, reason:" << RdKafka::err2str(ec);
      }
    }
    return ec;
  }
   */

  //virtual ErrorCode metadata (bool all_topics, const Topic *only_rkt,  Metadata **metadatap, int timeout_ms) = 0;

  bool bb_kafka_consumer::consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const {
    char errstr[128];
    auto expires = kspp::milliseconds_since_epoch() + 1000 * timeout.count();
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    const struct rd_kafka_group_list *grplist = nullptr;

    /* FIXME: Wait for broker to come up. This should really be abstracted by librdkafka. */
    do {
      if (expires < kspp::milliseconds_since_epoch()) {
        return false;
      }
      if (err) {
        DLOG(ERROR) << "retrying group list in 1s, ec: " << rd_kafka_err2str(err);
        std::this_thread::sleep_for(1s);
      } else if (grplist) {
        // the previous call must have succeded bu returned an empty list -
        // bug in rdkafka when using ssl - we cannot separate this from a non existent group - we must retry...
        LOG_IF(FATAL, grplist->group_cnt != 0) << "group list should be empty";
        rd_kafka_group_list_destroy(grplist);
        LOG(INFO) << "got empty group list - retrying in 1s";
        std::this_thread::sleep_for(1s);
      }

      err = rd_kafka_list_groups(consumer_->c_ptr(), consumer_group.c_str(), &grplist, 1000);

      DLOG_IF(INFO, err != 0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: " << err;
      DLOG_IF(INFO, err == 0)
              << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: OK" << " grplist->group_cnt: "
              << grplist->group_cnt;
    } while (err == RD_KAFKA_RESP_ERR__TRANSPORT || err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS ||
             err == RD_KAFKA_RESP_ERR__PARTIAL);

    if (err) {
      LOG(ERROR) << "failed to retrieve groups, ec: " << rd_kafka_err2str(err);
      return false;
      //throw std::runtime_error(rd_kafka_err2str(err));
    }
    bool found = (grplist->group_cnt > 0);
    rd_kafka_group_list_destroy(grplist);
    return found;
  }
} // namespace