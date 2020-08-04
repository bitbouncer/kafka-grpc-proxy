#include <kspp/cluster_config.h>
#include <kspp/internal/sources/kafka_consumer.h>

#pragma once

namespace bb {

  struct usage_marker {
    int64_t offset;
    int64_t ts;
  };

  class usage_marker_queue {
  public:
    usage_marker_queue() : empty_(true) {
    }

    inline size_t size() const {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      return queue_.size();
    }

    inline bool empty() const {
      return empty_;
    }

    inline void push_back(usage_marker i) {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      {
        empty_ = false;
        queue_.push_back(i);
      }
    }

    inline usage_marker front() {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      return queue_.front();
    }

    inline void pop_front() {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      {
        queue_.pop_front();
        if (queue_.size() == 0)
          empty_ = true;
      }
    }

    int64_t find_min_offset() {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      int64_t min_offset = INT64_MAX;
      for (auto i = queue_.cbegin(); i != queue_.cend(); ++i)
        min_offset = std::min(min_offset, i->offset);
      return min_offset;
    }

  private:
    std::deque<usage_marker> queue_;
    bool empty_;
    mutable kspp::spinlock spinlock_;
  };


  class grpc_kafka_consumer {
  public:
    typedef std::function<bool(RdKafka::Message *)> on_data;

    grpc_kafka_consumer(std::shared_ptr<kspp::cluster_config> cluster_config, std::string topic, int partition,
                        int64_t offset);

    ~grpc_kafka_consumer();

    void mark_offset(int64_t offset);

    inline bool contains(int64_t offset) {
      kspp::spinlock::scoped_lock xxx(offset_lock_);
      return ((offset >= begin_) && (offset <= next_offset_));
    }

    //std::shared_ptr<RdKafka::Message>  consume(int64_t offset);
    void consume(int64_t offset, on_data);

    inline int64_t get_tail_size() const {
      return tail_.size();
    }

    inline int64_t get_offset_begin() const {
      return begin_;
    }

    inline bool idle() const {
      return usage_markers_.empty();
    }

    inline int64_t get_next_offset() const {
      return next_offset_;
    }

    void mark_gc(int64_t timestamp);

    inline std::string topic() const {
      return consumer_->topic();
    }

  private:
    size_t do_gc();

    void bg_thread();

    mutable kspp::spinlock offset_lock_;
    std::mutex data_mutex_;

    std::unique_ptr<kspp::kafka_consumer> consumer_;
    std::deque<std::unique_ptr<RdKafka::Message>> tail_;

    int64_t begin_;
    int64_t next_offset_;

    bb::usage_marker_queue usage_markers_;
    bool exit_ = false;
    std::thread bg_thread_;
  };


}