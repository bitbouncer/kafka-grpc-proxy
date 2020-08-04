#include "grpc_kafka_consumer.h"
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <kspp/kspp.h>
#include "grpc_utils.h"

using namespace std::chrono_literals;

static boost::uuids::random_generator s_random_uuid_gen;

namespace bb {
  grpc_kafka_consumer::grpc_kafka_consumer(std::shared_ptr<kspp::cluster_config> cluster_config, std::string topic,
                                           int partition,
                                           int64_t offset)
      : begin_(offset), next_offset_(offset),
        consumer_(std::make_unique<kspp::kafka_consumer>(cluster_config, topic, partition,
                                                         boost::uuids::to_string(s_random_uuid_gen()), false)),
        bg_thread_([this]() { bg_thread(); }) {
    mark_offset(offset);
    consumer_->start(offset);
  }

  grpc_kafka_consumer::~grpc_kafka_consumer() {
    LOG(INFO) << "EXITING CONSUMER: " << consumer_->topic();
    exit_ = true;
    if (bg_thread_.joinable())
      bg_thread_.join();
  }

  void grpc_kafka_consumer::mark_offset(int64_t offset) {
    if (offset < begin_)
      LOG(ERROR) << "marking offset: " << offset << ", < BEGIN: " << begin_;
    usage_markers_.push_back({offset, kspp::milliseconds_since_epoch()});
  }

  void grpc_kafka_consumer::consume(int64_t offset, on_data f) {
    mark_offset(offset);

    std::scoped_lock xxx(data_mutex_);

    if (consumer_ == nullptr) {
      LOG(FATAL) << "oops";
    }

    // special case for BEGIN & END
    if (offset == kspp::OFFSET_BEGINNING) {
      for (auto i = tail_.cbegin(); i != tail_.cend(); ++i) {
        if (!f(i->get()))
          return;
      }
    } else if (offset == kspp::OFFSET_END || offset == kspp::OFFSET_STORED) {

    } else {
      // check if it's in the _tail
      //std::deque < std::unique_ptr < RdKafka::Message >> ::const_iterator first = _tail.cend();
      for (auto i = tail_.cend() - 1; i != tail_.cbegin() - 1; --i) {
        if ((*i)->offset() < offset) {
          for (auto j = i + 1; j != tail_.cend(); ++j) {
            if (!f(j->get()))
              return;
          }
          break;
        }
      }
    }

    bool want_more = true;
    while (want_more) {
      auto p = consumer_->consume(0);
      if (p) {
        {
          //LOG(INFO) << "got kafka msg " << p->offset();
          kspp::spinlock::scoped_lock xxx(offset_lock_);
          next_offset_ = p->offset() + 1;
        }
        want_more = f(p.get());
        tail_.push_back(std::move(p));
      } else {
        break;
      }
    }
  }

  void grpc_kafka_consumer::mark_gc(int64_t timestamp) {
    // get rid of old data
    while (!usage_markers_.empty() && usage_markers_.front().ts < timestamp) {
      usage_markers_.pop_front();
    }
    // if there are nothing in the queue will will marke with INT64_MAX which will leat to this being unavailable for usage and eventually resycled
    begin_ = usage_markers_.find_min_offset();
  }


  //it seems to be problematic to release memory from deque
  //https://stackoverflow.com/questions/1242357/how-to-release-memory-from-stddeque
  size_t grpc_kafka_consumer::do_gc() {
    size_t elements = 0;
    std::scoped_lock xxx(data_mutex_);
    while (tail_.size() && tail_[0]->offset() < begin_) {
      tail_.front().reset();
      tail_.pop_front();
      ++elements;
    }
    tail_.shrink_to_fit();
    return elements;
  }

  void grpc_kafka_consumer::bg_thread() {
    int64_t next_do_gc = kspp::milliseconds_since_epoch() + 1000;
    while (!_exit) {
      if (kspp::milliseconds_since_epoch() > next_do_gc) {
        auto count = do_gc();
        next_do_gc = kspp::milliseconds_since_epoch() + 1000;
      }
      std::this_thread::sleep_for(100ms);
    }
  }
}