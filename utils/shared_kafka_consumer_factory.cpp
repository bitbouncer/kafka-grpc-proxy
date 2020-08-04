#include <kspp/kspp.h>
#include "shared_kafka_consumer_factory.h"
#include "grpc_utils.h" // for milliseconds_since_epoch

using namespace std::chrono_literals;

shared_kafka_consumer_factory::shared_kafka_consumer_factory(std::shared_ptr<kspp::cluster_config> cluster_config)
    : cluster_config_(cluster_config), bg_thread_([this]() { bg_thread(); }) {
}

shared_kafka_consumer_factory::~shared_kafka_consumer_factory() {
  if (bg_thread_.joinable())
    bg_thread_.join();
  LOG(INFO) << "exiting with " << cache_.size() << " connections in cache";
}

std::shared_ptr<bb::grpc_kafka_consumer>
shared_kafka_consumer_factory::get_consumer(std::string topic, int partition, int64_t offset) {
  kspp::spinlock::scoped_lock xxx(lock_);
  {
    std::string key = topic + "#" + std::to_string(partition);
    auto items = cache_.equal_range(key);
    for (auto i = items.first; i != items.second; ++i) {
      if (i->second->contains(offset)) {
        LOG(INFO) << "reusing source from cache: " << i->first << " cache size: " << i->second->get_tail_size()
                  << ", offsets: " << i->second->get_offset_begin() << " - " << i->second->get_next_offset()
                  << " start offset: " << offset;
        i->second->mark_offset(offset);
        return i->second;
      }
    }
    LOG(INFO) << "creating a new streaming source, topic: " << topic << " offset: " << offset;
    auto p = std::make_shared<bb::grpc_kafka_consumer>(cluster_config_, topic, partition, offset);
    cache_.insert(std::multimap<std::string, std::shared_ptr<bb::grpc_kafka_consumer>>::value_type(key, p));
    return p;
  }
}

void shared_kafka_consumer_factory::bg_thread() {
  int64_t next_mark_gc = kspp::milliseconds_since_epoch() + 1000;
  int64_t next_check_idle = kspp::milliseconds_since_epoch() + 10000;
  int64_t next_log = kspp::milliseconds_since_epoch() + 10000;
  while (true) {
    if (kspp::milliseconds_since_epoch() > next_mark_gc) {
      kspp::spinlock::scoped_lock xxx(lock_);
      for (auto& i : cache_) {
        i.second->mark_gc(kspp::milliseconds_since_epoch() - 20000);
      }
      next_mark_gc = kspp::milliseconds_since_epoch() + 1000;
    }

    if (kspp::milliseconds_since_epoch() > next_check_idle) {
      std::vector<std::shared_ptr<bb::grpc_kafka_consumer>> to_be_killed;
      {
        kspp::spinlock::scoped_lock xxx(lock_);

        for (std::multimap<std::string, std::shared_ptr<bb::grpc_kafka_consumer>>::iterator i = cache_.begin();
             i != cache_.end();) { // note no ++i an loop
          if (i->second->idle()) {
            to_be_killed.push_back(i->second);
            i->second.reset(); // try to force map to release memory (so it's not put into allocator)
            i = cache_.erase(i);
          } else {
            ++i; // here instead ov in the loop
          }
        }
      }
      next_check_idle = kspp::milliseconds_since_epoch() + 1000;
      for (auto &i :to_be_killed) {
        LOG(INFO) << "PURGING " << i->topic() << ", usage count: " << i.use_count();
        i.reset();
      }
    }

    if (kspp::milliseconds_since_epoch() > next_log) {
      next_log = kspp::milliseconds_since_epoch() + 10000;
      size_t cache_size = 0;
      size_t cached_messages = 0;
      {
        kspp::spinlock::scoped_lock xxx(lock_);

        cache_size = cache_.size();
        for (const auto &i : cache_)
          cached_messages += i.second->get_tail_size();
      }

      LOG(INFO) << "cache contains: " << cache_size << " connections, nr_of_msg: " << cached_messages;
    }

    std::this_thread::sleep_for(100ms);
  }
}


