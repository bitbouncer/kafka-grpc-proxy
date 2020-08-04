#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/cluster_config.h>
#include <bb_streaming.grpc.pb.h>
#include <utils/grpc_utils.h>
#include <utils/grpc_topic_authorizer.h>
#include <utils/grpc_apikey_auth.h>
#include "call_data.h"
#include <utils/shared_kafka_consumer_factory.h>

#pragma once

#define RPC_LIFETIME     15000
#define RPC_MAX_MESSAGES 10000
#define RPC_MAX_SIZE     10 * 1000 *1000

using namespace std::chrono_literals;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using bitbouncer::streaming::SubscriptionRequest;
using bitbouncer::streaming::SubscriptionData;
using bitbouncer::streaming::streamprovider;

// Class encompasing the state and logic needed to serve a request.
class subscribe_call_data : public call_data {
public:
  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  subscribe_call_data(streamprovider::AsyncService *service, ServerCompletionQueue *cq,
                      std::shared_ptr<shared_kafka_consumer_factory> cache, std::shared_ptr<grpc_topic_authorizer> auth)
      : service_(service), cq_(cq), stream_(&ctx_), _status(CREATE), _cache(cache), _authorizer(auth),
        _isCancelled(ctx_), _bg_thread([this]() { bg_thread(); }) {
    ctx_.AsyncNotifyWhenDone(&_isCancelled);
    Proceed(true);
  }

  bool isCancelled() const { return _isCancelled.isCancelled; }

  virtual ~subscribe_call_data() {
    int64_t t1 = kspp::milliseconds_since_epoch();
    if (_bg_thread.joinable())
      _bg_thread.join();
    if (_source) {
      LOG(INFO) << log_header("CLOSING") << "exported: " << _messages_written << ", messages in: " << (t1 - _t0) / 1000
                << "s";
    }
  }

  void Proceed(bool ok) override {
    if (_status == CREATE) {
      // Make this instance progress to the PROCESS state.
      _status = START;
      service_->RequestSubscribe(&ctx_, &request_, &stream_, cq_, cq_, this);
    } else if (_status == START) {
      // Spawn a new CallData instance to serve new clients while we process
      // the one for this CallData. The instance will deallocate itself as
      // part of its FINISH state.
      new subscribe_call_data(service_, cq_, _cache, _authorizer);

      std::string api_key = get_api_key(ctx_);
      std::string api_secret = get_api_secret(ctx_);
      //LOG(INFO) << "headers: " << get_metadata(ctx_);
      _source_ip = get_forwarded_for(ctx_);
      //ctx_.peer();

      auto auth_res = _authorizer->authorize_read(api_key, api_secret, request_.topic());
      _client_id = auth_res.second;

      if (auth_res.second == 0) {
        LOG(WARNING) << log_header("PERMISSION_DENIED") << "api_key: " << api_key;
        Status authentication_error(grpc::StatusCode::UNAUTHENTICATED, "authentication failed for key: " + api_key);
        stream_.Finish(authentication_error, this);
        _status = FINISH;
        return;
      }

      _t0 = kspp::milliseconds_since_epoch();
      _end_of_life_ts = kspp::milliseconds_since_epoch() + RPC_LIFETIME;
      _real_topic = auth_res.first;
      _source = _cache->get_consumer(_real_topic, request_.partition(), request_.offset());
      _cursor = request_.offset();

      LOG(WARNING) << log_header("START STREAMING") << _real_topic << ", offset: " << request_.offset();
      _pending_msg = false;
      _status = VALIDATE;
      _next_eof_message = kspp::milliseconds_since_epoch() + 4000;  // give kafka some time
    } else if (_status == VALIDATE) {
      // we should never get here
    } else if (_status == PROCESS) {
      if (isCancelled()) {
        LOG(INFO) << log_header("CANCELLED") << "ok = " << ok;
        _status = FINISH;
        stream_.Finish(Status(grpc::StatusCode::CANCELLED, "closed"), this);
        return;
      }

      if (kspp::milliseconds_since_epoch() > _end_of_life_ts) {
        LOG(INFO) << log_header("EOL") << " finishing OK";
        _status = FINISH;
        stream_.Finish(Status(), this);
        return;
      }

      // this a try to fix backpreassure - do not stream to much to client that cannot handle it.
      if (_aprox_bytes_written > RPC_MAX_SIZE) {
        LOG(INFO) << log_header("RPC_MAX_SIZE reached") << " finishing OK";
        _status = FINISH;
        stream_.Finish(Status(), this);
        return;
      }

      try_forward_messages();
    } else {
      GPR_ASSERT(_status == FINISH);
      // Once in the FINISH state, deallocate ourselves (CallData).
      delete this;
    }
  }

private:
  std::string log_header(std::string event) {
    std::string s = event + ", tenant: " + std::to_string(_client_id) + ", topic: " + request_.topic() + ":" +
                    std::to_string(request_.partition()) + ", ip: " + _source_ip + " ";
    return s;
  }

  void try_forward_messages() {
    if (_status != PROCESS) {
      _pending_msg = false;
      return;
    }

    reply_.clear_data();
    size_t approx_msg_size = 0;
    reply_.set_eof(false);
    _source->consume(_cursor, [this, &approx_msg_size](RdKafka::Message *msg) {
      if (!msg) {
        reply_.set_eof(true);
        return false;
      }
      SubscriptionData *record = reply_.add_data();
      //is key avro needs minumum 5 bytes and leading 0
      auto key_sz = msg->key_len();
      if (key_sz > 4 && msg->key()->data()[0] == 0) {
        int32_t encoded_schema_id = -1;
        memcpy(&encoded_schema_id, &msg->key()->data()[1], 4);
        int32_t schema_id = ntohl(encoded_schema_id);
        record->set_key_schema(schema_id);
        record->set_key(msg->key()->substr(5));
      } else {
        record->set_key_schema(0);
        if (key_sz)
          record->set_key(*msg->key());
      }

      auto val_sz = msg->len();
      if (val_sz > 4 && ((const char *) msg->payload())[0] == 0) {
        int32_t encoded_schema_id = -1;
        memcpy(&encoded_schema_id, &((const char *) msg->payload())[1], 4);
        int32_t schema_id = ntohl(encoded_schema_id);
        record->set_value_schema(schema_id);
        record->set_value(&((const char *) msg->payload())[5], msg->len() - 5);
      } else {
        record->set_value_schema(0);
        if (val_sz)
          record->set_value((const char *) msg->payload(), msg->len());
      }
      record->set_offset(msg->offset());
      record->set_timestamp(msg->timestamp().timestamp);
      approx_msg_size += key_sz + val_sz;
      _aprox_bytes_written += key_sz + val_sz;
      _cursor = msg->offset() + 1;

      // there's a max size of grpc of 4MB - lets keep less than that
      // kafka has a max size of 1MB so stop well before that
      if (approx_msg_size > 2500000)
        return false;

      return true;
    });

    _messages_written += reply_.data_size();

    if (reply_.data_size() == 0 && _next_eof_message < kspp::milliseconds_since_epoch()) {
      // emit eof once per 4s
      // we could check other status from consumer as well - if no topic / no leader etc
      ++_messages_written; // should we count this empty thing??
      _next_eof_message = kspp::milliseconds_since_epoch() + 4000;
      _pending_msg = true;
      stream_.Write(reply_, this);
      return;
    }

    if (reply_.data_size()) {
      _pending_msg = true;
      stream_.Write(reply_, this);
      return;
    }

    _pending_msg = false;
    return;
  }

  //tries to wakeup normal grpc processing
  void bg_thread() {
    //LOG(INFO) << "bg_thread init";
    while (_status != VALIDATE && _status != FINISH) {
      std::this_thread::sleep_for(500ms);
    }

    //LOG(INFO) << "bg_thread starting validate";
    while (_status == VALIDATE && _status != FINISH) {
      std::this_thread::sleep_for(500ms);
      //TODO add validate precondition for require topic leaders
      if (false) {
        stream_.Finish(Status(grpc::StatusCode::FAILED_PRECONDITION, "topic leader not found"), this);
        _status = FINISH;
      } else {
        _status = PROCESS;
        if (request_.max_poll_time() > 0) {
          LOG(INFO) << "overriden max_poll_time";
        }
      }
      break;
    }

    //LOG(INFO) << "bg_thread starting processing";
    while (_status == PROCESS) {
      //std::this_thread::sleep_for(100ms);
      if (_pending_msg) {
        std::this_thread::sleep_for(15ms);
        continue;
      }
      try_forward_messages();
      if (!_pending_msg)
        std::this_thread::sleep_for(100ms);
    }
    //LOG(INFO) << "bg_thread exiting";
  }

  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  streamprovider::AsyncService *service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue *cq_;
  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  SubscriptionRequest request_;
  std::string _real_topic;
  // What we send back to the client.
  bitbouncer::streaming::SubscriptionBundle reply_;
  volatile bool _pending_msg = false;
  int64_t _cursor; // next offset to be read
  int64_t _t0 = 0;
  int64_t _messages_written = 0;
  int64_t _aprox_bytes_written = 0;
  int64_t _next_eof_message = 0;
  int64_t _end_of_life_ts = 0;
  int _client_id = 0;
  std::string _source_ip;
  ::grpc::ServerAsyncWriter<bitbouncer::streaming::SubscriptionBundle> stream_;

  // Let's implement a tiny state machine with the following states.
  enum CallStatus {
    CREATE, START, VALIDATE, PROCESS, FINISH
  };
  volatile CallStatus _status;  // The current serving state.
  std::shared_ptr<bb::grpc_kafka_consumer> _source;
  std::shared_ptr<grpc_topic_authorizer> _authorizer;
  std::shared_ptr<shared_kafka_consumer_factory> _cache;
  IsCancelledCallback _isCancelled;
  std::thread _bg_thread;
};