#include <memory>
#include <iostream>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include <kspp/avro/avro_schema_registry.h>
#include <kspp/cluster_config.h>
#include <bb_streaming.grpc.pb.h>
#include <utils/grpc_apikey_auth.h>
#include <utils/grpc_topic_authorizer.h>
#include "call_data.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using bitbouncer::streaming::GetSchemaRequest;
using bitbouncer::streaming::GetSchemaReply;
using bitbouncer::streaming::streamprovider;

class get_schema_call_data : public call_data {
public:
  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  get_schema_call_data(streamprovider::AsyncService *service, ServerCompletionQueue *cq,
                       std::shared_ptr<kspp::avro_schema_registry> schema_registry,
                       std::shared_ptr<grpc_topic_authorizer> auth)
      : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), _schema_registry(schema_registry),
        _authorizer(auth) {
    // Invoke the serving logic right away.
    Proceed(true);
  }

  void Proceed(bool ok) override {
    if (status_ == CREATE) {
      // Make this instance progress to the PROCESS state.
      status_ = PROCESS;
      service_->RequestGetSchema(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
      // Spawn a new CallData instance to serve new clients while we process
      // the one for this CallData. The instance will deallocate itself as
      // part of its FINISH state.
      new get_schema_call_data(service_, cq_, _schema_registry, _authorizer);

      std::string api_key = get_api_key(ctx_);
      if (!_authorizer->authorize_api(api_key)) {
        LOG(WARNING) << "authentication failed ";
        //responder_.Finish(Status(grpc::StatusCode::UNAUTHENTICATED, "authentication failed"), this);
        responder_.FinishWithError(Status::CANCELLED, this);
        status_ = FINISH;
      } else {
        // The actual processing.
        request_.schema_id();
        auto schema = _schema_registry->get_schema(request_.schema_id());
        if (schema) {
          std::stringstream ss;
          schema->toJson(ss);
          //LOG(INFO) << "avro_schema_registry get " << request_.schema_id() << "-> " << ss.str();
          reply_.set_schema(ss.str());
        } else {
          reply_.set_schema("");
          // TODO get the actual error code from schema registry
        }
        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      }
    } else {
      GPR_ASSERT(status_ == FINISH);
      // Once in the FINISH state, deallocate ourselves (CallData).
      delete this;
    }
  }

  /*virtual void set_error(){
    status_ = FINISH;
    LOG(INFO) << "set-error";
  }
  */

private:
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
  GetSchemaRequest request_;
  // What we send back to the client.
  GetSchemaReply reply_;

  // The means to get back to the client.
  ServerAsyncResponseWriter<GetSchemaReply> responder_;

  // Let's implement a tiny state machine with the following states.
  enum CallStatus {
    CREATE, PROCESS, FINISH
  };
  CallStatus status_;  // The current serving state.

  //std::shared_ptr<kspp::cluster_config> s_cluster_config;
  std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
  std::shared_ptr<grpc_topic_authorizer> _authorizer;
};
