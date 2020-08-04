#include <memory>
#include <iostream>
#include <csignal>
#include <string>
#include <thread>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <boost/program_options.hpp>
#include "subscribe_call_data.h"
#include "get_schema_call_data.h"
#include <kspp/utils/env.h>
#include <utils/grpc_utils.h>
#include <utils/grpc_topic_authorizer.h>

using namespace std::chrono_literals;
using namespace kspp;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

#define SERVICE_NAME "kafka_grpc_proxy"
#define DEFAULT_PORT "50063"

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

class ServerImpl final {
public:
  ServerImpl(std::string bindTo, std::shared_ptr<shared_kafka_consumer_factory> cache,
             std::shared_ptr<grpc_topic_authorizer> auth, std::shared_ptr<avro_schema_registry> schema_registry,
             std::map<std::string, std::string> labels)
      : _server_address(bindTo), _cache(cache), _authorizer(auth), _schema_registry(schema_registry), labels_(labels) {
  }

  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    ServerBuilder builder;
    bb_set_channel_args(builder);
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(_server_address, grpc::InsecureServerCredentials());
    //builder.AddListeningPort(server_address, call_creds);
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&stream_service_);
    //builder.RegisterService(&get_schema_service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    LOG(INFO) << "Server listening on " << _server_address;

    // Proceed to the server's main loop.
    HandleRpcs();
  }

private:


  // This can be run in multiple threads if needed.
  void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
    new subscribe_call_data(&stream_service_, cq_.get(), _cache, _authorizer);
    new get_schema_call_data(&stream_service_, cq_.get(), _schema_registry, _authorizer);
    while (run) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      bool ok = true;
      void *tag = nullptr;
      if ((cq_->Next(&tag, &ok)) == false) {
        LOG(ERROR) << "cq_->Next failed";
        //if (tag)
        //  static_cast<call_data *>(tag)->set_error();
      }

      if (tag == nullptr)
        continue;

      static_cast<call_data *>(tag)->Proceed(ok);
    }
  }

  std::string _server_address;
  std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
  std::shared_ptr<grpc_topic_authorizer> _authorizer;
  std::shared_ptr<shared_kafka_consumer_factory> _cache;
  std::unique_ptr<ServerCompletionQueue> cq_;
  streamprovider::AsyncService stream_service_;
  std::unique_ptr<Server> server_;
  std::map<std::string, std::string> labels_;
};

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("app_realm", boost::program_options::value<std::string>()->default_value(get_env_and_log("APP_REALM", "DEV")),
       "app_realm")
      ("port", boost::program_options::value<std::string>()->default_value(get_env_and_log("PORT", DEFAULT_PORT)),
       "port")
      ("topic_auth_topic",
       boost::program_options::value<std::string>()->default_value(kspp::get_env_and_log("TOPIC_AUTH_TOPIC")),
       "topic_auth_topic");

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string app_realm;
  if (vm.count("app_realm")) {
    app_realm = vm["app_realm"].as<std::string>();
  }
  std::string port;
  if (vm.count("port")) {
    port = vm["port"].as<std::string>();
  }

  std::string topic_auth_topic;
  if (vm.count("topic_auth_topic")) {
    topic_auth_topic = vm["topic_auth_topic"].as<std::string>();
  }
  if (topic_auth_topic.size() == 0) {
    std::cerr << "topic_auth_topic must be specified" << std::endl;
    return -1;
  }

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->validate();
  config->log();
  config->get_schema_registry();
  //config->get_cluster_metadata()->close();

  LOG(INFO) << "port               : " << port;
  LOG(INFO) << "topic_auth_topic   : " << topic_auth_topic;
  LOG(INFO) << "discovering facts...";

  std::string hostname = default_hostname();
  std::map<std::string, std::string> labels = {
      {"app_name", SERVICE_NAME},
      {"port",      port},
      {"host",      hostname},
      {"app_realm", app_realm}
  };

  std::string bindTo = "0.0.0.0:" + port;

  auto auth = std::make_shared<grpc_topic_authorizer>(config, topic_auth_topic);
  auto cache = std::make_shared<shared_kafka_consumer_factory>(config);
  auto schema_registry = config->get_schema_registry();

  //std::signal(SIGINT, sigterm);
  //std::signal(SIGTERM, sigterm);
  //std::signal(SIGPIPE, SIG_IGN);

  ServerImpl server(bindTo, cache, auth, schema_registry, labels);
  server.Run();
  LOG(INFO) << "exiting";
  return 0;
}
