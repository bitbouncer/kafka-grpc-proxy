#include "grpc_apikey_auth.h"
#include <glog/logging.h>

#define API_KEY_HEADER           "api-key"
#define SECRET_ACCESS_KEY_HEADER "secret-access-key"
#define X_FORWARDED_FOR          "x-forwarded-for"


/*bool autheticate(grpc::ServerContext &server_context, const std::string &api_key, const std::string &secret) {
  const std::multimap<grpc::string_ref, grpc::string_ref>& client_metadata = server_context.client_metadata();
  LOG(INFO) << "autheticate: " << client_metadata.size();
  bool has_key = false;
  bool has_secret = false;

  for (const auto &i : client_metadata) {
    if ((i.first == "api-key") && (i.second == api_key))
      has_key = true;
    if ((i.first == "api-secret") && (i.second == secret))
      has_secret = true;
  }
  return has_key && has_secret;
}
*/


std::string get_api_key(grpc::ServerContext &server_context) {
  const std::multimap<grpc::string_ref, grpc::string_ref> &client_metadata = server_context.client_metadata();
  for (const auto &i : client_metadata) {
    if (i.first == API_KEY_HEADER)
      return std::string(i.second.data(), i.second.size());
  }
  return "";
}

std::string get_api_secret(grpc::ServerContext &server_context) {
  const std::multimap<grpc::string_ref, grpc::string_ref> &client_metadata = server_context.client_metadata();
  for (const auto &i : client_metadata) {
    if (i.first == SECRET_ACCESS_KEY_HEADER)
      return std::string(i.second.data(), i.second.size());
  }
  return "";
}

std::string get_metadata(grpc::ServerContext &server_context) {
  const std::multimap<grpc::string_ref, grpc::string_ref> &client_metadata = server_context.client_metadata();
  std::string result;
  for (const auto &i : client_metadata) {
    if (result.size())
      result += ", ";
    result += std::string(i.first.data(), i.first.size()) + "=" + std::string(i.second.data(), i.second.size());
  }
  return result;
}

/*
class MyCustomAuthenticator : public grpc::MetadataCredentialsPlugin {
public:
  MyCustomAuthenticator(const grpc::string& ticket) : ticket_(ticket) {}

  grpc::Status GetMetadata(grpc::string_ref service_url,
  grpc::string_ref method_name,
  const grpc::AuthContext& channel_auth_context,
  std::multimap<grpc::string, grpc::string>* metadata) override {
    metadata->insert(std::make_pair("x-custom-auth-ticket", ticket_));
    return grpc::Status::OK;
  }

private:
  grpc::string ticket_;
};
*/


std::string get_forwarded_for(grpc::ServerContext &server_context) {
  const std::multimap<grpc::string_ref, grpc::string_ref> &client_metadata = server_context.client_metadata();
  for (const auto &i : client_metadata) {
    if (i.first == X_FORWARDED_FOR)
      return std::string(i.second.data(), i.second.size());
  }
  return "unknown_ip";
}

