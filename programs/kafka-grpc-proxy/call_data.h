#pragma once

class call_data {
public:
  call_data() {}

  virtual ~call_data() {}

  virtual void Proceed(bool ok) = 0;
  //virtual void set_error()=0;
};

//https://github.com/grpc/grpc/issues/18929

struct IsCancelledCallback final : public call_data {
  IsCancelledCallback(const grpc::ServerContext &ctx)
      : _ctx(ctx) {}

  void Proceed(bool ok) override {
    isCancelled = _ctx.IsCancelled();
  }

  //virtual void set_error() {}; // going away

  bool isCancelled = false;

private:
  const grpc::ServerContext &_ctx;
};

struct RPCContextWithCancelled : public call_data {
  bool isCancelled() const { return _isCancelled.isCancelled; }

protected:
  RPCContextWithCancelled()
      : _isCancelled(_ctx) { _ctx.AsyncNotifyWhenDone(&_isCancelled); }

  grpc::ServerContext _ctx;

  IsCancelledCallback _isCancelled;
};
