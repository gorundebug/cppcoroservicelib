// Eligibility probe only: official hiredis callbacks on the existing Asio loop.
// No SDK endpoint or production capability is declared by this program.
#include <utility>
#include <boost/asio.hpp>
#include <hiredis/async.h>
#include <hiredis/hiredis_ssl.h>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <thread>
#include <unistd.h>
namespace asio = boost::asio;
using namespace std::chrono_literals;
struct Reply {
  int type{};
  long long integer{};
  std::string text;
  std::vector<Reply> elements;
  static Reply copy(const redisReply* raw) {
    if (!raw) throw std::runtime_error("native connection closed before reply");
    Reply out{raw->type, raw->integer, {}, {}};
    if (raw->str) out.text.assign(raw->str, raw->len);
    for (size_t i=0; i<raw->elements; ++i) out.elements.push_back(copy(raw->element[i]));
    return out;
  }
};
struct Connection : std::enable_shared_from_this<Connection> {
  asio::any_io_executor executor;
  asio::posix::stream_descriptor reader, writer;
  asio::steady_timer timeout;
  redisAsyncContext* native{};
  redisSSLContext* tls{};
  bool readWanted{}, writeWanted{}, readPending{}, writePending{}, closed{};
  explicit Connection(asio::any_io_executor ex):executor(ex),reader(ex),writer(ex),timeout(ex){}
  ~Connection() { close(); if(tls) redisFreeSSLContext(tls); }
  void close() { if(native) { auto* n=native; native=nullptr; redisAsyncFree(n); } }
  static std::shared_ptr<Connection> open(asio::any_io_executor ex,const std::string& host,int port) {
    // Hiredis itself resolves names synchronously. This probe accepts IPs only;
    // production integration must resolve through Coro's existing async resolver.
    (void)asio::ip::make_address(host);
    auto c=std::make_shared<Connection>(ex);
    c->native=redisAsyncConnect(host.c_str(),port);
    if(!c->native || c->native->err) throw std::runtime_error("native connect failed");
    if(const char* ca=std::getenv("REDIS_TEST_CA")) {
      redisInitOpenSSL();
      redisSSLOptions options{};
      options.cacert_filename=ca;
      options.cert_filename=std::getenv("REDIS_TEST_CERT");
      options.private_key_filename=std::getenv("REDIS_TEST_KEY");
      options.server_name=std::getenv("REDIS_TEST_SERVER_NAME");
      options.verify_name=options.server_name ? options.server_name : host.c_str();
      options.verify_mode=REDIS_SSL_VERIFY_PEER;
      redisSSLContextError error{};
      c->tls=redisCreateSSLContextWithOptions(&options,&error);
      if(!c->tls || redisInitiateSSLWithContext(&c->native->c,c->tls)!=REDIS_OK)
        throw std::runtime_error("native TLS setup failed");
    }
    c->reader.assign(::dup(c->native->c.fd));
    c->writer.assign(::dup(c->native->c.fd));
    auto& hooks=c->native->ev; hooks.data=c.get();
    hooks.addRead=[](void* p){auto& c=*static_cast<Connection*>(p);c.readWanted=true;c.armRead();};
    hooks.addWrite=[](void* p){auto& c=*static_cast<Connection*>(p);c.writeWanted=true;c.armWrite();};
    hooks.delRead=[](void* p){auto& c=*static_cast<Connection*>(p);c.readWanted=false;c.reader.cancel();};
    hooks.delWrite=[](void* p){auto& c=*static_cast<Connection*>(p);c.writeWanted=false;c.writer.cancel();};
    hooks.cleanup=[](void* p){auto& c=*static_cast<Connection*>(p);c.closed=true;c.native=nullptr;c.reader.close();c.writer.close();c.timeout.cancel();};
    hooks.scheduleTimer=[](void* p,timeval tv){auto self=static_cast<Connection*>(p)->shared_from_this();self->timeout.expires_after(std::chrono::seconds(tv.tv_sec)+std::chrono::microseconds(tv.tv_usec));self->timeout.async_wait([self](auto e){if(!e && self->native)redisAsyncHandleTimeout(self->native);});};
    redisAsyncSetConnectCallback(c->native,[](const redisAsyncContext*,int){});
    return c;
  }
  void armRead() {
    if(closed || readPending || !readWanted)return;
    readPending=true;
    reader.async_wait(asio::posix::stream_descriptor::wait_read,[self=shared_from_this()](auto e){
      self->readPending=false;
      if(!e && self->native && self->readWanted)redisAsyncHandleRead(self->native);
      self->armRead();
    });
  }
  void armWrite() {
    if(closed || writePending || !writeWanted)return;
    writePending=true;
    writer.async_wait(asio::posix::stream_descriptor::wait_write,[self=shared_from_this()](auto e){
      self->writePending=false;
      if(!e && self->native && self->writeWanted)redisAsyncHandleWrite(self->native);
      self->armWrite();
    });
  }
  struct Pending {
    asio::steady_timer wake;
    std::optional<Reply> reply;
    std::exception_ptr failure;
    explicit Pending(asio::any_io_executor ex):wake(ex,5s){}
  };
  asio::awaitable<Reply> command(std::vector<std::string> args) {
    auto self=shared_from_this();
    if(!native || closed)throw std::runtime_error("retired connection");
    auto pending=std::make_shared<Pending>(executor);
    auto* owned=new std::shared_ptr<Pending>(pending);
    std::vector<const char*> values;std::vector<size_t> sizes;
    for(auto& a:args){values.push_back(a.data());sizes.push_back(a.size());}
    const auto result=redisAsyncCommandArgv(native,[](redisAsyncContext*,void* r,void* p){
      std::unique_ptr<std::shared_ptr<Pending>> holder(static_cast<std::shared_ptr<Pending>*>(p));
      auto& value=**holder;
      try{value.reply=Reply::copy(static_cast<redisReply*>(r));}catch(...){value.failure=std::current_exception();}
      value.wake.cancel();
    },owned,static_cast<int>(values.size()),values.data(),sizes.data());
    if(result!=REDIS_OK){delete owned;throw std::runtime_error("native admission rejected");}
    if(!pending->reply && !pending->failure){boost::system::error_code e;co_await pending->wake.async_wait(asio::redirect_error(asio::use_awaitable,e));}
    if(!pending->reply && !pending->failure){close();throw std::runtime_error("command deadline");}
    if(pending->failure)std::rethrow_exception(pending->failure);
    if(pending->reply->type==REDIS_REPLY_ERROR)throw std::runtime_error(pending->reply->text);
    co_return std::move(*pending->reply);
  }
};
void check(bool value,const char* what){if(!value)throw std::runtime_error(what);}
asio::awaitable<void> probe(std::string host,int port,std::string key) {
  auto ex=co_await asio::this_coro::executor;
  auto command=Connection::open(ex,host,port),read=Connection::open(ex,host,port);
  try {
    if(const char* password=std::getenv("REDIS_TEST_PASSWORD")) {
      std::vector<std::string> auth{"AUTH",password};
      co_await command->command(auth);co_await read->command(auth);
    }
    if(const char* mode=std::getenv("REDIS_TEST_MODE");mode && std::string(mode)=="ambiguity") {
      std::vector<std::string> add{"XADD",key,"*","value","once"};
      bool ambiguous=false;
      try {co_await command->command(add);}catch(const std::exception&){ambiguous=true;}
      check(ambiguous,"lost reply was reported as confirmed");
      command->close();read->close();
      auto replacement=Connection::open(ex,"127.0.0.1",6379);
      try {
        if(const char* password=std::getenv("REDIS_TEST_PASSWORD")) {
          std::vector<std::string> auth{"AUTH",password};co_await replacement->command(auth);
        }
        std::vector<std::string> count{"XLEN",key};
        check((co_await replacement->command(count)).integer==1,"ambiguous write absent or replayed");
        std::vector<std::string> remove{"DEL",key};co_await replacement->command(remove);
        replacement->close();
      }catch(...){replacement->close();throw;}
      std::cout<<"PASS: ambiguous XADD exists once; explicit reconnect, no automatic replay\n";
      co_return;
    }
    std::vector<std::string> group{"XGROUP","CREATE",key,"workers","0","MKSTREAM"};
    co_await command->command(group);
    std::string binary("a\0b",3);
    std::vector<std::string> add{"XADD",key,"*","field",binary,"field","second"};
    auto id=co_await command->command(add);
    std::vector<std::string> receive{"XREADGROUP","GROUP","workers","probe","COUNT","1","STREAMS",key,">"};
    auto got=co_await read->command(receive);
    auto& record=got.elements.at(0).elements.at(1).elements.at(0);
    check(record.elements.at(0).text==id.text,"entry id mismatch");
    auto& fields=record.elements.at(1).elements;
    check(fields.size()==4 && fields[0].text==fields[2].text && fields[1].text==binary,"binary/duplicate fields lost");
    std::vector<std::string> pending{"XPENDING",key,"workers"};
    check((co_await command->command(pending)).elements.at(0).integer==1,"receive implicitly acknowledged");
    std::vector<std::string> ack{"XACK",key,"workers",id.text};
    check((co_await command->command(ack)).integer==1,"explicit ACK failed");
    check((co_await command->command(pending)).elements.at(0).integer==0,"pending remained after ACK");
    unsigned ticks=0;
    asio::steady_timer heartbeat(ex);
    std::function<void()> tick;
    tick=[&]{heartbeat.expires_after(5ms);heartbeat.async_wait([&](auto e){if(!e){++ticks;tick();}});};tick();
    asio::steady_timer cancel(ex,80ms);
    cancel.async_wait([read](auto e){if(!e)read->close();});
    receive.insert(receive.begin()+4,{"BLOCK","10000"});
    bool canceled=false;auto start=std::chrono::steady_clock::now();
    try {co_await read->command(receive);}catch(const std::exception&){canceled=true;}
    heartbeat.cancel();
    check(canceled && ticks>=5 && std::chrono::steady_clock::now()-start<500ms,"blocked read stalled executor or cancellation");
    std::vector<std::string> ping{"PING"};
    check((co_await command->command(ping)).text=="PONG","independent command connection stopped");
    std::vector<std::string> remove{"DEL",key};co_await command->command(remove);
    command->close();read->close();
    std::cout<<"PASS: Coro hiredis native binary/duplicate fields, explicit ACK, nonblocking read cancellation; heartbeat="<<ticks<<"\n";
  } catch(...) {command->close();read->close();throw;}
}
int main(int argc,char** argv){
  if(argc!=4){std::cerr<<"host-ip port unique-key required\n";return 2;}
  const int workers=std::getenv("REDIS_TEST_WORKERS") ? std::stoi(std::getenv("REDIS_TEST_WORKERS")) : 1;
  if(workers<1 || workers>8)return 2;
  asio::io_context io(workers);int result=0;
  // A single owner executor makes concurrent access to the native context impossible.
  asio::co_spawn(asio::make_strand(io),probe(argv[1],std::stoi(argv[2]),argv[3]),[&](std::exception_ptr e){if(e){result=1;try{std::rethrow_exception(e);}catch(const std::exception& x){std::cerr<<x.what()<<"\n";}}});
  {std::vector<std::jthread> threads;for(int i=1;i<workers;++i)threads.emplace_back([&]{io.run();});io.run();}
  return result;
}
