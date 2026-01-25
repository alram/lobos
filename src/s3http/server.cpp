#include <tuple>
#include <pthread.h>
#include <sched.h>

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/url.hpp>

#include "server.hpp"

#define SERVER_NAME "LOBOS BB"
// Set to ext4 max file size (16TiB)
#define MAX_OBJ_SIZE 16ULL<<40
#define PATH_DELIM '/'

namespace beast = boost::beast;
namespace http  = beast::http;
namespace asio   = boost::asio;
namespace fs    = boost::filesystem;

void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_setaffinity_np(
        pthread_self(),
        sizeof(cpu_set_t),
        &cpuset
    );
}

// Return a reasonable mime type based on the extension of a file.
beast::string_view S3HttpServer::mime_type(beast::string_view path) {
    using beast::iequals;
    auto const ext = [&path]
    {
        auto const pos = path.rfind(".");
        if(pos == beast::string_view::npos)
            return beast::string_view{};
        return path.substr(pos);
    }();
    if(iequals(ext, ".htm"))  return "text/html";
    if(iequals(ext, ".html")) return "text/html";
    if(iequals(ext, ".php"))  return "text/html";
    if(iequals(ext, ".css"))  return "text/css";
    if(iequals(ext, ".txt"))  return "text/plain";
    if(iequals(ext, ".js"))   return "application/javascript";
    if(iequals(ext, ".json")) return "application/json";
    if(iequals(ext, ".xml"))  return "application/xml";
    if(iequals(ext, ".swf"))  return "application/x-shockwave-flash";
    if(iequals(ext, ".flv"))  return "video/x-flv";
    if(iequals(ext, ".png"))  return "image/png";
    if(iequals(ext, ".jpe"))  return "image/jpeg";
    if(iequals(ext, ".jpeg")) return "image/jpeg";
    if(iequals(ext, ".jpg"))  return "image/jpeg";
    if(iequals(ext, ".gif"))  return "image/gif";
    if(iequals(ext, ".bmp"))  return "image/bmp";
    if(iequals(ext, ".ico"))  return "image/vnd.microsoft.icon";
    if(iequals(ext, ".tiff")) return "image/tiff";
    if(iequals(ext, ".tif"))  return "image/tiff";
    if(iequals(ext, ".svg"))  return "image/svg+xml";
    if(iequals(ext, ".svgz")) return "image/svg+xml";
    return "application/text";
}

std::string S3HttpServer::to_rfc1123(time_t t) {
    std::tm tm{};
    gmtime_r(&t, &tm);

    char buf[30];
    std::strftime(buf, sizeof(buf),
                  "%a, %d %b %Y %H:%M:%S GMT",
                  &tm);
    return buf; 
}

bool S3HttpServer::parse_aws_params(std::string_view t, std::unordered_map<std::string, std::string>& aws_params) {
      
    auto target = boost::urls::parse_relative_ref(t);
    if (!target) {
        return false;
    }
    
    boost::urls::url_view u = *target;
    for (auto const& param : u.params()) {
        aws_params.emplace(param.key, param.value);
    }
    return true;
}

void S3HttpServer::sanitize_target_path(std::string& target) {
    if (target.starts_with("/" + bucket_name))
        target.erase(0, bucket_name.size() + 1); // removes `/bucketname`

    // since we know we don't have extra filepath info we remove it all
    if (target.front() == '?')
        target.erase();

    // We have a /something, erase the /
    if (target.front() == PATH_DELIM)
        target.erase(0, 1);
}

// TODO this isn't used
http::message_generator S3HttpServer::not_found_bucket_res(beast::string_view bucket, http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req.keep_alive());
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchBucket</Code>"
        "<Message>The specified bucket does not exist</Message>"
        "<Resource>" + std::string(bucket) + "</Resource>"
        "<RequestId>not available</RequestId></Error>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::not_found_key_res(beast::string_view target, http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req.keep_alive());
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchKey</Code>"
        "<Message>The resource you requested does not exist</Message>"
        "<Resource>" + std::string(target) + "</Resource>"
        "<RequestId>DEADBEEF</RequestId>";
    res.prepare_payload();
    return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_head_object(beast::string_view object, http::request<http::buffer_body>&& req) {

    auto [size, last_modified] = co_await store_->do_metadata_req(object);

    if (last_modified == 0 && size == 0)
        co_return not_found_key_res(object, std::move(req));

    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    boost::string_view sv(std::to_string(last_modified));
    res.set(http::field::last_modified, sv);
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    co_return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_list_objects(beast::string_view prefix, 
    http::request<http::buffer_body>&& req, std::shared_ptr<session_buffer> session_buffer) {
    // idk man 1k feels like plenty ¯\_(ツ)_/¯ TODO
    session_buffer->reserve(1024);
    std::string h = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>" + bucket_name + "</Name>"
        "<Prefix>" + std::string(prefix) + "</Prefix>"
        "<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>";
    session_buffer->append(h);
    co_await store_->do_list(prefix, *session_buffer);
    std::string f = "<Marker></Marker></ListBucketResult>";
    session_buffer->append(f);
    
    http::response<http::buffer_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    
    res.body().data = session_buffer->data();
    res.body().size = session_buffer->size();
    res.body().more = false;

    res.prepare_payload();
    co_return res;
}

// TODO - get/put should handle chunks for large IO however this will require
// a lot of changes so for now we dont
asio::awaitable<http::message_generator> S3HttpServer::handle_get_object(beast::string_view object, http::request<http::buffer_body>&& req, std::shared_ptr<session_buffer> session_buffer) {
    auto [size, last_modified] = co_await store_->do_metadata_req(object);
    
    if (last_modified == 0)
        co_return not_found_key_res(object, std::move(req));
    
    session_buffer->resize(size);
    //todo check return
    co_await store_->do_read(object, 0, *session_buffer);

    http::response<http::buffer_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    res.set(http::field::last_modified, to_rfc1123(last_modified));

    res.body().data = session_buffer->data();
    res.body().size = session_buffer->size();
    res.body().more = false;

    res.prepare_payload();

    co_return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_request(http::request<http::buffer_body>&& req, 
    std::shared_ptr<session_buffer> session_buffer) {
    // Returns a bad request response
    auto const bad_request_res =
    [&req](beast::string_view why)
    {
        http::response<http::string_body> res{http::status::bad_request, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = std::string(why);
        res.prepare_payload();
        return res;
    };

    auto const delete_object_res = 
    [&req]()
    {
        http::response<http::string_body> res{http::status::no_content, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.keep_alive(req.keep_alive());
        return res;
    };

    auto const bucket_ops_res =
    [&req, this](std::unordered_map<std::string, std::string>& aws_params)
    {
        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::content_type, "application/xml");
        res.keep_alive(req.keep_alive());
        if (aws_params.contains("versioning")) {
            res.body() =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<VersioningConfiguration>"
                "<Status>Suspended</Status>"
                "<MfaDelete>Disabled</MfaDelete>"
                "</VersioningConfiguration>";
        } else if (aws_params.contains("object-lock")) {
            res.body() = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<ObjectLockConfiguration></ObjectLockConfiguration>";
        } else {
            res.body() = 
                "<ListAllMyBucketsResult><Buckets>"
                "<Bucket>"
                "<BucketRegion>lobos</BucketRegion>"
                "<CreationDate>1970-01-01T00:00:00+00:00</CreationDate>"
                "<Name>"+ bucket_name + "</Name>"
                "</Bucket>"
                "</Buckets>"
                "<Owner><ID>lobos</ID></Owner>"
                "</ListAllMyBucketsResult>";
        }
        res.prepare_payload();
        return res;
    };

    //Store aws' s3 url req params
    std::unordered_map<std::string, std::string> aws_params;
    if (!parse_aws_params(req.target(), aws_params)) {
        co_return bad_request_res("Malformed request");
    }

    // Ensure only / is used as a delimiter 
    auto it = aws_params.find("delimiter");
    if (it != aws_params.end()) {
        if (!it->second.empty() && it->second != std::string(1, PATH_DELIM)) {
            co_return bad_request_res("/ is the only supported delimiter.");
        }
    }

    std::string target = req.target();
    sanitize_target_path(target);

    // Handles HeadObject/HeadBucket requests
    if (req.method() == http::verb::head) {
        if (target.empty()) {
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::server, SERVER_NAME);
            res.insert("x-amz-bucket-region", "lobos");
            res.keep_alive(req.keep_alive());
            co_return res;
        }
        co_return co_await handle_head_object(target, std::move(req));
    }

    if (req.method() == http::verb::put) {
        auto ret = co_await store_->do_write(target, *session_buffer);
        if (ret < 0) {
            http::response<http::string_body> res{http::status::service_unavailable, req.version()};
            res.set(http::field::server, SERVER_NAME);
            res.keep_alive(req.keep_alive());
            res.prepare_payload();
            co_return res;
        } 
        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.insert("x-amz-object-size", std::to_string(req.body().size));
        res.keep_alive(req.keep_alive());
        res.prepare_payload();

        co_return res;
    }    

    // Now the big one, GET. It gets f'ed up and we use params to target between
    // Bucket reqs and Object reqs. This is most definitely broken and is going
    // to be a pain to go back to but that's for future Alex.
    if (req.method() == http::verb::get) {
        // We're look at the params to figure out what to do
        // this is naive and will not work with listobjectv1
        if (target.empty()) {
            if (aws_params.contains("list-type"))
                co_return co_await handle_list_objects(aws_params["prefix"], std::move(req), session_buffer);
            if (aws_params.contains("versioning") || 
                aws_params.contains("object-lock") || 
                aws_params.contains("max-buckets") ||
                aws_params.empty())
                co_return bucket_ops_res(aws_params);
        } else {
            // This is a get object probably?
            co_return co_await handle_get_object(target, std::move(req), session_buffer);
        }
    }

    if (req.method() == http::verb::delete_) {
        auto deleted = co_await store_->do_delete(target);
        if (!deleted)
            co_return not_found_key_res(target, std::move(req));

        co_return delete_object_res();
    }

    std::cout << "unsupported req: " << req.method() << " " << req.target() << std::endl;
    co_return bad_request_res("unsupported req");
}

// Handles an HTTP server connection
asio::awaitable<void> S3HttpServer::do_session(beast::tcp_stream stream) {
    beast::flat_buffer buffer;
    //TODO: might want to use a bufferpool 
    // https://claude.ai/chat/2e353513-17f0-4f0a-83e2-410cdabe1935
    for(;;)
    {
        auto session_buffer = make_buffer(0);
        // Set timeout
        stream.expires_after(std::chrono::seconds(30));

        http::request_parser<http::buffer_body> parser;
        parser.body_limit(MAX_OBJ_SIZE);

        // Parse headers first for PUT reqs
        co_await http::async_read_header(stream, buffer, parser, asio::use_awaitable);
        if (parser.get().method() == http::verb::put) {
            std::string target = parser.get().target();
            sanitize_target_path(target);
            auto content_length = parser.content_length().value_or(0);
            session_buffer->resize(content_length);

            parser.get().body().data = session_buffer->data();
            parser.get().body().size = session_buffer->size();
        }

        co_await http::async_read(stream, buffer, parser, asio::use_awaitable);
        auto req = parser.release();
    
        auto msg = co_await handle_request(std::move(req), session_buffer);

        bool keep_alive = msg.keep_alive();
        co_await beast::async_write(stream, std::move(msg), asio::use_awaitable);

        if (!keep_alive)
            break;
    }

    // Send a TCP shutdown
    stream.socket().shutdown(asio::ip::tcp::socket::shutdown_send);
}

asio::awaitable<void> S3HttpServer::do_listen(asio::ip::tcp::endpoint ep) {
    auto executor = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor{executor};

    acceptor.open(ep.protocol());
    acceptor.set_option(asio::socket_base::reuse_address(true));
#ifdef SO_REUSEPORT
    acceptor.set_option(asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>(true));
#endif
    acceptor.bind(ep);
    acceptor.listen(asio::socket_base::max_listen_connections);

    for (;;) {
        asio::ip::tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
        // no_delay improved throughput by almost 4x on loopback during my benchmarks
        socket.set_option(asio::ip::tcp::no_delay(true));

        auto on_session_except = [](std::exception_ptr e) {
            if (e) {
                try { std::rethrow_exception(e); }
                catch (std::exception const& ex) {
                    std::cerr << "Session error: "
                            << ex.what() << "\n";
                }
            }
        };

        asio::co_spawn(
            executor,
            do_session(beast::tcp_stream{std::move(socket)}), 
            on_session_except);
    }
}

void S3HttpServer::start(int threads, std::vector<int> pins) {
    std::cout << "Starting S3 HTTP server for bucket " << bucket_name << " at " << endpoint << std::endl;
    
    ioctxs_.reserve(threads);

    for (int i = 0; i < threads; ++i)
        ioctxs_.emplace_back(std::make_unique<asio::io_context>(1));

    // Set up signal handling FIRST, before threads start
    signals_ = std::make_unique<asio::signal_set>(*ioctxs_[0], SIGINT, SIGTERM);
    signals_->async_wait([this](boost::system::error_code const&, int sig) {
        std::cout << "Received: " << sig << " - Stopping." << std::endl;
        stop();
        store_->shutdown_store();
    });

    thread_pool_.reserve(threads);

    for (int i = 0; i < threads; i++) {
        thread_pool_.emplace_back([&, i]{

            if (pins.size() > 0)
                pin_thread_to_core(pins[i]);

            asio::co_spawn(
                *ioctxs_[i],
                do_listen(endpoint),
                [](std::exception_ptr e) {
                    if (e) {
                        try { std::rethrow_exception(e); }
                        catch (std::exception const&ex) {
                            std::cerr << "Error " << ex.what() << std::endl;
                        }
                    }
                });
            ioctxs_[i]->run();
        });
    }

    for (auto& t : thread_pool_)
        t.join();
}