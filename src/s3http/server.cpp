#include <tuple>
#include <pthread.h>
#include <sched.h>
#include <set>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/url.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include "server.hpp"
#include "s3_op_handler.hpp"

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

asio::awaitable<bool> S3HttpServer::auth_request(const http::request<http::buffer_body>& req) {
    if (conf_.auth_enabled) {
        auto auth = req["Authorization"];
        if(auth.empty())
            co_return false;

        // Make sure algorigthm is AWS4-HMAC-SHA256
        if (!auth.starts_with("AWS4-HMAC-SHA256"))
            co_return false;
        // Get the credential line
        auto pos_s = auth.find("Credential=") + beast::string_view("Credential=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        auto pos_e = auth.find(",");
        beast::string_view auth_creds = auth.substr(pos_s, pos_e-pos_s);
        // we only support one key/secret for now so we don't store it but
        // in the future if we want ot support multiple we'll change that
        auto in_pos = auth_creds.find("/");
        auth_creds.remove_prefix(in_pos+1);
        in_pos = auth_creds.find('/');
        auto scope_date = auth_creds.substr(0, in_pos);
        auto out_pos = auth_creds.find('/', in_pos+1);
        auto scope_region = auth_creds.substr(in_pos+1, out_pos-in_pos-1);

        auth.remove_prefix(pos_e + 1);

        // Get signed headers
        pos_s = auth.find("SignedHeaders=") + beast::string_view("SignedHeaders=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        pos_e = auth.find(",");
        beast::string_view auth_signedheaders = auth.substr(pos_s, pos_e-pos_s);
        auth.remove_prefix(pos_e + 1);

        // Get signature
        pos_s = auth.find("Signature=") + beast::string_view("Signature=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        beast::string_view auth_sign = auth.substr(pos_s);

        // Get path and query params
        beast::string_view target = req.target();
        std::string path;
        std::string query;
        pos_s = target.find('?');
        if (pos_s != beast::string_view::npos) {
            path = target.substr(0, pos_s);
            query = target.substr(pos_s + 1);
        } else {
            path = target;
            query = "";
        }

        // Query params must be in alphabetical order (kill me plz)
        std::set<std::string> s;
        while (true) {
            pos_s = query.find('&');
            // if the query is param with no val, we add a trailing =
            // cause apparently that's how it's done :shrug:
            auto val = query.substr(0, pos_s);
            if (!val.empty() && val.find('=') == beast::string_view::npos)
                val += "=";
            s.insert(val);
            query.erase(0, pos_s+1);
            if (pos_s == beast::string_view::npos) {
                query.erase(0);
                break;
            }
        }

        for (std::string q : s) {
            query += q + "&";
        }
        query.pop_back();        // lazy

        // String to Sha256
        std::string cannonicalRequest =
            std::string(req.method_string()) + "\n" +
            path + "\n" + query + "\n";

        // Extract and store the signed headers for this request
        pos_s = 0;
        std::string signedHeaders = std::string(auth_signedheaders);
        // this could bug i believe
        while (pos_s != beast::string_view::npos) {
            pos_s = auth_signedheaders.find(';');
            auto h = auth_signedheaders.substr(0, pos_s);
            cannonicalRequest += std::string(h) + ":" + std::string(req[h]) + "\n";
            auth_signedheaders.remove_prefix(pos_s+1);
        }

        cannonicalRequest += "\n" + signedHeaders + "\n" + std::string(req["x-amz-content-sha256"]);
        auto shaCannonReq = sha256_hex(cannonicalRequest);

        std::string stringToSign = "AWS4-HMAC-SHA256\n" +
            std::string(req["x-amz-date"]) +
            "\n" + std::string(auth_creds) + "\n"
            + shaCannonReq;

        auto date_key = hmac_sha256("AWS4"+ conf_.secret_key, std::string(scope_date));
        auto region_key = hmac_sha256(date_key, std::string(scope_region));
        auto svc_key = hmac_sha256(region_key, "s3");
        auto signing_key = hmac_sha256(svc_key, "aws4_request");
        auto calc = hmac_sha256(signing_key, stringToSign);

        if (hmactohex(calc) != auth_sign)
            co_return false;

    }
    co_return true;
}

bool S3HttpServer::parse_query_params(std::string_view t, std::unordered_map<std::string, std::string>& query_params) {
      
    auto target = boost::urls::parse_relative_ref(t);
    if (!target) {
        return false;
    }
    
    boost::urls::url_view u = *target;
    for (auto const& param : u.params()) {
        query_params.emplace(param.key, param.value);
    }
    return true;
}

void S3HttpServer::sanitize_target_path(std::string& target) {
    if (target.starts_with("/" + bucket_name))
        target.erase(0, bucket_name.size() + 1); // removes `/bucketname`

    // the query params are stored
    // into aws_params by now so we just strip 'em
    auto pos = target.find('?');
    if (pos != beast::string_view::npos) {
        target.erase(pos);
    }

    // We have a /something, erase the /
    if (target.front() == PATH_DELIM)
        target.erase(0, 1);
}

asio::awaitable<http::message_generator> S3HttpServer::handle_request(http::request<http::buffer_body>&& req, 
    std::shared_ptr<session_buffer> session_buffer) {

    //Store query params
    std::unordered_map<std::string, std::string> query_params;
    if (!parse_query_params(req.target(), query_params)) {
        co_return bad_request_res("InvalidRequest","Malformed request", std::move(req));
    }

    // Ensure only / is used as a delimiter 
    auto it = query_params.find("delimiter");
    if (it != query_params.end()) {
        if (!it->second.empty() && it->second != std::string(1, PATH_DELIM)) {
            co_return bad_request_res("InvalidRequest", "/ is the only supported delimiter.", std::move(req));
        }
    }

    std::string target{req.target()};
    sanitize_target_path(target);

    req.insert(lobos::s3::bucket, bucket_name);

    auto op = S3OpHandler(*store_, 
        std::move(req),
        std::move(session_buffer),
        std::move(target),
        std::move(query_params),
        active_mpus_
    );

    co_return co_await op.handle();
}

// Handles an HTTP server connection
asio::awaitable<void> S3HttpServer::do_session(beast::tcp_stream stream) {
    beast::flat_buffer buffer;
    for(;;)
    {
        auto session_buffer = make_buffer(0);
        // Set timeout
        stream.expires_after(std::chrono::seconds(30));

        http::request_parser<http::buffer_body> parser;
        parser.body_limit(MAX_OBJ_SIZE);
        // Parse headers first for PUT reqs
        co_await http::async_read_header(stream, buffer, parser, asio::use_awaitable);
        auto authed = co_await auth_request(parser.get());
        if (!authed) {
            auto msg = forbidden_res(std::move(parser.release()));
            co_await beast::async_write(stream, std::move(msg), asio::use_awaitable);
            if (!parser.get().keep_alive())
                break;
            continue;
        }

        if (parser.get().method() == http::verb::put || parser.get().method() == http::verb::post) {
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