
#pragma once 

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/config.hpp>
#include <boost/asio/signal_set.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>

#include "../common/common.hpp" 
#include "../index/index.hpp"
#include "../store/store.hpp"
#include "../store/buffer.hpp"
#include "../controlplane/loboscontrol_server.hpp"
#include "s3_bucket.hpp"


namespace beast = boost::beast;
namespace http  = beast::http;
namespace asio   = boost::asio;

namespace lobos::http {
    constexpr beast::string_view server_name = "lobos";
}

struct serverConfig {
    std::string address;
    short unsigned int port;
    std::string domain;
    bool auth_enabled;
    std::string grpc_server;
    bool use_spdk = false;
};

class S3HttpServer {
public:
    explicit S3HttpServer(
        Store* store,
        serverConfig c
    )
        : store_(store)
        , conf_(c)
    {
        auto const addr = asio::ip::make_address(conf_.address);
        endpoint = {addr, conf_.port};

        auto mpus = store_->get_active_mpus();

        // Start the control plane
        cp_ = std::make_unique<ControlPlane>(*store_, s3_users_);
        cp_server_.start(conf_.grpc_server, cp_.get());
        auto users = cp_->list_all_users("");
        for (const User& u : users) {
            s3_users_.insert(std::pair<std::string, std::string>(u.key, u.secret));
        }

        // Load all buckets
        auto buckets_records = store_->load_buckets();

        for (const auto& rec : buckets_records) {
            // key is prefix _ bucket name
            auto pos = rec.key.find('_');
            if (pos == std::string::npos) {
                std::cerr << "ugh this should not happen, returned a bucket idenfier: " << rec.key << std::endl;
                break;
            }
            std::string prefix = rec.key.substr(0, pos);
            std::string name = rec.key.substr(pos+1);

            auto bucket = std::make_unique<S3Bucket>(
                *store_,
                name,
                rec.owner,
                prefix,
                rec.created_at,
                std::move(mpus[name])
            );
            buckets_.emplace(name, std::move(bucket));
        }

    }
    ~S3HttpServer() {
        cp_server_.stop();
    };

    void start(int threads, std::vector<int> pins);
    void stop() {
        for (auto& ioc : ioctxs_) {
            ioc->stop();
        }
    }
private:
    Store* store_;
    serverConfig conf_;
    ControlPlaneServer cp_server_;
    std::unique_ptr<ControlPlane> cp_;

    std::vector<std::unique_ptr<asio::io_context>> ioctxs_;
    std::vector<std::thread> thread_pool_;
    std::unique_ptr<asio::signal_set> signals_;
    asio::ip::tcp::endpoint endpoint;

    asio::awaitable<void> do_listen(asio::ip::tcp::endpoint ep);
    asio::awaitable<void> do_session(beast::tcp_stream stream);
    asio::awaitable<http::message_generator> handle_request(
        http::request<http::buffer_body>&& req, 
        std::shared_ptr<session_buffer> session_buffer);

    void sanitize_target_path(beast::string_view& target, beast::string_view bucket, bool is_path_style);
    bool parse_query_params(std::string_view t, std::unordered_map<std::string, std::string>& query_params);

    asio::awaitable<bool> auth_request(const http::request<http::buffer_body>& req);

    std::unordered_map<std::string, std::string> s3_users_;
    // std::unordered_map<std::string, Multipart> active_mpus_;
    std::unordered_map<std::string, std::unique_ptr<S3Bucket>> buckets_;

    http::message_generator forbidden_res(http::request<http::buffer_body>&& req) {
        http::response<http::string_body> res{http::status::forbidden, req.version()};
        res.set(http::field::server, lobos::http::server_name);
        res.set(http::field::content_type, "application/xml");
        res.keep_alive(false);
        res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error><Code>InvalidAccessKeyId</Code>"
            "<Message></Message>"
            "</Error>";
        res.prepare_payload();
        return res;
    }
    http::message_generator bad_request_res(std::string code, std::string msg, http::request<http::buffer_body>&& req) {
        http::response<http::string_body> res{http::status::bad_request, req.version()};
        res.set(http::field::server, lobos::http::server_name);
        res.set(http::field::content_type, "application/xml");
        res.keep_alive(false);
        res.body() =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error>"
            "<Code>"+ code +"</Code>"
            "<Message>"+ msg + "</Message>"
            "<RequestId>...</RequestId>"
            "</Error>";
        res.prepare_payload();
        return res;
    }

    std::shared_ptr<session_buffer> make_buffer(size_t size) {
        if (conf_.use_spdk)
            return std::make_shared<spdk_buffer>(size);
        else
            return std::make_shared<vector_buffer>(size);
    }

};