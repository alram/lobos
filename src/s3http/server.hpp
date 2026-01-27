
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

#include "../index/index.hpp"
#include "../store/store.hpp"
#include "../store/buffer.hpp"


namespace beast = boost::beast;
namespace http  = beast::http;
namespace asio   = boost::asio;

struct httpConfig {
    std::string address;
    short unsigned int port;
    std::string access_key;
    std::string secret_key;
    bool use_spdk = false;
    bool auth_enabled = true;
};

class S3HttpServer {
public:
    explicit S3HttpServer(
        std::string dir,
        Store* store,
        httpConfig c
    )
        : store_(store)
        , conf_(c)
    {
        if (conf_.access_key.empty() || conf_.secret_key.empty())
            conf_.auth_enabled = false;

        auto const addr = asio::ip::make_address(conf_.address);
        endpoint = {addr, conf_.port};

        // Bucket name is the last dir passed
        // or lobos in non-fs mode
        if (dir.back() == '/')
            dir.pop_back();
        auto const pos = dir.rfind("/");
        bucket_name = dir.substr(pos + 1);

    }
    ~S3HttpServer() {};

    void start(int threads, std::vector<int> pins);
    void stop() {
        for (auto& ioc : ioctxs_) {
            ioc->stop();
        }
    }
private:
    Store* store_;
    httpConfig conf_;
    std::vector<std::unique_ptr<asio::io_context>> ioctxs_;
    std::vector<std::thread> thread_pool_;
    std::unique_ptr<asio::signal_set> signals_;
    asio::ip::tcp::endpoint endpoint;
    std::string bucket_name;

    asio::awaitable<void> do_listen(asio::ip::tcp::endpoint ep);
    asio::awaitable<void> do_session(beast::tcp_stream stream);
    asio::awaitable<http::message_generator> handle_request(
        http::request<http::buffer_body>&& req, 
        std::shared_ptr<session_buffer> session_buffer);

    void sanitize_target_path(std::string& target);
    bool parse_aws_params(std::string_view t, std::unordered_map<std::string, std::string>& aws_params);
    static std::string to_rfc1123(time_t t);
    static beast::string_view mime_type(beast::string_view path);
    std::string create_dest_dirs_if_not_exist(std::string object);

    asio::awaitable<bool> auth_request(const http::request<http::buffer_body>& req);

    asio::awaitable<http::message_generator> handle_get_object(beast::string_view object, 
        http::request<http::buffer_body>&& req,
        std::shared_ptr<session_buffer> session_buffer);
    asio::awaitable<http::message_generator> handle_head_object(beast::string_view object,
        http::request<http::buffer_body>&& req);
    asio::awaitable<http::message_generator> handle_list_objects(beast::string_view prefix,
        http::request<http::buffer_body>&& req,
        std::shared_ptr<session_buffer> session_buffer);
    asio::awaitable<http::message_generator> handle_put_object(beast::string_view object,
        http::request<http::buffer_body>&& req);

    http::message_generator not_found_bucket_res(beast::string_view bucket, http::request<http::buffer_body>&& req);
    http::message_generator not_found_key_res(beast::string_view object, http::request<http::buffer_body>&& req);
    http::message_generator forbidden_res(http::request<http::buffer_body>&& req);

    std::shared_ptr<session_buffer> make_buffer(size_t size) {
        if (conf_.use_spdk)
            return std::make_shared<spdk_buffer>(size);
        else
            return std::make_shared<vector_buffer>(size);
    }

};