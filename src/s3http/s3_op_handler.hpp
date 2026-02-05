#pragma once

#include "server.hpp"

namespace lobos::s3 {
    constexpr beast::string_view bucket = "x-lobos-bucket";
}

class S3OpHandler {
public:
    S3OpHandler(Store& store,
              http::request<http::buffer_body>&& req,
              std::shared_ptr<session_buffer> buffer,
              std::string key,
              std::unordered_map<std::string, std::string> query_params,
              std::unordered_map<std::string, Multipart>& active_mpus,
              std::unordered_map<std::string, Bucket>& buckets)
        : store_(store)
        , req_(std::move(req))
        , buffer_(std::move(buffer))
        , key_(std::move(key))
        , query_params_(std::move(query_params))
        , active_mpus_(active_mpus)
        , buckets_(buckets)
    {}

    asio::awaitable<http::message_generator> handle() {
        if (!req_[lobos::s3::bucket].empty() && !buckets_.contains(req_[lobos::s3::bucket]))
            co_return no_such_bucket_res();
        if (key_.empty())
            is_bucket_op_ = true;
        key_ = buckets_[req_[lobos::s3::bucket]].prefix + key_;
        switch (req_.method()) {
            case http::verb::head:    co_return co_await handle_head();
            case http::verb::get:      co_return co_await handle_get();
            case http::verb::put:    co_return co_await handle_put();
            case http::verb::delete_: co_return co_await handle_delete();
            case http::verb::post:   co_return co_await handle_post();
            default:                 co_return co_await method_not_allowed();
        }
    }

private:
    Store& store_;
    http::request<http::buffer_body> req_;
    std::shared_ptr<session_buffer> buffer_;
    std::string key_;
    std::unordered_map<std::string, std::string> query_params_;
    std::unordered_map<std::string, Multipart>& active_mpus_;
    std::unordered_map<std::string, Bucket>& buckets_;
    bool is_bucket_op_{false};

    // Verb handling func
    asio::awaitable<http::message_generator> handle_head();
    asio::awaitable<http::message_generator> handle_get();
    asio::awaitable<http::message_generator> handle_put();
    asio::awaitable<http::message_generator> handle_post();
    asio::awaitable<http::message_generator> handle_delete();
    asio::awaitable<http::message_generator> method_not_allowed() {
        co_return bad_request_res("InvalidRequest", "unsupported req");
    };

    // S3 returns
    asio::awaitable<http::message_generator> ok_bucket_ops();
    asio::awaitable<http::message_generator> ok_list_all_buckets();
    asio::awaitable<http::message_generator> ok_list_objects();
    asio::awaitable<http::message_generator> ok_list_mpu_parts();
    asio::awaitable<http::message_generator> s3_get_object();
    asio::awaitable<http::message_generator> complete_mpu();
    http::message_generator bad_request_res(std::string code, std::string msg);
    http::message_generator key_not_found_res();
    http::message_generator internal_error_res();
    http::message_generator no_such_bucket_res();
};