#pragma once

#include "../store/store.hpp"

class S3Bucket {
friend class S3OpHandler;
public:
    S3Bucket(Store& store, std::string name, uint64_t owner, std::string prefix, time_t created_at, std::unordered_map<std::string, Multipart> mpus)
        : store_(store)
        , name_(name)
        , owner_(owner)
        , prefix_(prefix)
        , created_at_(created_at)
        , mpus_(std::move(mpus))
    {}

    asio::awaitable<bool> create_bucket();
    asio::awaitable<int> delete_bucket();
    
    asio::awaitable<std::tuple<size_t, time_t>> get_object_metadata(std::string& key);
    asio::awaitable<int> put_object(std::string& key, std::shared_ptr<session_buffer>& buffer);
    asio::awaitable<int> get_object(std::string& key, uint64_t& offset, std::shared_ptr<session_buffer>& buffer);
    asio::awaitable<int> delete_object(std::string& key);
    asio::awaitable<void> list_objects(std::string& prefix, std::shared_ptr<session_buffer>& buffer);

    asio::awaitable<std::string> create_mpu(std::string& key);
    asio::awaitable<bool> abort_mpu(std::string& upload_id);
    asio::awaitable<std::string> complete_mpu(std::string& upload_id, std::vector<int>& parts);

private:
    Store& store_;
    std::string name_;
    uint64_t owner_;
    std::string prefix_;
    time_t created_at_;

    std::unordered_map<std::string, Multipart> mpus_;
    BucketStats stats_; // we won't use this unless we've a persistent index

    void generate_prefix();
    static std::string generate_upload_id();

};