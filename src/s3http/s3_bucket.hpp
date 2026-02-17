#pragma once

#include "../store/store.hpp"

class S3Bucket {
friend class S3OpHandler;
public:
    virtual ~S3Bucket() = default;

    virtual asio::awaitable<bool> create_bucket() = 0;
    virtual asio::awaitable<int> delete_bucket() = 0;
    virtual asio::awaitable<std::tuple<size_t, time_t>> get_object_metadata(std::string& key) = 0;
    virtual asio::awaitable<int> put_object(std::string& key, std::shared_ptr<session_buffer>& buffer) = 0;
    virtual asio::awaitable<int> get_object(std::string& key, uint64_t& offset, std::shared_ptr<session_buffer>& buffer) = 0;
    virtual asio::awaitable<int> delete_object(std::string& key) = 0;
    virtual asio::awaitable<void> list_objects(std::string& prefix, std::shared_ptr<session_buffer>& buffer) = 0;
    virtual asio::awaitable<std::string> create_mpu(std::string& key) = 0;
    virtual asio::awaitable<bool> abort_mpu(std::string& upload_id) = 0;
    virtual asio::awaitable<std::string> complete_mpu(std::string& upload_id, std::vector<int>& parts) = 0;

    // accessors
    virtual const std::string& name() const = 0;
    virtual time_t created_at() const = 0;

    // mpu state
    virtual bool has_mpu(const std::string& upload_id) const = 0;
    virtual const std::string& mpu_key(const std::string& upload_id) const = 0;
    virtual bool mpu_has_part(const std::string& upload_id, int part_n) const = 0;
    virtual void add_mpu_part(const std::string& upload_id, int part_n, const Part& p, const size_t size) = 0;
    virtual const std::map<int, Part>& mpu_parts(const std::string& upload_id) const = 0;

    // mpu iteration (for listing all mpus)
    using mpu_visitor = std::function<void(const std::string& upload_id, const Multipart& mpu)>;
    virtual void for_each_mpu(const mpu_visitor& visitor) const = 0;
};