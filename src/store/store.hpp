#pragma once

#include <span>

#include <boost/asio/awaitable.hpp>

#include "buffer.hpp"
#include "../common/common.hpp" 
#include "../index/index.hpp"

namespace asio = boost::asio;

class Store {
public:
    virtual ~Store() = default;
    virtual void init_store(std::string devSpec) = 0;
    virtual asio::awaitable<int> do_write(std::string& object, session_buffer& buffer) = 0;
    virtual asio::awaitable<int> do_read(std::string& object, uint64_t offset, session_buffer& buffer) = 0;
    virtual asio::awaitable<int> do_delete(std::string& object) = 0;
    virtual asio::awaitable<std::map<std::string, ObjectBase>> do_list(std::string& prefix) = 0;
    virtual asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string& object) = 0;
    virtual void shutdown_store() = 0;
    // Bucket ops
    virtual asio::awaitable<bool> create_bucket(std::string& object, BucketMetadata& md) = 0;
    virtual asio::awaitable<int> delete_bucket(std::string& bucket) = 0;
    virtual std::vector<BucketRecord> load_buckets() = 0;
    // MPU stuff
    virtual asio::awaitable<int> do_create_mpu(std::string& object, std::string& upload_id) = 0;
    virtual std::unordered_map<std::string, std::unordered_map<std::string,Multipart>>  get_active_mpus() = 0;
    virtual asio::awaitable<int> do_assemble_mpu(std::string& bucket, std::string& upload_id, Multipart& mp, std::vector<int>& parts) = 0;
    virtual asio::awaitable<int> do_abort_mpu(std::string& object, std::string& bucket, std::string& upload_id, Multipart& mp) = 0;
    // Control plane stuff
    virtual int metadata_add_user(User u) = 0;
    virtual std::vector<User> metadata_list_users(std::string filter) = 0;
    virtual bool metadata_remove_user(std::string& name) = 0;
    virtual int metadata_add_key(User u) = 0;
    virtual bool metadata_rm_key(std::string user, User u) = 0;
protected:
    const std::string lobos_state_prefix = ".__lobos__";
    const std::string lobos_bucket_prefix = lobos_state_prefix + "bucket__";
    const std::string lobos_mpu_prefix = lobos_state_prefix + "mpu__";
    const std::string lobos_user_prefix = lobos_state_prefix + "user__";
};
