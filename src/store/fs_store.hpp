#pragma once

#include <iostream>
#include <fcntl.h>
#include <map>

#include <boost/asio/awaitable.hpp>

#include "store.hpp"
#include "../index/index.hpp"

namespace asio = boost::asio;

// I'm trying REALLY hard to not name this filestore...
class FsStore : public Store {
public:
    explicit FsStore(bool use_index) {
        if (use_index)
            index_ = std::make_unique<IndexStore<ObjectBase>>();
    };

    void init_store(std::string) override;
    asio::awaitable<int> do_write(std::string& oid, session_buffer& buffer) override;
    asio::awaitable<int> do_read(std::string& oid, uint64_t offset, session_buffer& buffer) override;
    asio::awaitable<int> do_delete(std::string& oid) override;
    asio::awaitable<void> do_list(std::string& prefix, session_buffer& buffer) override;
    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string& oid) override;
    void shutdown_store() override {};
    // Buckets
    asio::awaitable<bool> create_bucket(std::string& key, BucketMetadata& md) override;
    asio::awaitable<int> delete_bucket(std::string& bucket) override;
    std::vector<BucketRecord> load_buckets() override;
    // MPU
    asio::awaitable<int> do_create_mpu(std::string& oid, std::string& upload_id)override;
    std::unordered_map<std::string, std::unordered_map<std::string,Multipart>> get_active_mpus() override;
    asio::awaitable<int> do_assemble_mpu(std::string& bucket, std::string& upload_id, Multipart& mp, std::vector<int>& parts) override;
    asio::awaitable<int> do_abort_mpu(std::string& oid, std::string& bucket, std::string& upload_id, Multipart& mp) override;
    // Control plane
    int metadata_add_user(User u) override;
    std::vector<User> metadata_list_users(std::string filter) override;
    bool metadata_remove_user(std::string& name) override;
    int metadata_add_key(User u) override;
    bool metadata_rm_key(std::string user, User u) override;

private:
    std::unique_ptr<IndexStore<ObjectBase>> index_ = nullptr;
    void create_dest_dirs_if_not_exist(const std::string& oid);
    void build_index_at_boot();
};