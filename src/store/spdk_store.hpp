#pragma once

#include <thread>
#include <future>
#include <coroutine>
#include <iostream>
#include <ctime>
#include <unordered_map>
#include <span>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include "store.hpp"
#include "../index/index.hpp"
#include "spdk_stats.hpp"

extern "C" {
#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/blob_bdev.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/log.h>
#include <spdk/string.h>
}

namespace asio = boost::asio;

struct SpdkReactorConf {
    std::string log_level;
    bool intr_mode;
    int reactor_core;
};

class SpdkReactor {
public:
    SpdkReactor(SpdkReactorConf c) {
        conf = c;
        reactor_thread = std::thread([this] {
            spdk_app_opts opts{};
            spdk_app_opts_init(&opts, sizeof(opts));
            opts.name = "lobos_spdk";
            if (conf.log_level == "debug")
                opts.print_level = SPDK_LOG_DEBUG;
            if (conf.intr_mode)
                opts.interrupt_mode = conf.intr_mode;
            if (conf.reactor_core)
                opts.lcore_map = std::to_string(conf.reactor_core).c_str();
            spdk_app_start(&opts, [](void* arg) {
                auto self = static_cast<SpdkReactor*>(arg);
                self->t = spdk_get_thread();
                self->ready.set_value();
            }, this);
        });
        ready.get_future().wait();
    }

    ~SpdkReactor() {}

    void stop() {
        spdk_app_stop(0);
    }

    void join() {
        if (reactor_thread.joinable()) {
            reactor_thread.join();
        }
    }

    spdk_thread* get_thread() const { return t; }
private:
    SpdkReactorConf conf;
    std::thread reactor_thread;
    std::promise<void> ready;
    spdk_thread* t = nullptr;
};

struct BlobMetadata {
    time_t last_modified;
    size_t size;
}__attribute__((packed));

struct IoCtx {
    std::string key;
    session_buffer* buffer;
    std::unique_ptr<BlobMetadata> md;

    spdk_blob_id blob_id;
    spdk_blob* blob;
    uint64_t offset = 0;

    // defines whether we close the blob on 
    // successful write
    bool close = true;

    IoCtx() = default;

    IoCtx(session_buffer& buf)
        :
        buffer(&buf)
    {}
};

struct BlobStoreConfig {
    size_t cluster_sz;
    uint64_t max_use_pct;
};

class SpdkStore : public Store {
public:
    explicit SpdkStore(SpdkReactor* spdk_reactor, BlobStoreConfig c) : spdk_reactor_(spdk_reactor) {
        conf_ = c;
    };

    void init_store(std::string devSpec) override;
    asio::awaitable<int> do_write(std::string& object, session_buffer& buffer) override;
    asio::awaitable<int> create_and_size_blob(IoCtx* ioctx);
    asio::awaitable<int> write_data(IoCtx* ioctx);
    asio::awaitable<int> do_read(std::string& object, uint64_t offset, session_buffer& buffer) override;
    asio::awaitable<bool> do_delete(std::string& object) override;
    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string& object) override;
    asio::awaitable<void> do_list(std::string& prefix, session_buffer& buffer) override;
    // Buckets
    asio::awaitable<bool> create_bucket(std::string& key, BucketMetadata& md) override;
    asio::awaitable<int> delete_bucket(std::string& bucket) override;
    std::vector<BucketRecord> load_buckets() override;
    // MPU
    asio::awaitable<int> do_create_mpu(std::string& object, std::string& upload_id) override;
    std::unordered_map<std::string, std::unordered_map<std::string,Multipart>> get_active_mpus() override;
    asio::awaitable<int> do_assemble_mpu(std::string& bucket, std::string& upload_id, Multipart& mp, std::vector<int>& parts) override;
    asio::awaitable<int> do_abort_mpu(std::string& object, std::string& bucket, std::string& upload_id, Multipart& mp)override;
    void shutdown_store() override;
    // Metadata
    int metadata_add_user(User u) override;
    std::vector<User> metadata_list_users(std::string filter) override;
    bool metadata_remove_user(std::string& name) override;
    int metadata_add_key(User u) override;
    bool metadata_rm_key(std::string user, User u) override;

    ~SpdkStore() {}

private:
    SpdkReactor* spdk_reactor_;
    spdk_bs_dev* bdev_ = nullptr;
    spdk_blob_store* bs_ = nullptr;
    spdk_io_channel* io_channel_ = nullptr;
    uint64_t io_unit_size_;
    std::unique_ptr<IndexStore> index_ = nullptr;
    spdk_blob_id user_blob_id_;
    spdk_blob_id bucket_blob_id_;

    BlobStoreConfig conf_;
    std::atomic<bool> index_ready = false;
    std::atomic<bool> store_shutdown = false;

    std::unique_ptr<SpdkStats> stats_;
    bool read_only = false;
    std::thread fill_check_thread_;
    std::atomic<bool> run_fill_checker_ = true;
    void make_store_ro_if_full_checker() {
        fill_check_thread_= std::thread([this] {
            while (run_fill_checker_) {
                std::this_thread::sleep_for(std::chrono::seconds(SPDK_STATS_UPDATE_INTERVAL_SEC));
                if (stats_->get_store_percent_used() > conf_.max_use_pct)
                    read_only = true;
            }
        });
    }
    void shutdown_fill_checker() {
        std::cout << "Shutting down fill checker" << std::endl;
        run_fill_checker_ = false;
        if (fill_check_thread_.joinable())
            fill_check_thread_.join();
    }

    void build_index_at_boot();
    static void iter_cb(void *cb_arg, struct spdk_blob *blb, int bserrno);
    static void get_blob_metadata(spdk_blob* blob, Object* o, const char*& key);
    void do_delete_async(spdk_blob_id blobid);
    void create_or_load_state_objects(std::string lobos_prefix);
    bool metadata_remove_user_keys(std::string& name, std::string key);
};

struct BlobOpCtx {
    SpdkStore* store;
    IoCtx* ioctx;
    std::function<void(int)> complete;

    ~BlobOpCtx() {
        delete ioctx;
    }
};

struct UserOpCtx {
    SpdkStore* store;
    std::vector<User>* users;
    spdk_blob* blob;
    bool complete{false};
};

struct BucketCRUDOpCtx {
    SpdkStore* store;
    std::string key;
    BucketMetadata md;
    spdk_blob* blob;
    std::function<void(int)> complete;
};

struct BucketsListOpCtx {
    SpdkStore* store;
    std::vector<BucketRecord>* buckets;
    bool complete;
};