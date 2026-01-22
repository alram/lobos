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
    size_t size;
    spdk_blob* blob;

    IoCtx() = default;

    IoCtx(session_buffer& buf)
        :
        size(buf.size()),
        buffer(&buf)
    {}
};

struct BlobStoreConfig {
    size_t cluster_sz;
    uint64_t max_use_pct;
};

struct SpdkBSStats {
    uint64_t total_clusters;
    uint64_t available_clusters;
};


class SpdkStore : public Store {
public:
    explicit SpdkStore(SpdkReactor* spdk_reactor, BlobStoreConfig c) : spdk_reactor_(spdk_reactor) {
        conf_ = c;
        stats_ = std::make_unique<SpdkBSStats>();
    };

    void init_store(std::string devSpec) override;
    asio::awaitable<int> do_write(std::string o, session_buffer& buffer) override;
    asio::awaitable<int> do_read(std::string o, uint64_t offset, session_buffer& buffer) override;
    asio::awaitable<bool> do_delete(std::string_view o) override;
    void shutdown_store() override { shutdown_blobstore(); };
    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) override;
    asio::awaitable<void> do_list(std::string_view prefix, session_buffer& buffer) override;
    
    void shutdown_blobstore();
    void build_index_at_boot();
    static void iter_cb(void *cb_arg, struct spdk_blob *blb, int bserrno);
    static void get_blob_metadata(spdk_blob* blob, Object* o, const char*& key);
    void do_delete_async(spdk_blob_id blobid);

    void start_stats_engine();
    void update_stats();
    void lock_store_if_full();

    ~SpdkStore() {
        run_stats_engine_ = false;
        if (stats_t_.joinable())
            stats_t_.join();
    }

private:
    SpdkReactor* spdk_reactor_;
    
    spdk_blob_store* bs_ = nullptr;
    spdk_io_channel* io_channel_ = nullptr;
    uint64_t io_unit_size_;
    std::unique_ptr<IndexStore> index_ = nullptr;

    BlobStoreConfig conf_;
    std::atomic<bool> index_ready = false;
    std::atomic<bool> store_ready = false;

    std::unique_ptr<SpdkBSStats> stats_;
    std::thread stats_t_;
    std::atomic<bool> run_stats_engine_ = true;
    std::atomic<bool> stats_updating = false;
    bool read_only = false;
};

struct BlobOpCtx {
    SpdkStore* store;
    IoCtx* ioctx;
    std::function<void(int)> complete;

    ~BlobOpCtx() {
        delete ioctx;
    }
};