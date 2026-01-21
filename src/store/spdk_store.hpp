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

struct SpdkAwaiter {
    int rc = 0;
    void* result = nullptr;
    std::coroutine_handle<> h;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> handle) {
        h = handle;
    }
    
    auto await_resume() {
        return std::make_pair(rc, result);
    }

    void complete(int r, void* res = nullptr) {
        rc = r;
        result = res;
        h.resume();
    }
};

class Task {
    public:
        struct promise_type {
            // int value;
            std::exception_ptr error;

            Task get_return_object() {
                return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
            }

            std::suspend_never initial_suspend() { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }

            // void return_value(T v) { value = std::move(v); }
            void return_void() {}
            void unhandled_exception() { error = std::current_exception(); }
        };
        std::coroutine_handle<promise_type> h;
};

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
            // TODO set reactor core from config

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

struct SpdkConfig {
    uint32_t cluster_sz;
};

class SpdkStore : public Store {
public:
    explicit SpdkStore(SpdkReactor* spdk_reactor, SpdkConfig c) : spdk_reactor_(spdk_reactor) {
        conf = c;
    };

    void init_store(std::string devSpec) override;
    asio::awaitable<size_t> do_write(std::string o, session_buffer& buffer) override;
    asio::awaitable<int> do_read(std::string o, uint64_t offset, session_buffer& buffer) override;
    asio::awaitable<bool> do_delete(std::string_view o) override;
    void shutdown_store() override { shutdown_blobstore(); };
    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) override;
    asio::awaitable<void> do_list(std::string_view prefix, session_buffer& buffer) override;
    
    void shutdown_blobstore();
    void build_index_at_boot();
    static void iter_cb(void *cb_arg, struct spdk_blob *blb, int bserrno);
    static void get_blob_metadata(spdk_blob* blob, Object* o, const char*& key);

private:
    spdk_blob_store* bs_ = nullptr;
    spdk_io_channel* io_channel_ = nullptr;
    uint64_t io_unit_size_;
    uint64_t free_clusters_;
    SpdkReactor* spdk_reactor_;
    std::unique_ptr<IndexStore> index_ = nullptr;
    SpdkConfig conf;
    std::atomic<bool> index_ready = false;
    std::atomic<bool> store_ready = false;
};

struct BlobOpCtx {
    SpdkStore* store;
    IoCtx* ioctx;
    std::function<void(int)> complete;

    ~BlobOpCtx() {
        delete ioctx;
    }
};