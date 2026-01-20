#pragma once

#include <iostream>
#include <fcntl.h>

#include <boost/asio/awaitable.hpp>

#include "store.hpp"

namespace asio = boost::asio;

// I'm trying REALLY hard to not name this filestore...
class FsStore : public Store {
    void init_store(std::string devSpec=nullptr) override {
        backend = "filesystem";
    };
    asio::awaitable<size_t> do_write(std::string o, std::span<uint8_t> data) override;
    asio::awaitable<int> do_read(std::string o, uint64_t offset, std::span<uint8_t> buffer)override;
    asio::awaitable<bool> do_delete(std::string_view o) override;
    void shutdown_store() override {}; //nothing to do here
    asio::awaitable<void> do_list(std::string_view prefix, std::vector<uint8_t>& buffer) override;

    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) override;

    static std::string create_dest_dirs_if_not_exist(std::string object);
};