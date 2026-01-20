#pragma once

#include <span>

namespace asio = boost::asio;

class Store {
    public:
        virtual ~Store() = default;
        virtual void init_store(std::string s) = 0;
        virtual void shutdown_store() = 0;

        virtual asio::awaitable<size_t> do_write(std::string o, std::span<uint8_t> data) = 0;
        virtual asio::awaitable<int> do_read(std::string o, uint64_t offset, std::span<uint8_t> buffer) = 0;
        virtual asio::awaitable<bool> do_delete(std::string_view o) = 0;
        virtual asio::awaitable<void> do_list(std::string_view prefix, std::vector<uint8_t>& buffer) = 0;
        virtual asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) = 0;

        std::string backend;
};
