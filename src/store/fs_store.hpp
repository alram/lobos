#pragma once

#include <iostream>
#include <fcntl.h>
#include <map>

#include <boost/asio/awaitable.hpp>

#include "store.hpp"

namespace asio = boost::asio;

// I'm trying REALLY hard to not name this filestore...
class FsStore : public Store {
public:
    void init_store(std::string) override {};
    asio::awaitable<int> do_write(std::string o, session_buffer& buffer) override;
    asio::awaitable<int> do_read(std::string o, uint64_t offset, session_buffer& buffer) override;
    asio::awaitable<bool> do_delete(std::string_view o) override;
    asio::awaitable<void> do_list(std::string_view prefix, session_buffer& buffer) override;
    asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) override;
    void shutdown_store() override {};
    asio::awaitable<int> do_create_mpu(std::string_view o, std::string uploadId) override;
    std::unordered_map<std::string, Multipart> get_active_mpus() override;
    asio::awaitable<int> do_assemble_mpu(std::string upload_id, Multipart mp, std::vector<int> parts) override;
    asio::awaitable<int> do_abort_mpu(std::string upload_id, Multipart mp) override;
private:
    static std::string create_dest_dirs_if_not_exist(std::string object);
};