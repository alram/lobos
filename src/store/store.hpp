#pragma once

#include <span>

#include "buffer.hpp"
#include "../common/common.hpp" 

namespace asio = boost::asio;

class Store {
public:
    virtual ~Store() = default;
    virtual void init_store(std::string s) = 0;
    virtual asio::awaitable<int> do_write(std::string o, session_buffer& buffer) = 0;
    virtual asio::awaitable<int> do_read(std::string o, uint64_t offset, session_buffer& buffer) = 0;
    virtual asio::awaitable<bool> do_delete(std::string_view o) = 0;
    virtual asio::awaitable<void> do_list(std::string_view prefix, session_buffer& buffer) = 0;
    virtual asio::awaitable<std::tuple<size_t, time_t>> do_metadata_req(std::string_view o) = 0;
    virtual void shutdown_store() = 0;
    // MPU stuff
    virtual asio::awaitable<int> do_create_mpu(std::string_view o, std::string upload_id) = 0;
    virtual std::unordered_map<std::string, Multipart> get_active_mpus() = 0;
    virtual asio::awaitable<int> do_assemble_mpu(std::string upload_id, Multipart mp, std::vector<int> parts) = 0;
    virtual asio::awaitable<int> do_abort_mpu(std::string upload_id, Multipart mp) = 0;\
protected:
    std::string lobos_state_prefix = ".__lobos__";
    std::string lobos_mpu_prefix = lobos_state_prefix + "mpus__";
};
