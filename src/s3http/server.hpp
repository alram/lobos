
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/config.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>

#include "../index/index.hpp"
#include "../store/store.hpp"


namespace beast = boost::beast;
namespace http  = beast::http;
namespace asio   = boost::asio;


class S3HttpServer {
    public:
        explicit S3HttpServer(
            std::string address,
            unsigned short port, 
            std::string dir, 
            Store* store
        )
            : store_(store)
        {
            auto const addr = asio::ip::make_address(address);
            endpoint = {addr, port};

            // Bucket name is the last dir passed
            // or lobos in non-fs mode
            if (dir.back() == '/')
                dir.pop_back();
            auto const pos = dir.rfind("/");
            bucket_name = dir.substr(pos + 1);

        }
        ~S3HttpServer() {}; 

        void start(int threads, std::vector<int> pins);
    private:
        Store* store_;

        asio::ip::tcp::endpoint endpoint;
        std::string bucket_name;

        asio::awaitable<void> do_listen(asio::ip::tcp::endpoint ep);
        asio::awaitable<void> do_session(beast::tcp_stream stream);
        asio::awaitable<http::message_generator> handle_request(
            http::request<http::buffer_body>&& req, 
            std::shared_ptr<std::vector<uint8_t>> session_buffer);

        void sanitize_target_path(std::string& target);
        bool parse_aws_params(std::string_view t, std::unordered_map<std::string, std::string>& aws_params);
        static std::string to_rfc1123(time_t t);
        static beast::string_view mime_type(beast::string_view path);
        std::string create_dest_dirs_if_not_exist(std::string object);

        asio::awaitable<http::message_generator> handle_get_object(beast::string_view object, 
            http::request<http::buffer_body>&& req, 
            std::shared_ptr<std::vector<uint8_t>> session_buffer);
        asio::awaitable<http::message_generator> handle_head_object(beast::string_view object, 
            http::request<http::buffer_body>&& req);
        asio::awaitable<http::message_generator> handle_list_objects(beast::string_view prefix, 
            http::request<http::buffer_body>&& req, 
            std::shared_ptr<std::vector<uint8_t>> session_buffer);
        asio::awaitable<http::message_generator> handle_put_object(beast::string_view object, 
            http::request<http::buffer_body>&& req);

        http::message_generator not_found_bucket_res(beast::string_view bucket, http::request<http::buffer_body>&& req);
        http::message_generator not_found_key_res(beast::string_view object, http::request<http::buffer_body>&& req);

};