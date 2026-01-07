
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


namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;


class S3HttpServer {
    public:
        explicit S3HttpServer(
            std::string address, 
            unsigned short port, 
            std::string dir, 
            IndexStore* index_store
        )
            : index_store_(index_store) 
        {
            auto const addr = net::ip::make_address(address);
            endpoint = {addr, port};

            // Bucket name is the last dir passed 
            if (dir.back() == '/')
                dir.pop_back();
            auto const pos = dir.rfind("/");
            bucket_name = dir.substr(pos + 1);
        }
        ~S3HttpServer() {}; 

        void start(int threads, bool pin);
    private:
        IndexStore* index_store_;

        net::ip::tcp::endpoint endpoint;
        std::string bucket_name;

        net::awaitable<void> do_listen(net::ip::tcp::endpoint ep);
        net::awaitable<void> do_session(beast::tcp_stream stream);
        http::message_generator handle_request(http::request<http::file_body>&& req);


        void sanitize_target_path(std::string& target);
        bool parse_aws_params(std::string_view t, std::unordered_map<std::string, std::string>& aws_params);
        static std::string to_rfc1123(time_t t);
        static beast::string_view mime_type(beast::string_view path);
        std::string create_dest_dirs_if_not_exist(std::string object);
        auto do_metadata_req(boost::string_view path);

        std::string do_list_objects(std::string path);
        http::message_generator handle_get_object(beast::string_view object, http::request<http::file_body>&& req);
        http::message_generator handle_head_object(beast::string_view object, http::request<http::file_body>&& req);
        http::message_generator handle_list_objects(beast::string_view prefix, http::request<http::file_body>&& req);
        http::message_generator handle_put_object(beast::string_view object, http::request<http::file_body>&& req);

        http::message_generator not_found_bucket_res(beast::string_view bucket, http::request<http::file_body>&& req);
        http::message_generator not_found_key_res(beast::string_view object, http::request<http::file_body>&& req);

};