#include <tuple>
#include <pthread.h>
#include <sched.h>

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/url.hpp>

#include "server.hpp"

#define SERVER_NAME "LOBOS BB"
// Set to ext4 max file size (16TiB)
#define MAX_OBJ_SIZE 16ULL<<40
#define PATH_DELIM '/'

namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;
namespace fs    = boost::filesystem;

void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_setaffinity_np(
        pthread_self(),
        sizeof(cpu_set_t),
        &cpuset
    );
}

// Return a reasonable mime type based on the extension of a file.
beast::string_view S3HttpServer::mime_type(beast::string_view path) {
    using beast::iequals;
    auto const ext = [&path]
    {
        auto const pos = path.rfind(".");
        if(pos == beast::string_view::npos)
            return beast::string_view{};
        return path.substr(pos);
    }();
    if(iequals(ext, ".htm"))  return "text/html";
    if(iequals(ext, ".html")) return "text/html";
    if(iequals(ext, ".php"))  return "text/html";
    if(iequals(ext, ".css"))  return "text/css";
    if(iequals(ext, ".txt"))  return "text/plain";
    if(iequals(ext, ".js"))   return "application/javascript";
    if(iequals(ext, ".json")) return "application/json";
    if(iequals(ext, ".xml"))  return "application/xml";
    if(iequals(ext, ".swf"))  return "application/x-shockwave-flash";
    if(iequals(ext, ".flv"))  return "video/x-flv";
    if(iequals(ext, ".png"))  return "image/png";
    if(iequals(ext, ".jpe"))  return "image/jpeg";
    if(iequals(ext, ".jpeg")) return "image/jpeg";
    if(iequals(ext, ".jpg"))  return "image/jpeg";
    if(iequals(ext, ".gif"))  return "image/gif";
    if(iequals(ext, ".bmp"))  return "image/bmp";
    if(iequals(ext, ".ico"))  return "image/vnd.microsoft.icon";
    if(iequals(ext, ".tiff")) return "image/tiff";
    if(iequals(ext, ".tif"))  return "image/tiff";
    if(iequals(ext, ".svg"))  return "image/svg+xml";
    if(iequals(ext, ".svgz")) return "image/svg+xml";
    return "application/text";
}

std::string S3HttpServer::to_rfc1123(time_t t) {
    std::tm tm{};
    gmtime_r(&t, &tm);

    char buf[30];
    std::strftime(buf, sizeof(buf),
                  "%a, %d %b %Y %H:%M:%S GMT",
                  &tm);
    return buf; 
}

std::string S3HttpServer::do_list_objects(std::string prefix) {
    std::string data;
    data.reserve(8192); //idk man 8k feels plenty provided we do the max-keys 1000 thing
    data.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>" + bucket_name + "</Name>"
        "<Prefix>" + prefix + "</Prefix>"
        "<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
    );
    if(index_store_) {
        std::unordered_set<std::string> seen;
        std::string last_entry;

        auto it = index_store_->index.lower_bound(prefix);
        for (; it != index_store_->index.end(); ++it) {
            if (it->first.compare(0, prefix.size(), prefix) != 0)
                break;
            std::string entry = it->first;
        
            // Prevent from listing recursively
            size_t pos = entry.find(PATH_DELIM, prefix.size());
            if (pos != std::string::npos) {
                entry = it->first.substr(0, pos);
                if(entry == last_entry) {
                    continue;
                }
            }
            last_entry = entry;
            if (it->second.type == 'd') {
                data.append(
                    "<CommonPrefixes>"
                    "<Prefix>" + entry + PATH_DELIM + "</Prefix>"
                    "</CommonPrefixes>"
                );
            } else if (seen.insert(entry).second) {
                data.append(
                    "<Contents>"
                    "<Key>" + entry + "</Key>"
                    "<LastModified>" + std::to_string(it->second.last_modified) + "</LastModified>"
                    "<Size>" + std::to_string(it->second.size) + "</Size>"
                    "</Contents>"
                );
            }
        }
    } else {
        // Check if prefix exists. If it doesn't, go one up in the dir hierarchy
        // and list everything that _starts_ with prefix
        fs::path path = prefix;
        if (!fs::exists(path)) {
            auto pos = path.string().find(PATH_DELIM);
            if (pos == beast::string_view::npos) {
                path.clear();
            }
            else {
                path = prefix.substr(0, pos);
                prefix = prefix.substr(pos+1);
            }
        } else {
            prefix.clear();
        }

        if (path.empty())
            path = fs::current_path();

        for (auto& entry : boost::make_iterator_range(fs::directory_iterator(path), {})) {
            if (!prefix.empty()) {
                auto s = path.string() + '/' + prefix;
                if (!entry.path().string().starts_with(s)) {
                    continue;
                }
            }
            if (fs::is_directory(entry.path())) {
                data.append(
                    "<CommonPrefixes>"
                    "<Prefix>" + entry.path().filename().string() + PATH_DELIM + "</Prefix>"
                    "</CommonPrefixes>"
                );
            } else {
                data.append(
                    "<Contents>"
                    "<Key>" + entry.path().filename().string() + "</Key>"
                    "<LastModified>" + std::to_string(fs::last_write_time(entry)) + "</LastModified>"
                    "<Size>" + std::to_string(fs::file_size(entry.path())) + "</Size>"
                    "</Contents>"
            );
            }
        }
    }
    return data.append("<Marker></Marker></ListBucketResult>");
}

auto S3HttpServer::do_metadata_req(boost::string_view path) {
    size_t size;
    time_t last_modified;

    if (index_store_) {
        auto it = index_store_->index.find(path);
        if (it == index_store_->index.end()) {
            size = last_modified = 0;
        } else {
            size = it->second.size;
            last_modified = it->second.last_modified;
        }
    } else {
        try {
            size = fs::file_size(path);
            last_modified = fs::last_write_time(path);
        } catch (const fs::filesystem_error& e) {
            size = last_modified = 0;
        }
    }

    return std::tuple{size, last_modified};
}

bool S3HttpServer::parse_aws_params(std::string_view t, std::unordered_map<std::string, std::string>& aws_params) {
      
    auto target = boost::urls::parse_relative_ref(t);
    if (!target) {
        return false;
    }
    
    boost::urls::url_view u = *target;
    for (auto const& param : u.params()) {
        aws_params.emplace(param.key, param.value);
    }
    return true;
}

void S3HttpServer::sanitize_target_path(std::string& target) {
    if (target.starts_with("/" + bucket_name))
        target.erase(0, bucket_name.size() + 1); // removes `/bucketname`

    // since we know we don't have extra filepath info we remove it all
    if (target.front() == '?')
        target.erase();

    // We have a /something, erase the /
    if (target.front() == PATH_DELIM)
        target.erase(0, 1);
}

std::string S3HttpServer::create_dest_dirs_if_not_exist(std::string object) {
    sanitize_target_path(object);

    bool path_exist = true;
    //We need to ensure all the parents directories exist before anything
    auto pos = object.rfind(PATH_DELIM);
    if (pos != beast::string_view::npos) {
        auto path = object.substr(0, pos);
        if (index_store_) {
            auto it = index_store_->index.find(path);
            if (it == index_store_->index.end())
                path_exist = false;
        } else if (!fs::exists(path)) {
            path_exist = false;
        }
        if (!path_exist) {
            fs::create_directories(path);
            //TODO missing index add;
        }
    } // else this is just `/key` so we don't care? I think?

    return object;
}

// TODO this isn't used
http::message_generator S3HttpServer::not_found_bucket_res(beast::string_view bucket, http::request<http::file_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req.keep_alive());
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchBucket</Code>"
        "<Message>The specified bucket does not exist</Message>"
        "<Resource>" + std::string(bucket) + "</Resource>"
        "<RequestId>not available</RequestId></Error>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::not_found_key_res(beast::string_view target, http::request<http::file_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req.keep_alive());
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchKey</Code>"
        "<Message>The resource you requested does not exist</Message>"
        "<Resource>" + std::string(target) + "</Resource>"
        "<RequestId>DEADBEEF</RequestId>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::handle_head_object(beast::string_view object, http::request<http::file_body>&& req) {

    auto [size, last_modified] = do_metadata_req(object);

    if (last_modified == 0 && size == 0)
        return not_found_key_res(object, std::move(req));

    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    boost::string_view sv(std::to_string(last_modified));
    res.set(http::field::last_modified, sv);
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return res;
}

http::message_generator S3HttpServer::handle_list_objects(beast::string_view prefix, http::request<http::file_body>&& req) {
        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::content_type, "application/xml");
        res.keep_alive(req.keep_alive());
        auto objects =  do_list_objects(prefix);
        res.body() = objects;
        res.prepare_payload();
        return res;
}

http::message_generator S3HttpServer::handle_get_object(beast::string_view object, http::request<http::file_body>&& req) {
        
    auto [_, last_modified] = do_metadata_req(object);

    if (last_modified == 0)
        return not_found_key_res(object, std::move(req));        
        
    http::response<http::file_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    res.set(http::field::last_modified, to_rfc1123(last_modified));

    beast::error_code ec;
    http::file_body::value_type body;

    body.open(std::string(object).c_str(), beast::file_mode::scan, ec);
    if (ec) {
        return not_found_key_res(object, std::move(req));
    }

    res.content_length(body.size());
    res.body() = std::move(body);
    res.keep_alive(req.keep_alive());
    res.prepare_payload();

    return res;
}

http::message_generator S3HttpServer::handle_request(http::request<http::file_body>&& req) {
    // Returns a bad request response
    auto const bad_request_res =
    [&req](beast::string_view why)
    {
        http::response<http::string_body> res{http::status::bad_request, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = std::string(why);
        res.prepare_payload();
        return res;
    };

    auto const delete_object_res = 
    [&req]()
    {
        http::response<http::string_body> res{http::status::no_content, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.keep_alive(req.keep_alive());
        return res;
    };

    auto const bucket_ops_res =
    [&req, this](std::unordered_map<std::string, std::string>& aws_params)
    {
        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::content_type, "application/xml");
        res.keep_alive(req.keep_alive());
        if (aws_params.contains("versioning")) {
            res.body() =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<VersioningConfiguration>"
                "<Status>Suspended</Status>"
                "<MfaDelete>Disabled</MfaDelete>"
                "</VersioningConfiguration>";
        } else if (aws_params.contains("object-lock")) {
            res.body() = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<ObjectLockConfiguration></ObjectLockConfiguration>";
        } else {
            res.body() = 
                "<ListAllMyBucketsResult><Buckets>"
                "<Bucket>"
                "<BucketRegion>lobos</BucketRegion>"
                "<CreationDate>1970-01-01T00:00:00+00:00</CreationDate>"
                "<Name>"+ bucket_name + "</Name>"
                "</Bucket>"
                "</Buckets>"
                "<Owner><ID>lobos</ID></Owner>"
                "</ListAllMyBucketsResult>";
        }
        res.prepare_payload();
        return res;
    };

    //Store aws' s3 url req params
    std::unordered_map<std::string, std::string> aws_params;
    if (!parse_aws_params(req.target(), aws_params)) {
        return bad_request_res("Malformed request");
    }

    // Ensure only / is used as a delimiter 
    auto it = aws_params.find("delimiter");
    if (it != aws_params.end()) {
        if (!it->second.empty() && it->second != std::string(1, PATH_DELIM)) {
            return bad_request_res("/ is the only supported delimiter.");
        }
    }

    std::string target = req.target();
    sanitize_target_path(target);

    // Handles HeadObject/HeadBucket requests
    if (req.method() == http::verb::head) {
        if (target.empty()) {
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::server, SERVER_NAME);
            res.insert("x-amz-bucket-region", "lobos");
            res.keep_alive(req.keep_alive());
            return res;
        }
        return handle_head_object(target, std::move(req));
    }

    if (req.method() == http::verb::put) {
        const auto size = fs::file_size(target);
        if (index_store_) {
            std::time_t now = std::time(nullptr);

            Object o = {
                size,
                now,
                'f',
            };
            index_store_->add_entry(target, o);
        }

        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.insert("x-amz-object-size", std::to_string(size));
        res.content_length(0);
        res.keep_alive(req.keep_alive());
        return res;
    }    

    // Now the big one, GET. It gets f'ed up and we use params to target between
    // Bucket reqs and Object reqs. This is most definitely broken and is going
    // to be a pain to go back to but that's for future Alex.
    if (req.method() == http::verb::get) {
        // We're look at the params to figure out what to do
        // this is naive and will not work with listobjectv1
        if (target.empty()) {
            if (aws_params.contains("list-type"))
                return handle_list_objects(aws_params["prefix"], std::move(req));
            if (aws_params.contains("versioning") || 
                aws_params.contains("object-lock") || 
                aws_params.contains("max-buckets") ||
                aws_params.empty())
                return bucket_ops_res(aws_params);
        } else {
            // This is a get object probably?
            return handle_get_object(target, std::move(req));
        }
    }

    if (req.method() == http::verb::delete_) {
        auto deleted = fs::remove(target);
        if (!deleted)
            return not_found_key_res(target, std::move(req));
        if (index_store_)
            index_store_->index.erase(target);

        return delete_object_res();
    }

    std::cout << "unsupported req: " << req.method() << " " << req.target() << std::endl;
    return bad_request_res("unsupported req");
}

// Handles an HTTP server connection
net::awaitable<void> S3HttpServer::do_session(beast::tcp_stream stream) {
    beast::flat_buffer buffer;

    for(;;)
    {
        // Set timeout
        stream.expires_after(std::chrono::seconds(30));

        http::request_parser<http::file_body> parser;
        parser.body_limit(MAX_OBJ_SIZE);

        // Parse headers first for PUT reqs
        co_await http::async_read_header(stream, buffer, parser);

        if (parser.get().method() == http::verb::put) {
            std::string target = std::string(parser.get().target());
            auto object = create_dest_dirs_if_not_exist(target);
            beast::error_code ec;
            // TODO here we wanna handle checksum that some clients provide
            // it's stored in the body 
            parser.get().body().open(object.c_str(), beast::file_mode::write, ec);
            if (ec) {
                //TODO
            }
        }

        co_await http::async_read(stream, buffer, parser);
        
        auto req = parser.release();
        http::message_generator msg = handle_request(std::move(req));

        bool keep_alive = msg.keep_alive();
        co_await beast::async_write(stream, std::move(msg));

        if (!keep_alive)
            break;
    }

    // Send a TCP shutdown
    stream.socket().shutdown(net::ip::tcp::socket::shutdown_send);
}

net::awaitable<void> S3HttpServer::do_listen(net::ip::tcp::endpoint ep) {
    auto executor = co_await net::this_coro::executor;
    net::ip::tcp::acceptor acceptor{executor};

    acceptor.open(ep.protocol());
    acceptor.set_option(net::socket_base::reuse_address(true));
#ifdef SO_REUSEPORT
    acceptor.set_option(net::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>(true));
#endif
    acceptor.bind(ep);
    acceptor.listen(net::socket_base::max_listen_connections);

    for (;;) {
        net::ip::tcp::socket socket = co_await acceptor.async_accept(net::use_awaitable);
        // no_delay improved throughput by almost 4x on loopback during my benchmarks
        socket.set_option(net::ip::tcp::no_delay(true));

        auto on_session_except = [](std::exception_ptr e) {
            if (e) {
                try { std::rethrow_exception(e); }
                catch (std::exception const& ex) {
                    std::cerr << "Session error: "
                            << ex.what() << "\n";
                }
            }
        };

        net::co_spawn(
            executor,
            do_session(beast::tcp_stream{std::move(socket)}), 
            on_session_except);
    }
}

void S3HttpServer::start(int threads, bool pin) {
    std::cout << "Starting S3 HTTP server for bucket " << bucket_name << " at " << endpoint << std::endl;
    
    std::vector<std::unique_ptr<net::io_context>> ioctxs;
    ioctxs.reserve(threads);

    for (int i = 0; i < threads; ++i)
        ioctxs.emplace_back(std::make_unique<net::io_context>(1));

    std::vector<std::thread> thread_pool;
    thread_pool.reserve(threads);

    for (int i = 0; i < threads; i++) {
        thread_pool.emplace_back([&, i]{

            if (pin)
                pin_thread_to_core(i);

            net::co_spawn(
                *ioctxs[i],
                do_listen(endpoint),
                [](std::exception_ptr e) {
                    if (e) {
                        try { std::rethrow_exception(e); }
                        catch (std::exception const&ex) {
                            std::cerr << "Error " << ex.what() << std::endl;
                        }
                    }
                });
            ioctxs[i]->run();
        });
    }

    for (auto& t : thread_pool)
        t.join();
}