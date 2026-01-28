#include <tuple>
#include <pthread.h>
#include <sched.h>
#include <set>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/url.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/evp.h>

#include "server.hpp"

#define SERVER_NAME "LOBOS BB"
// Set to ext4 max file size (16TiB)
#define MAX_OBJ_SIZE 16ULL<<40
#define PATH_DELIM '/'

namespace beast = boost::beast;
namespace http  = beast::http;
namespace asio   = boost::asio;
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

std::string sha256_hex(const std::string& input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.c_str()), input.size(), hash);

    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

std::vector<unsigned char> hmac_sha256(const std::string& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int len;

    HMAC(EVP_sha256(),
         key.c_str(), key.size(),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &len);

    return std::vector<unsigned char>(hash, hash + len);
}

std::vector<unsigned char> hmac_sha256(const std::vector<unsigned char>& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int len;

    HMAC(EVP_sha256(),
         key.data(), key.size(),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &len);

    return std::vector<unsigned char>(hash, hash + len);
}

std::string hmactohex(const std::vector<unsigned char>& key) {
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)key[i];
    }
    return ss.str();
}

std::string md5_hex(const uint8_t* data, size_t len) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;

    // Create a message digest context
    EVP_MD_CTX* context = EVP_MD_CTX_new();

    if (context != nullptr) {
        if (EVP_DigestInit_ex(context, EVP_md5(), nullptr) &&
            EVP_DigestUpdate(context, data, len) &&
            EVP_DigestFinal_ex(context, hash, &hash_len)) {
        }
        EVP_MD_CTX_free(context);
    }

    std::stringstream ss;
    for (unsigned int i = 0; i < hash_len; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

asio::awaitable<bool> S3HttpServer::auth_request(const http::request<http::buffer_body>& req) {
    if (conf_.auth_enabled) {
        auto auth = req["Authorization"];
        if(auth.empty())
            co_return false;

        // Make sure algorigthm is AWS4-HMAC-SHA256
        if (!auth.starts_with("AWS4-HMAC-SHA256"))
            co_return false;
        // Get the credential line
        auto pos_s = auth.find("Credential=") + beast::string_view("Credential=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        auto pos_e = auth.find(",");
        beast::string_view auth_creds = auth.substr(pos_s, pos_e-pos_s);
        // we only support one key/secret for now so we don't store it but
        // in the future if we want ot support multiple we'll change that
        auto in_pos = auth_creds.find("/");
        auth_creds.remove_prefix(in_pos+1);
        in_pos = auth_creds.find('/');
        auto scope_date = auth_creds.substr(0, in_pos);
        auto out_pos = auth_creds.find('/', in_pos+1);
        auto scope_region = auth_creds.substr(in_pos+1, out_pos-in_pos-1);

        auth.remove_prefix(pos_e + 1);

        // Get signed headers
        pos_s = auth.find("SignedHeaders=") + beast::string_view("SignedHeaders=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        pos_e = auth.find(",");
        beast::string_view auth_signedheaders = auth.substr(pos_s, pos_e-pos_s);
        auth.remove_prefix(pos_e + 1);

        // Get signature
        pos_s = auth.find("Signature=") + beast::string_view("Signature=").size();
        if (pos_s == beast::string_view::npos)
            co_return false;
        beast::string_view auth_sign = auth.substr(pos_s);

        // Get path and query params
        beast::string_view target = req.target();
        std::string path;
        std::string query;
        pos_s = target.find('?');
        if (pos_s != beast::string_view::npos) {
            path = target.substr(0, pos_s);
            query = target.substr(pos_s + 1);
        } else {
            path = target;
            query = "";
        }

        // Query params must be in alphabetical order (kill me plz)
        std::set<std::string> s;
        while (true) {
            pos_s = query.find('&');
            // if the query is param with no val, we add a trailing =
            // cause apparently that's how it's done :shrug:
            auto val = query.substr(0, pos_s);
            if (!val.empty() && val.find('=') == beast::string_view::npos)
                val += "=";
            s.insert(val);
            query.erase(0, pos_s+1);
            if (pos_s == beast::string_view::npos) {
                query.erase(0);
                break;
            }
        }

        for (std::string q : s) {
            query += q + "&";
        }
        query.pop_back();        // lazy

        // String to Sha256
        std::string cannonicalRequest =
            std::string(req.method_string()) + "\n" +
            path + "\n" + query + "\n";

        // Extract and store the signed headers for this request
        pos_s = 0;
        std::string signedHeaders = std::string(auth_signedheaders);
        // this could bug i believe
        while (pos_s != beast::string_view::npos) {
            pos_s = auth_signedheaders.find(';');
            auto h = auth_signedheaders.substr(0, pos_s);
            cannonicalRequest += std::string(h) + ":" + std::string(req[h]) + "\n";
            auth_signedheaders.remove_prefix(pos_s+1);
        }

        cannonicalRequest += "\n" + signedHeaders + "\n" + std::string(req["x-amz-content-sha256"]);
        auto shaCannonReq = sha256_hex(cannonicalRequest);

        std::string stringToSign = "AWS4-HMAC-SHA256\n" +
            std::string(req["x-amz-date"]) +
            "\n" + std::string(auth_creds) + "\n"
            + shaCannonReq;

        auto date_key = hmac_sha256("AWS4"+ conf_.secret_key, std::string(scope_date));
        auto region_key = hmac_sha256(date_key, std::string(scope_region));
        auto svc_key = hmac_sha256(region_key, "s3");
        auto signing_key = hmac_sha256(svc_key, "aws4_request");
        auto calc = hmac_sha256(signing_key, stringToSign);

        if (hmactohex(calc) != auth_sign)
            co_return false;

    }
    co_return true;
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

    // the query params are stored
    // into aws_params by now so we just strip 'em
    auto pos = target.find('?');
    if (pos != beast::string_view::npos) {
        target.erase(pos);
    }

    // We have a /something, erase the /
    if (target.front() == PATH_DELIM)
        target.erase(0, 1);
}

// TODO this isn't used
http::message_generator S3HttpServer::not_found_bucket_res(beast::string_view bucket, http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchBucket</Code>"
        "<Message>The specified bucket does not exist</Message>"
        "<Resource>" + std::string(bucket) + "</Resource>"
        "<RequestId>not available</RequestId></Error>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::not_found_key_res(beast::string_view target, http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchKey</Code>"
        "<Message>The resource you requested does not exist</Message>"
        "<Resource>" + std::string(target) + "</Resource>"
        "<RequestId>DEADBEEF</RequestId>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::forbidden_res(http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::forbidden, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>InvalidAccessKeyId</Code>"
        "<Message></Message>"
        "</Error>";
    res.prepare_payload();
    return res;
}

http::message_generator S3HttpServer::bad_request_res(std::string code, std::string msg, http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::bad_request, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error>"
        "<Code>"+ code +"</Code>"
        "<Message>"+ msg + "</Message>"
        "<RequestId>...</RequestId>"
        "</Error>";
    res.prepare_payload();
    return res;
}

// Apparently on internal server error amzn returns 200 still in some instance:?
// todo look into that
http::message_generator S3HttpServer::internal_err_res(http::request<http::buffer_body>&& req) {
    http::response<http::string_body> res{http::status::internal_server_error, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error>"
        "<Code>InternalError</Code>"
        "<Message>We encountered an internal error. Please try again.</Message>"
        "</Error>";

    res.prepare_payload();
    return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_head_object(beast::string_view object, http::request<http::buffer_body>&& req) {

    auto [size, last_modified] = co_await store_->do_metadata_req(object);

    if (last_modified == 0 && size == 0)
        co_return not_found_key_res(object, std::move(req));

    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    boost::string_view sv(std::to_string(last_modified));
    res.set(http::field::last_modified, sv);
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    co_return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_list_objects(beast::string_view prefix, 
    http::request<http::buffer_body>&& req, std::shared_ptr<session_buffer> session_buffer) {
    // idk man 1k feels like plenty ¯\_(ツ)_/¯ TODO
    session_buffer->reserve(1024);
    std::string h = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>" + bucket_name + "</Name>"
        "<Prefix>" + std::string(prefix) + "</Prefix>"
        "<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>";
    session_buffer->append(h);
    co_await store_->do_list(prefix, *session_buffer);
    std::string f = "<Marker></Marker></ListBucketResult>";
    session_buffer->append(f);
    
    http::response<http::buffer_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, "application/xml");
    
    res.body().data = session_buffer->data();
    res.body().size = session_buffer->size();
    res.body().more = false;

    res.prepare_payload();
    co_return res;
}

// TODO - get/put should handle chunks for large IO however this will require
// a lot of changes so for now we dont
asio::awaitable<http::message_generator> S3HttpServer::handle_get_object(beast::string_view object, http::request<http::buffer_body>&& req, std::shared_ptr<session_buffer> session_buffer) {
    auto [size, last_modified] = co_await store_->do_metadata_req(object);
    
    if (last_modified == 0)
        co_return not_found_key_res(object, std::move(req));
    
    session_buffer->resize(size);
    //todo check return
    co_await store_->do_read(object, 0, *session_buffer);

    http::response<http::buffer_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.set(http::field::content_type, mime_type(object));
    res.set(http::field::last_modified, to_rfc1123(last_modified));

    res.body().data = session_buffer->data();
    res.body().size = session_buffer->size();
    res.body().more = false;

    res.prepare_payload();

    co_return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_complete_mpu(std::string upload_id,
    http::request<http::buffer_body>&& req, std::shared_ptr<session_buffer> session_buffer) {

    std::string xml(reinterpret_cast<const char*>(session_buffer->data()), session_buffer->size());

    std::vector<int> parts;
    boost::property_tree::ptree pt;
    std::istringstream ss(xml);
    boost::property_tree::read_xml(ss, pt);
    // We just store the parts in order the user tells us to
    // assemble them
    for (auto& part : pt.get_child("CompleteMultipartUpload")) {
        if (part.first == "Part") {
            auto part_n = part.second.get<int>("PartNumber");
            if (!active_mpus_[upload_id].parts.contains(part_n))
                co_return bad_request_res("InvalidPart",
                    "One or more of the specified parts could not be found. "
                    "The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
                    , std::move(req));
            parts.push_back(part_n);
        }
    }
    auto rc = co_await store_->do_assemble_mpu(upload_id, active_mpus_[upload_id], parts);
    if (rc < 0)
        co_return internal_err_res(std::move(req));
    std::string key = active_mpus_[upload_id].key;
    active_mpus_.erase(upload_id);

    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.keep_alive(req.keep_alive());
    res.body() =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        " <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Bucket>" + bucket_name + "</Bucket>"
        "<Key>" + key + "</Key>"
        "</CompleteMultipartUploadResult>";

    res.prepare_payload();
    co_return res;
}


std::string generate_upload_id() {
    unsigned char buf[16];
    RAND_bytes(buf, 16);
    std::stringstream ss;
    for (int i = 0; i < 16; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)buf[i];
    }
    return ss.str();
}

asio::awaitable<http::message_generator> S3HttpServer::handle_create_mpu(beast::string_view object,
        http::request<http::buffer_body>&& req) {
    auto upload_id = generate_upload_id();
    co_await store_->do_create_mpu(object, upload_id);
    Multipart mp{
        object,
        std::time(nullptr),
        0,
    };
    active_mpus_.insert(std::pair<std::string, Multipart>(upload_id, mp));

    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::server, SERVER_NAME);
    res.keep_alive(req.keep_alive());
    res.body() =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>" + bucket_name + "</Bucket>"
        "<Key>" + std::string(object) + "</Key>"
        "<UploadId>"+ upload_id + "</UploadId>"
        "</InitiateMultipartUploadResult>";

    res.prepare_payload();
    co_return res;
}

asio::awaitable<http::message_generator> S3HttpServer::handle_request(http::request<http::buffer_body>&& req, 
    std::shared_ptr<session_buffer> session_buffer) {

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
        } else if (aws_params.contains("uploads")) {
            std::string s = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<Bucket>" + bucket_name + "</Bucket>";
            for (auto& it : active_mpus_) {
                s += 
                "<Upload>"
                "<Key>" + it.second.key + "</Key>"
                "<UploadId>" + it.first + "</UploadId>"
                "<Initiated>" + to_rfc1123(it.second.init_time) + "</Initiated>"
                "</Upload>";
            }
            s += "</ListMultipartUploadsResult>";
            res.body() = s;
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
        co_return bad_request_res("InvalidRequest","Malformed request", std::move(req));
    }

    // Ensure only / is used as a delimiter 
    auto it = aws_params.find("delimiter");
    if (it != aws_params.end()) {
        if (!it->second.empty() && it->second != std::string(1, PATH_DELIM)) {
            co_return bad_request_res("InvalidRequest", "/ is the only supported delimiter.", std::move(req));
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
            co_return res;
        }
        co_return co_await handle_head_object(target, std::move(req));
    }

    if (req.method() == http::verb::put) {
        bool is_mpu = false;
        if (aws_params.contains("partNumber") || aws_params.contains("uploadId")) {
            is_mpu = true;
            if(!aws_params.contains("partNumber"))
                co_return bad_request_res("InvalidRequest", "Missing required parameter: partNumber", std::move(req));
            if(!aws_params.contains("uploadId"))
                co_return bad_request_res("InvalidRequest", "Missing required parameter: upload_id", std::move(req));
            int part_number = std::stoi(aws_params["partNumber"]);
            if (part_number < 1 || part_number > 10000)
                co_return bad_request_res("InvalidRequest", "partNumber must be between 1 and 10000", std::move(req));
            auto it = active_mpus_.find(aws_params["uploadId"]);;
            if (it == active_mpus_.end())
                co_return bad_request_res("NoSuchUpload", "The specified upload does not exist.", std::move(req));
            // TODO min body size for MPU
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
            // We manipulate target to make it a mpu identifiable object
            target = ".__lobos__mpus__/" + aws_params["uploadId"] + "_" + aws_params["partNumber"] + "_" + target;
        }
        auto ret = co_await store_->do_write(target, *session_buffer);
        if (ret < 0) {
            http::response<http::string_body> res{http::status::service_unavailable, req.version()};
            res.set(http::field::server, SERVER_NAME);
            res.keep_alive(req.keep_alive());
            res.prepare_payload();
            co_return res;
        }
        auto etag = md5_hex(session_buffer->data(), session_buffer->size());
        if (is_mpu) {
            Part p {
                session_buffer->size(),
                etag,
            };
            int part_n = std::stoi(aws_params["partNumber"]);
            active_mpus_[aws_params["uploadId"]].parts.insert(std::pair<int,Part>(part_n, p));
        }

        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::server, SERVER_NAME);
        res.set(http::field::etag, "\"" + etag + "\"" );
        res.insert("x-amz-object-size", std::to_string(req.body().size));
        res.keep_alive(req.keep_alive());
        res.prepare_payload();

        co_return res;
    }    

    // Now the big one, GET. It gets f'ed up and we use params to target between
    // Bucket reqs and Object reqs. This is most definitely broken and is going
    // to be a pain to go back to but that's for future Alex.
    if (req.method() == http::verb::get) {
        // We're look at the params to figure out what to do
        // this is naive and will not work with listobjectv1
        if (target.empty()) {
            if (aws_params.contains("list-type"))
                co_return co_await handle_list_objects(aws_params["prefix"], std::move(req), session_buffer);
            if (aws_params.contains("versioning") || 
                aws_params.contains("object-lock") || 
                aws_params.contains("max-buckets") ||
                aws_params.contains("uploads") ||
                aws_params.empty())
                co_return bucket_ops_res(aws_params);
        } else {
            if (aws_params.contains("uploadId")) {
                auto upload_id = aws_params["uploadId"];
                if (!active_mpus_.contains(upload_id))
                    co_return bad_request_res("NoSuchUpload", "The specified upload does not exist", std::move(req));
                http::response<http::string_body> res{http::status::ok, req.version()};
                res.set(http::field::server, SERVER_NAME);
                res.set(http::field::content_type, "application/xml");
                res.keep_alive(req.keep_alive());
                std::string s =
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    "<ListPartsResult>"
                    "<Bucket>"+ bucket_name +"</Bucket>"
                    "<Key>"+ active_mpus_[upload_id].key + "</Key>"
                    "<UploadId>" + upload_id + "</UploadId>";
                for (auto& it : active_mpus_[upload_id].parts) {
                    s += 
                        "<Part>"
                        "<PartNumber>" + std::to_string(it.first) + "</PartNumber>"
                        "<Size>" + std::to_string(it.second.size) + "</Size>"
                        "</Part>";
                }
                s += "</ListPartsResult>";
                res.body() = s;
                res.prepare_payload();
                co_return res;
            }
            // This is a get object probably?
            co_return co_await handle_get_object(target, std::move(req), session_buffer);
        }
    }

    if (req.method() == http::verb::delete_) {
        if (aws_params.contains("uploadId")) {
            auto upload_id = aws_params["uploadId"];
            if (!active_mpus_.contains(upload_id))
                co_return bad_request_res("NoSuchUpload", "The specified upload does not exist", std::move(req));
            if (target != active_mpus_[upload_id].key)
                co_return bad_request_res("NoSuchUpload", "The specified upload does not exist", std::move(req));

            co_await store_->do_abort_mpu(upload_id, active_mpus_[upload_id]);
            active_mpus_.erase(upload_id);
        } else {
            auto deleted = co_await store_->do_delete(target);
            if (!deleted)
                co_return not_found_key_res(target, std::move(req));
        }
        co_return delete_object_res();
    }

    if (req.method() == http::verb::post) {
        // new mpu
        if (aws_params.contains("uploads")) {
            co_return co_await handle_create_mpu(target, std::move(req));
        }
        // complete mpu
        if (aws_params.contains("uploadId")) {
            auto upload_id = aws_params["uploadId"];
            if (!active_mpus_.contains(upload_id))
                co_return bad_request_res("NoSuchUpload", "The specified upload does not exist", std::move(req));
            if (target != active_mpus_[upload_id].key)
                co_return bad_request_res("NoSuchUpload", "The specified upload does not exist", std::move(req));

            co_return co_await handle_complete_mpu(upload_id, std::move(req), session_buffer);
        }
    }

    std::cout << "unsupported req: " << req.method() << " " << req.target() << std::endl;
    co_return bad_request_res("InvalidRequest", "unsupported req", std::move(req));
}

// Handles an HTTP server connection
asio::awaitable<void> S3HttpServer::do_session(beast::tcp_stream stream) {
    beast::flat_buffer buffer;
    for(;;)
    {
        auto session_buffer = make_buffer(0);
        // Set timeout
        stream.expires_after(std::chrono::seconds(30));

        http::request_parser<http::buffer_body> parser;
        parser.body_limit(MAX_OBJ_SIZE);
        // Parse headers first for PUT reqs
        co_await http::async_read_header(stream, buffer, parser, asio::use_awaitable);
        auto authed = co_await auth_request(parser.get());
        if (!authed) {
            auto msg = forbidden_res(std::move(parser.release()));
            co_await beast::async_write(stream, std::move(msg), asio::use_awaitable);
            if (!parser.get().keep_alive())
                break;
            continue;
        }

        if (parser.get().method() == http::verb::put || parser.get().method() == http::verb::post) {
            std::string target = parser.get().target();
            sanitize_target_path(target);
            auto content_length = parser.content_length().value_or(0);
            session_buffer->resize(content_length);

            parser.get().body().data = session_buffer->data();
            parser.get().body().size = session_buffer->size();
        }

        co_await http::async_read(stream, buffer, parser, asio::use_awaitable);
        auto req = parser.release();
    
        auto msg = co_await handle_request(std::move(req), session_buffer);

        bool keep_alive = msg.keep_alive();
        co_await beast::async_write(stream, std::move(msg), asio::use_awaitable);

        if (!keep_alive)
            break;
    }

    // Send a TCP shutdown
    stream.socket().shutdown(asio::ip::tcp::socket::shutdown_send);
}

asio::awaitable<void> S3HttpServer::do_listen(asio::ip::tcp::endpoint ep) {
    auto executor = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor{executor};

    acceptor.open(ep.protocol());
    acceptor.set_option(asio::socket_base::reuse_address(true));
#ifdef SO_REUSEPORT
    acceptor.set_option(asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>(true));
#endif
    acceptor.bind(ep);
    acceptor.listen(asio::socket_base::max_listen_connections);

    for (;;) {
        asio::ip::tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
        // no_delay improved throughput by almost 4x on loopback during my benchmarks
        socket.set_option(asio::ip::tcp::no_delay(true));

        auto on_session_except = [](std::exception_ptr e) {
            if (e) {
                try { std::rethrow_exception(e); }
                catch (std::exception const& ex) {
                    std::cerr << "Session error: "
                            << ex.what() << "\n";
                }
            }
        };

        asio::co_spawn(
            executor,
            do_session(beast::tcp_stream{std::move(socket)}), 
            on_session_except);
    }
}

void S3HttpServer::start(int threads, std::vector<int> pins) {
    std::cout << "Starting S3 HTTP server for bucket " << bucket_name << " at " << endpoint << std::endl;
    
    ioctxs_.reserve(threads);

    for (int i = 0; i < threads; ++i)
        ioctxs_.emplace_back(std::make_unique<asio::io_context>(1));

    // Set up signal handling FIRST, before threads start
    signals_ = std::make_unique<asio::signal_set>(*ioctxs_[0], SIGINT, SIGTERM);
    signals_->async_wait([this](boost::system::error_code const&, int sig) {
        std::cout << "Received: " << sig << " - Stopping." << std::endl;
        stop();
        store_->shutdown_store();
    });

    thread_pool_.reserve(threads);

    for (int i = 0; i < threads; i++) {
        thread_pool_.emplace_back([&, i]{

            if (pins.size() > 0)
                pin_thread_to_core(pins[i]);

            asio::co_spawn(
                *ioctxs_[i],
                do_listen(endpoint),
                [](std::exception_ptr e) {
                    if (e) {
                        try { std::rethrow_exception(e); }
                        catch (std::exception const&ex) {
                            std::cerr << "Error " << ex.what() << std::endl;
                        }
                    }
                });
            ioctxs_[i]->run();
        });
    }

    for (auto& t : thread_pool_)
        t.join();
}