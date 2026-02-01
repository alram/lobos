#include <boost/url.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include "s3_op_handler.hpp"

std::string generate_upload_id() {
    unsigned char buf[16];
    RAND_bytes(buf, 16);
    std::stringstream ss;
    for (int i = 0; i < 16; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)buf[i];
    }
    return ss.str();
}

asio::awaitable<http::message_generator> S3OpHandler::handle_head() {
    if (key_.empty()) {
        http::response<http::string_body> res{http::status::ok, req_.version()};
        res.set(http::field::server, lobos::http::server_name);
        res.insert("x-amz-bucket-region", "lobos");
        res.keep_alive(req_.keep_alive());
        co_return res;
    }
    auto [size, last_modified] = co_await store_.do_metadata_req(key_);

    if (last_modified == 0 && size == 0)
        co_return key_not_found_res();

    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, mime_type(key_));
    boost::string_view sv(std::to_string(last_modified));
    res.set(http::field::last_modified, sv);
    res.content_length(size);
    res.keep_alive(req_.keep_alive());
    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::handle_get() {
    if (req_[lobos::s3::bucket].empty()) {
        co_return co_await ok_list_all_buckets();
    }

    if (key_.empty()) {
        // removed aws_params.contains.empty() on 
        // purpose since now it should be handled by
        // bucket detect
        if (query_params_.contains("versioning") || 
            query_params_.contains("object-lock") || 
            query_params_.contains("max-buckets") ||
            query_params_.contains("uploads")) {
            co_return co_await ok_bucket_ops();
        }

        // This is a list objects
        co_return co_await ok_list_objects();
    }

    if (query_params_.contains("uploadId")) {
        if (!active_mpus_.contains(query_params_["uploadId"]))
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist");
        co_return co_await ok_list_mpu_parts();
    }
    // This is a get object probably?
    co_return co_await s3_get_object();
}

asio::awaitable<http::message_generator> S3OpHandler::handle_put() {
    bool is_mpu = false;
    if (query_params_.contains("partNumber") || query_params_.contains("uploadId")) {
        is_mpu = true;
        if(!query_params_.contains("partNumber"))
            co_return bad_request_res("InvalidRequest", "Missing required parameter: partNumber");
        if(!query_params_.contains("uploadId"))
            co_return bad_request_res("InvalidRequest", "Missing required parameter: upload_id");
        int part_number = std::stoi(query_params_["partNumber"]);
        if (part_number < 1 || part_number > 10000)
            co_return bad_request_res("InvalidRequest", "partNumber must be between 1 and 10000");
        auto it = active_mpus_.find(query_params_["uploadId"]);;
        if (it == active_mpus_.end())
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist.");
        // TODO min body size for MPU
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        // We manipulate key to make it a mpu identifiable object in the backend
        key_ = ".__lobos__mpus__/" + query_params_["uploadId"] + "_" + query_params_["partNumber"] + "_" + key_;
    }
    auto ret = co_await store_.do_write(key_, *buffer_);
    if (ret < 0)
        co_return internal_error_res();

    auto etag = md5_hex(buffer_->data(), buffer_->size());
    if (is_mpu) {
        Part p {
            buffer_->size(),
            etag,
        };
        int part_n = std::stoi(query_params_["partNumber"]);
        active_mpus_[query_params_["uploadId"]].parts.insert(std::pair<int,Part>(part_n, p));
        active_mpus_[query_params_["uploadId"]].current_size += ret;
    }

    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::etag, "\"" + etag + "\"" );
    res.insert("x-amz-object-size", std::to_string(req_.body().size));
    res.keep_alive(req_.keep_alive());
    res.prepare_payload();

    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::handle_post() {
    // new mpu
    if (query_params_.contains("uploads")) {
        auto upload_id = generate_upload_id();
        co_await store_.do_create_mpu(key_, upload_id);
        Multipart mp{
            key_,
            std::time(nullptr),
            0,
        };
        active_mpus_.insert(std::pair<std::string, Multipart>(upload_id, mp));

        http::response<http::string_body> res{http::status::ok, req_.version()};
        res.set(http::field::server, lobos::http::server_name);
        res.keep_alive(req_.keep_alive());
        res.body() =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<InitiateMultipartUploadResult>"
            "<Bucket>" + std::string(req_[lobos::s3::bucket]) + "</Bucket>"
            "<Key>" + key_ + "</Key>"
            "<UploadId>"+ upload_id + "</UploadId>"
            "</InitiateMultipartUploadResult>";

        res.prepare_payload();
        co_return res;
    }
    // complete mpu
    if (query_params_.contains("uploadId")) {
        beast::string_view upload_id = query_params_["uploadId"];
        if (!active_mpus_.contains(upload_id))
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist");
        if (key_ != active_mpus_[upload_id].key)
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist");

        co_return co_await complete_mpu();
    }
}

asio::awaitable<http::message_generator> S3OpHandler::handle_delete() {
    if (query_params_.contains("uploadId")) {
        auto upload_id = query_params_["uploadId"];
        if (!active_mpus_.contains(upload_id))
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist");
        if (key_ != active_mpus_[upload_id].key)
            co_return bad_request_res("NoSuchUpload", "The specified upload does not exist");

        co_await store_.do_abort_mpu(upload_id, active_mpus_[upload_id]);
        active_mpus_.erase(upload_id);
    } else {
        auto deleted = co_await store_.do_delete(key_);
        if (!deleted)
            co_return key_not_found_res();
    }
    http::response<http::string_body> res{http::status::no_content, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.keep_alive(req_.keep_alive());
    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::ok_bucket_ops() {
    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req_.keep_alive());

    if (query_params_.contains("versioning")) {
        res.body() =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<VersioningConfiguration>"
            "<Status>Suspended</Status>"
            "<MfaDelete>Disabled</MfaDelete>"
            "</VersioningConfiguration>";
    } else if (query_params_.contains("object-lock")) {
        res.body() = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<ObjectLockConfiguration></ObjectLockConfiguration>";
    } else if (query_params_.contains("uploads")) {
        std::string s = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Bucket>" + std::string(req_[lobos::s3::bucket]) + "</Bucket>";
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
        co_return bad_request_res("InvalidRequest", "This shouldn't happen but here we are");
    }
    res.prepare_payload();

    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::ok_list_all_buckets() {
    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req_.keep_alive());
    res.body() = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListAllMyBucketsResult><Buckets>"
        "<Bucket>"
        "<BucketRegion>lobos</BucketRegion>"
        "<CreationDate>1970-01-01T00:00:00+00:00</CreationDate>"
        "<Name>TODO for loop here when all buckets are impl</Name>"
        "</Bucket>"
        "</Buckets>"
        "<Owner><ID>lobos</ID></Owner>"
        "</ListAllMyBucketsResult>";

    res.prepare_payload();
    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::ok_list_objects() {
    beast::string_view prefix = query_params_["prefix"];

    // idk man 1k feels like plenty ¯\_(ツ)_/¯ TODO
    buffer_->reserve(1024);
    std::string h = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>" + std::string(req_[lobos::s3::bucket]) + "</Name>"
        "<Prefix>" + std::string(prefix) + "</Prefix>"
        "<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>";
    buffer_->append(h);
    co_await store_.do_list(prefix, *buffer_);
    std::string f = "<Marker></Marker></ListBucketResult>";
    buffer_->append(f);
    
    http::response<http::buffer_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, "application/xml");
    
    res.body().data = buffer_->data();
    res.body().size = buffer_->size();
    res.body().more = false;

    res.prepare_payload();
    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::ok_list_mpu_parts() {
    auto upload_id = query_params_["uploadId"];

    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(req_.keep_alive());
    std::string s =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListPartsResult>"
        "<Bucket>"+ std::string(req_[lobos::s3::bucket]) +"</Bucket>"
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

asio::awaitable<http::message_generator> S3OpHandler::s3_get_object() {
    auto [size_md, last_modified] = co_await store_.do_metadata_req(key_);
    if (last_modified == 0)
        co_return key_not_found_res();

    // Check if it has a range header
    size_t offset = 0;
    size_t read_end = 0;
    auto const it = req_.find(http::field::range);
    if (it != req_.end()) {
        if(!it->value().starts_with("bytes="))
            co_return bad_request_res("InvalidRequest", "Specified Range request is invalid");

        beast::string_view range = it->value().substr(strlen("bytes="));
        auto pos = range.find("-");
        if (pos == beast::string_view::npos)
            co_return bad_request_res("InvalidRequest", "Specified Range request is invalid");
        offset = std::stoi(range.substr(0, pos));
        // it's valid to have a range like bytes=<offset>-
        // so we check if empty;
        auto read_end_sv = range.substr(pos+1);
        if(!read_end_sv.empty())
            read_end = std::stoi(read_end_sv);
    }

    size_t size = 0;
    // TODO this should be a 416 but i need to refactor the whole error stuff
    if (offset > size_md)
        co_return bad_request_res("InvalidRequest", "Specified Range request is invalid");

    if (read_end == 0) {
        size = size_md - offset;
    } else {
        size = read_end - offset + 1; //inclusive so we add +1
    }
    buffer_->resize_clear(size);

    //todo check return
    co_await store_.do_read(key_, offset, *buffer_);

    http::response<http::buffer_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, mime_type(key_));
    res.set(http::field::last_modified, to_rfc1123(last_modified));

    res.body().data = buffer_->data();
    res.body().size = buffer_->size();
    res.body().more = false;

    res.prepare_payload();

    co_return res;
}

asio::awaitable<http::message_generator> S3OpHandler::complete_mpu() {
    std::string xml(reinterpret_cast<const char*>(buffer_->data()), buffer_->size());
    beast::string_view upload_id = query_params_["uploadId"];
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
                    "The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.");
            parts.push_back(part_n);
        }
    }
    auto rc = co_await store_.do_assemble_mpu(upload_id, active_mpus_[upload_id], parts);
    if (rc < 0)
        co_return internal_error_res();
    std::string key = active_mpus_[upload_id].key;
    active_mpus_.erase(upload_id);

    http::response<http::string_body> res{http::status::ok, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.keep_alive(req_.keep_alive());
    res.body() =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        " <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Bucket>" + std::string(req_[lobos::s3::bucket]) + "</Bucket>"
        "<Key>" + key + "</Key>"
        "</CompleteMultipartUploadResult>";

    res.prepare_payload();
    co_return res;
}

http::message_generator S3OpHandler::bad_request_res(std::string code, std::string msg) {
    http::response<http::string_body> res{http::status::bad_request, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
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

http::message_generator S3OpHandler::key_not_found_res() {
    http::response<http::string_body> res{http::status::not_found, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.set(http::field::content_type, "application/xml");
    res.keep_alive(false);
    res.body() = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NoSuchKey</Code>"
        "<Message>The resource you requested does not exist</Message>"
        "<Resource>" + key_ + "</Resource>"
        "<RequestId>DEADBEEF</RequestId>";
    res.prepare_payload();
    return res;
}

http::message_generator S3OpHandler::internal_error_res() {
    http::response<http::string_body> res{http::status::service_unavailable, req_.version()};
    res.set(http::field::server, lobos::http::server_name);
    res.keep_alive(req_.keep_alive());
    res.prepare_payload();
    return res;
}