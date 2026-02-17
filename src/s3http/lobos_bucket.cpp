#include "lobos_bucket.hpp"

std::string LobosBucket::generate_upload_id() {
    unsigned char buf[16];
    RAND_bytes(buf, 16);
    std::stringstream ss;
    for (int i = 0; i < 16; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)buf[i];
    }
    return ss.str();
}

// Buckets are just xattrs of the bucket_object_prefix object
// key: <prefix>_<bucket_name>
// val: owner, created_at, (eventually: stats, acl),
asio::awaitable<bool> LobosBucket::create_bucket(bool shadowed) {
    BucketMetadata md = {
        owner_,
        created_at_,
        shadowed,
    };

    auto created = co_await store_.create_bucket(name_, md);
    co_return created;
}

asio::awaitable<int> LobosBucket::delete_bucket() {

    // Abort all incomplete MPU
    std::vector<std::string> mpus_to_abort;
    if (mpus_.size() > 0) {
        for (auto k : mpus_)
            mpus_to_abort.push_back(k.first);
    }

    for (auto& upload_id : mpus_to_abort) {
        auto aborted = co_await abort_mpu(upload_id);
        if (!aborted)
            co_return -1;
    }

    co_return co_await store_.delete_bucket(name_);
}

asio::awaitable<std::tuple<size_t, time_t>> LobosBucket::get_object_metadata(std::string& key) {
    std::string oid = name_ + "/" + key;
    co_return co_await store_.do_metadata_req(oid);
}

// TODO MPU logic should happen there?
asio::awaitable<int> LobosBucket::put_object(std::string& key, std::shared_ptr<session_buffer>& buffer) {
    std::string oid = name_ + "/" + key;
    co_return co_await store_.do_write(oid, *buffer);
}

asio::awaitable<int> LobosBucket::get_object(std::string& key, uint64_t& offset, std::shared_ptr<session_buffer>& buffer) {
    std::string oid = name_ + "/" + key;
    co_return co_await store_.do_read(oid, offset, *buffer);
}

asio::awaitable<int> LobosBucket::delete_object(std::string& key) {
    std::string oid = name_ + "/" + key;
    co_return co_await store_.do_delete(oid);
}

asio::awaitable<void> LobosBucket::list_objects(std::string& prefix, std::shared_ptr<session_buffer>& buffer) {
    std::string pre = name_ + "/" + prefix;
    // co_return co_await store_.do_list(pre, *buffer);
    auto objs = co_await store_.do_list(pre);
    for (auto& obj : objs) {
        // these should already have been filtered out
        if (!obj.second.list)
            continue;
        std::string s;
        if (obj.first.ends_with('/')) {
            s =  "<CommonPrefixes>"
                "<Prefix>" + obj.first + "</Prefix>"
                "</CommonPrefixes>";
        } else {
            s ="<Contents>"
                "<Key>" + obj.first + "</Key>"
                "<LastModified>" + to_iso8601(obj.second.last_modified) + "</LastModified>"
                "<Size>" + std::to_string(obj.second.size) + "</Size>"
                "</Contents>";
        }
        buffer->append(s);
    }
}

asio::awaitable<std::string> LobosBucket::create_mpu(std::string& key) {
    std::string oid = name_ + "_" + key;
    auto upload_id = generate_upload_id();
    auto rc = co_await store_.do_create_mpu(oid, upload_id);
    if (rc)
        co_return "";

    mpus_.insert(std::pair<std::string, Multipart>(upload_id, Multipart{
        .key = key,
        .init_time = std::time(nullptr),
        .current_size = 0,
    }));

    co_return upload_id;
}

asio::awaitable<bool> LobosBucket::abort_mpu(std::string& upload_id) {
    std::string oid = name_ + "_" + mpus_[upload_id].key;
    auto rc = co_await store_.do_abort_mpu(oid, name_, upload_id, mpus_[upload_id]);
    if (rc)
        co_return false;
    mpus_.erase(upload_id);
    co_return true;
}

asio::awaitable<std::string> LobosBucket::complete_mpu(std::string& upload_id, std::vector<int>& parts) {
    auto rc = co_await store_.do_assemble_mpu(name_, upload_id, mpus_[upload_id], parts);
    if (rc)
        co_return "";
    std::string key = mpus_[upload_id].key;
    mpus_.erase(upload_id);
    co_return key;
}