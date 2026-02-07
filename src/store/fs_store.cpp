#include <boost/beast/core.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio/awaitable.hpp>

#include <fstream>

#include "fs_store.hpp"

namespace beast = boost::beast;
namespace asio = boost::asio;
namespace fs = boost::filesystem;

void FsStore::init_store(std::string) {
    create_dest_dirs_if_not_exist(lobos_mpu_prefix + "/dummy");
    create_dest_dirs_if_not_exist(lobos_user_prefix + "/dummy");

    if (!boost::filesystem::exists(lobos_bucket_prefix)) {
        int flags = O_WRONLY | O_CREAT;
        int fd = open(lobos_bucket_prefix.c_str(), flags, 0644);
        if (fd < 0)
            throw std::runtime_error("could not create bucket state object");
        close(fd);
    }
}
asio::awaitable<int> FsStore::do_write(std::string o, session_buffer& buffer) {
    create_dest_dirs_if_not_exist(o);
    // We always overwrite since it's an object
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    int fd = open(o.c_str(), flags, 0644);
    size_t total_written = 0;
    while (total_written < buffer.size()) {
        ssize_t written = write(fd, buffer.data() + total_written, buffer.size() - total_written);
        if (written < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("Write failed: " + std::string(strerror(errno)));
        }
        total_written += written;
    }
    fsync(fd);
    close(fd);

    co_return total_written;

}

asio::awaitable<int> FsStore::do_read(std::string o, uint64_t offset, session_buffer& buffer) {
    std::string f(o);
    int fd = open(f.c_str(), O_RDONLY);
    if (fd < 0)
        co_return -1;

    ssize_t bytes_read = pread(fd, buffer.data(), buffer.size(), offset);
    if (bytes_read < 0)
        co_return -1;
    close(fd);

    co_return 0;
}

asio::awaitable<bool> FsStore::do_delete(std::string_view o) {
    auto deleted = boost::filesystem::remove(o);
    co_return deleted;
}

asio::awaitable<void> FsStore::do_list(std::string& prefix, session_buffer& buffer) {
    // In the prefix, we want the last /
    // which will indicate our actual path
    // then we'll use what's right of the path as
    // an actual prefix
    auto pos = prefix.find_last_of('/');
    fs::path path = prefix.substr(0, pos);
    std::string pre = prefix.substr(pos+1);
    for (auto& entry : boost::make_iterator_range(fs::directory_iterator(path), {})) {
        if (entry.path().string().find(lobos_state_prefix) != beast::string_view::npos)
            continue;

        std::string_view sv = path.string() + "/" + pre;
        if (!entry.path().string().starts_with(sv))
            continue;

        std::string s;
        if (fs::is_directory(entry.path())) {
            s =  "<CommonPrefixes>"
                "<Prefix>" + entry.path().filename().string() + '/' + "</Prefix>"
                "</CommonPrefixes>";
            
        } else {
            s = "<Contents>"
                "<Key>" + entry.path().filename().string() + "</Key>"
                "<LastModified>" + to_iso8601(fs::last_write_time(entry)) + "</LastModified>"
                "<Size>" + std::to_string(fs::file_size(entry.path())) + "</Size>"
                "</Contents>";
        }
        buffer.append(s);
    }
    co_return;
};

asio::awaitable<bool> FsStore::create_bucket(std::string& key, BucketMetadata& md) {
    // as is its create/replace which should be fine?
    key = "user." + key;
    int ret = setxattr(lobos_bucket_prefix.c_str(), key.c_str(), &md, sizeof(md), 0);
    if (ret == -1)
        co_return false;
    // create the dir otherwise ls on empty bucket will fail
    auto pos = key.find('_');
    auto dir = key.substr(pos+1) + "/dummy";
    create_dest_dirs_if_not_exist(dir);

    co_return true;
};

asio::awaitable<int> FsStore::delete_bucket(std::string_view bucket) {
    // boost::system::error_code ec;
    // fs::remove(bucket, ec);
    // if (ec)
    //     co_return ec.value();
    co_return 0;
};

std::vector<BucketRecord> FsStore::load_buckets() {
    std::vector<BucketRecord> buckets{};
    ssize_t len = listxattr(lobos_bucket_prefix.c_str(), nullptr, 0);
    std::vector<char> keys(len);
    len = listxattr(lobos_bucket_prefix.c_str(), keys.data(), len);
    if (len <= 0)
        return buckets;

    const char* key = keys.data();
    const char* pre = ("users." + lobos_bucket_prefix).c_str();
    while (key < keys.data() + len) {
        if (strncmp(key, pre, strlen(pre))) {
            BucketMetadata md;
            ssize_t ret = getxattr(lobos_bucket_prefix.c_str(), key, &md, sizeof(md));
            if (ret == sizeof(md))
                buckets.emplace_back(BucketRecord{
                    std::string(key + 6), //skips 'users.' since its fs specific
                    md.owner,
                    md.created_at
                });
        }
        key += strlen(key) + 1;
    }

    return buckets;
}

asio::awaitable<std::tuple<size_t, time_t>> FsStore::do_metadata_req(std::string_view o) {
    size_t size;
    time_t last_modified;
    try {
        size = fs::file_size(o);
        last_modified = fs::last_write_time(o);
    } catch (const fs::filesystem_error& e) {
        size = last_modified = 0;
    }
    co_return std::tuple{size, last_modified};
}

asio::awaitable<int> FsStore::do_create_mpu(std::string& oid, std::string& upload_id) {
    // we simply create an empty file stating an MPU for key has been created
    std::string path = lobos_mpu_prefix + "/" + oid + "_" + upload_id + "_initiated";
    int fd = open(path.c_str(), O_CREAT | O_RDONLY, 0600);
    if (fd < 0)
        co_return fd;
    close(fd);

    co_return 0;
}

std::unordered_map<std::string, std::unordered_map<std::string,Multipart>> FsStore::get_active_mpus() {
    fs::path p = lobos_mpu_prefix + "/";
    std::unordered_map<std::string, std::unordered_map<std::string,Multipart>> mpus = {};
    // In top level we have all the mpu initiation
    // format: <bucket>_<key>_<upload_id>_initiated
    for (auto& entry : boost::make_iterator_range(fs::directory_iterator(p))) {
        std::string file = entry.path().string().substr(p.string().length());
        if (file.find("initiated") != beast::string_view::npos) {
            std::istringstream ss(file);
            std::string bucket;
            std::string upload_id;
            std::getline(ss, bucket, '_');

            Multipart mp;
            std::getline(ss, mp.key, '_');
            std::getline(ss, upload_id, '_');
            mp.init_time = fs::last_write_time(entry);

            mpus[bucket].emplace(upload_id, mp);
        }
    }

    // Now we scan for the parts
    // format: <upload_id>_<partN>_<key>
    for (auto& [bucket, m] : mpus) {
        fs::path part_path = bucket + "/" + lobos_mpu_prefix + "/";
        if (!fs::exists(part_path))
            continue;

        for (auto& entry : boost::make_iterator_range(fs::directory_iterator(part_path))) {
            std::istringstream ss(entry.path().string().substr(part_path.string().length()));
            std::string upload_id;
            std::string part;
            std::string key;

            std::getline(ss, upload_id, '_');
            std::getline(ss, part, '_');
            std::getline(ss, key, '_');
            
            auto part_size = fs::file_size(entry.path());

            m[upload_id].parts.emplace(std::stoi(part), Part{
                .size = part_size,
                .etag = "0000"
            });
            m[upload_id].current_size += part_size;
        }
    }
    return mpus;
}

asio::awaitable<int> FsStore::do_assemble_mpu(std::string& bucket, std::string& upload_id, Multipart& mp, std::vector<int>& parts) {
    // all the sanity checks shoulda been done at the server level
    // technically could race tho
    std::ofstream out(bucket + "/" + mp.key, std::ios::binary | std::ios::trunc);

    // TODO failure in the middle of this will leave
    // corrupted objects.
    std::vector<char> chunk(1024*1024); //1MiB chunks
    for (const auto& part : parts) {
        // <uploadId>_<partNumber>_<key>
        auto src_path = bucket + "/" + lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key;
        std::ifstream in(src_path, std::ios::binary);
        if (!in)
            co_return -EIO;
        while (in) {
            in.read(chunk.data(), chunk.size());
            auto bytes_read = in.gcount();
            if (bytes_read > 0) {
                out.write(chunk.data(), bytes_read);
                if (!out)
                    co_return -EIO;
            }
        }
    }
    out.close();

    // Delete part files
    for (const auto& part : parts) {
        auto src_path = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key + "_" + bucket;
        fs::remove(src_path);
    }
    // remove the initiate
    fs::remove(lobos_mpu_prefix + "/" + bucket + "_" + mp.key + "_" + upload_id + "_initiated");

    co_return 0;
}

asio::awaitable<int> FsStore::do_abort_mpu(std::string& oid, std::string& bucket, std::string& upload_id, Multipart& mp) {
    // Delete part files
    for (const auto& part : mp.parts) {
        auto src_path = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key + "_" + bucket;
        fs::remove(src_path);
    }
    // remove the initiate
    fs::remove(lobos_mpu_prefix + "/" + oid + "_" + upload_id + "_initiated");
    
    co_return 0;
}

std::string FsStore::create_dest_dirs_if_not_exist(std::string object) {
    //We need to ensure all the parents directories exist before anything
    auto pos = object.rfind('/');
    if (pos != beast::string_view::npos) {
        auto path = object.substr(0, pos);
        if (!fs::exists(path)) {
            fs::create_directories(path);
        }
    }
    return object;
}

int FsStore::metadata_add_user(User u) {
    std::string key = lobos_user_prefix + "/" + u.name + "/";
    create_dest_dirs_if_not_exist(key);
    // We create keys automatically for every new user
    auto ret = metadata_add_key(u);
    return ret;
}

std::vector<User> FsStore::metadata_list_users(std::string filter) {
    fs::path p = lobos_user_prefix + "/";
    std::vector<User> users = {};

    for (auto& user : boost::make_iterator_range(fs::directory_iterator(p))) {
        User u;
        u.name = user.path().string().substr(p.string().length());

        for (auto& entry : boost::make_iterator_range(fs::directory_iterator(user))) {
            std::string s = entry.path().string().substr(user.path().string().length() + 1);
            std::istringstream ss(s);
            std::getline(ss, u.key, '_');
            std::getline(ss, u.secret, '_');
            std::getline(ss, u.backend);
            users.emplace_back(u);
        }
    }
    return users;
}

bool FsStore::metadata_remove_user(std::string& name) {
    const std::string dir = lobos_user_prefix + "/" + name;
    boost::system::error_code ec;
    fs::remove_all(dir, ec);

    if (ec) {
        std::cerr << "error deleting dir: " << ec << std::endl;
        return false;
    }
    return true;
}

int FsStore::metadata_add_key(User u) {
    std::string key = lobos_user_prefix + "/" + u.name + "/" + u.key + "_" + u.secret + "_" + u.backend;
    int fd = open(key.c_str(), O_CREAT | O_RDONLY, 0600);
    if (fd < 0)
        return fd;
    close(fd);

    return 0;
}

bool FsStore::metadata_rm_key(std::string user, User u) {
    fs::path p = lobos_user_prefix + "/" + user + "/" + u.key + "_" + u.secret + "_" + u.backend;
    auto r = fs::remove(p);
    if (r)
        return r;
    return 0;
}