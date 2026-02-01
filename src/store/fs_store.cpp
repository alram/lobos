#include <boost/beast/core.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio/awaitable.hpp>

#include <fstream>

#include "fs_store.hpp"

namespace beast = boost::beast;
namespace asio = boost::asio;
namespace fs = boost::filesystem;

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

asio::awaitable<void> FsStore::do_list(std::string_view prefix, session_buffer& buffer) {
    std::string pref_s(prefix);
    fs::path path = pref_s;
    if (!fs::exists(path)) {
        auto pos = path.string().find('/');
        if (pos == beast::string_view::npos) {
            path.clear();
        }
        else {
            path = prefix.substr(0, pos);
            prefix = prefix.substr(pos+1);
        }
    } else {
        pref_s.clear();
    }

    if (path.empty())
        path = fs::current_path();

    for (auto& entry : boost::make_iterator_range(fs::directory_iterator(path), {})) {
        if (entry.path().string().find(lobos_state_prefix) != beast::string_view::npos)
            continue;

        if (!pref_s.empty()) {
            auto s = path.string() + '/' + pref_s;
            if (!entry.path().string().starts_with(s)) {
                continue;
            }
        }
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

asio::awaitable<int> FsStore::do_create_mpu(std::string_view o, std::string uploadId) {
    // we simply create an empty file stating an MPU for key has been created
    std::string path = lobos_mpu_prefix + "/" + o.data() + "_" + uploadId + "_initiated";
    int fd = open(path.c_str(), O_CREAT | O_RDONLY, 0600);
    if (fd < 0)
        co_return fd;
    close(fd);
    co_return 0;
}

std::unordered_map<std::string, Multipart> FsStore::get_active_mpus() {
    // load up the xattr that contain MPU information
    create_dest_dirs_if_not_exist(lobos_mpu_prefix + "/dummy");
    fs::path p = lobos_mpu_prefix + "/";
    std::unordered_map<std::string, Multipart> active_mpus = {};

    // There are two types of files
    // <key>_<uploadId>_initiated - means a MPU is created
    // <uploadId>_<partNumber>_<key> - part
    for (auto& entry : boost::make_iterator_range(fs::directory_iterator(p))) {
        std::string file = entry.path().string().substr(p.string().length());
        if (file.find("initiated") != beast::string_view::npos) {
            auto pos = file.find('_');
            std::string key = file.substr(0, pos);
            file.erase(0, pos+1);
            pos = file.find('_');
            std::string upload_id = file.substr(0, pos);

            if(!active_mpus.contains(upload_id)) {
                Multipart mp{
                    key,
                    fs::last_write_time(entry),
                    0,
                };
                active_mpus.insert(std::pair<std::string, Multipart>(upload_id, mp));
            } else {
                // we just update init time
                active_mpus[upload_id].init_time = fs::last_write_time(entry);
            }
        } else {
            auto pos = file.find('_');
            std::string upload_id = file.substr(0, pos);
            file.erase(0, pos+1);
            pos = file.find('_');
            int part_number = std::stoi(file.substr(0, pos));
            file.erase(0, pos+1);
            std::string key = file;
            size_t size = fs::file_size(entry.path());
            Part p{
                fs::file_size(entry.path()),
                "0", // TODO, compute etag
            };
            std::cout << "found part - key: " << key << " - uploadid: " << upload_id << "- partnumber: " << part_number << std::endl;
            if(!active_mpus.contains(upload_id)) {
                // create it, we'll update what is needed when we come up to the init object
                Multipart mp{
                    key,
                    0,
                    0,
                };
                active_mpus.insert(std::pair<std::string, Multipart>(upload_id, mp));
            }
            active_mpus[upload_id].parts.insert(std::pair<int,Part>(part_number, p));
            active_mpus[upload_id].current_size += size;
        }
    }
    return active_mpus;
}

asio::awaitable<int> FsStore::do_assemble_mpu(std::string upload_id, Multipart mp, std::vector<int> parts) {
    // all the sanity checks shoulda been done at the server level
    // technically could race tho
    std::ofstream out(mp.key, std::ios::binary | std::ios::trunc);

    // TODO failure in the middle of this will leave
    // corrupted objects.
    std::vector<char> chunk(1024*1024); //1MiB chunks
    for (const auto& part : parts) {
        // <uploadId>_<partNumber>_<key>
        auto src_path = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key;
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
        auto src_path = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key;;
        fs::remove(src_path);
    }
    // remove the initiate
    fs::remove(lobos_mpu_prefix + "/" + mp.key + "_" + upload_id + "_initiated");

    co_return 0;
}

asio::awaitable<int> FsStore::do_abort_mpu(std::string upload_id, Multipart mp) {
    // Delete part files
    for (const auto& part : mp.parts) {
        auto src_path = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key;
        fs::remove(src_path);
    }
    // remove the initiate
    fs::remove(lobos_mpu_prefix + "/" + mp.key + "_" + upload_id + "_initiated");
    
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