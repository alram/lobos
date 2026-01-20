#include <boost/beast/core.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio/awaitable.hpp>

#include "fs_store.hpp"

namespace beast = boost::beast;
namespace asio = boost::asio;
namespace fs = boost::filesystem;

asio::awaitable<size_t> FsStore::do_write(std::string o, std::span<uint8_t> data) {
    create_dest_dirs_if_not_exist(o);
    // We always overwrite since it's an object
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    int fd = open(o.c_str(), flags, 0644);
    size_t total_written = 0;
    while (total_written < data.size()) {
        ssize_t written = write(fd, data.data() + total_written, data.size() - total_written);
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

asio::awaitable<int> FsStore::do_read(std::string o, uint64_t offset, std::span<uint8_t> buffer) {
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

asio::awaitable<void> FsStore::do_list(std::string_view prefix, std::vector<uint8_t>& buffer) {
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
        if (!pref_s.empty()) {
            auto s = path.string() + '/' + pref_s;
            if (!entry.path().string().starts_with(s)) {
                continue;
            }
        }
        size_t old_size = buffer.size();
        std::string s;
        if (fs::is_directory(entry.path())) {
            s =  "<CommonPrefixes>"
                "<Prefix>" + entry.path().filename().string() + '/' + "</Prefix>"
                "</CommonPrefixes>";
            
        } else {
            s = "<Contents>"
                "<Key>" + entry.path().filename().string() + "</Key>"
                "<LastModified>" + std::to_string(fs::last_write_time(entry)) + "</LastModified>"
                "<Size>" + std::to_string(fs::file_size(entry.path())) + "</Size>"
                "</Contents>";
        }
        buffer.insert(buffer.end(), s.begin(), s.end());
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