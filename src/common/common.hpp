#pragma once

#include <string>
#include <map>
#include <ctime>

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/evp.h>

#include <boost/beast/core.hpp>

struct Part {
    size_t size;
    std::string etag; //we dont use it but we will
};

struct Multipart {
    std::string key;
    time_t init_time;
    size_t current_size = 0;
    std::map<int, Part> parts{};
};

std::string sha256_hex(const std::string& input);

std::vector<unsigned char> hmac_sha256(const std::string& key, const std::string& data);

std::vector<unsigned char> hmac_sha256(const std::vector<unsigned char>& key, const std::string& data);

std::string hmactohex(const std::vector<unsigned char>& key);

std::string md5_hex(const uint8_t* data, size_t len);

boost::beast::string_view mime_type(boost::beast::string_view key);

std::string to_rfc1123(time_t t);
std::string to_iso8601(time_t t);

void random_gen(int len, const std::string allowed_chars, std::string& s);

template<typename T>
std::vector<uint8_t> as_bytes(const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);
    auto p = reinterpret_cast<const uint8_t*>(&val);
    return {p, p + sizeof(T)};
}

inline std::vector<uint8_t> as_bytes(const std::string& val) {
    return {val.begin(), val.end()};
}

struct User {
    std::string name;
    std::string key;
    std::string secret;
    std::string backend;
};

struct BucketMetadata {
    uint64_t owner;
    time_t created_at;
    bool is_shadow{false};
}__attribute__((packed));

struct BucketStats {
    uint64_t size_bytes;
    uint64_t num_objects;
};

struct BucketRecord {
    std::string key;
    uint64_t owner;
    time_t created_at;
    bool is_shadow;
};
