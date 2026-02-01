#include "common.hpp"

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

boost::beast::string_view mime_type(boost::beast::string_view key) {
    using boost::beast::iequals;
    auto const ext = [&key]
    {
        auto const pos = key.rfind(".");
        if(pos == boost::beast::string_view::npos)
            return boost::beast::string_view{};
        return key.substr(pos);
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

std::string to_rfc1123(time_t t) {
    std::tm tm{};
    gmtime_r(&t, &tm);

    char buf[30];
    std::strftime(buf, sizeof(buf),
                  "%a, %d %b %Y %H:%M:%S GMT",
                  &tm);
    return buf; 
}

std::string to_iso8601(time_t t) {
    std::tm tm{};
    gmtime_r(&t, &tm);

    char buf[30];
    std::strftime(buf, sizeof(buf),
                  "%Y-%m-%dT%H:%M:%SZ",
                  &tm);
    return buf; 
}