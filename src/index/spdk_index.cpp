#include "spdk_index.hpp"

spdk_blob_id SpdkIndex::get_blob_id(std::string& key) {
    return index_.at(key).blob_id;
}

SpdkIndexObject* SpdkIndex::get_entry(std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end())
        return nullptr;
    return &it->second;
}