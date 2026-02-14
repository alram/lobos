#include "spdk_index.hpp"

spdk_blob_id SpdkIndex::get_blob_id(std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end())
        return 0;
    return it->second.blob_id;
}
