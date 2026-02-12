#pragma once

#include <spdk/blob.h>
#include "index.hpp"

struct SpdkIndexObject : ObjectBase {
    spdk_blob_id blob_id;
};

class SpdkIndex : public IndexStore<SpdkIndexObject> {
public:
    SpdkIndexObject* get_entry(std::string& key);
    spdk_blob_id get_blob_id(std::string& key);
};