#pragma once

#include <cstdlib>
#include <string>
#include <map>

#include <spdk/blob.h>

struct Object {
    size_t size;
    time_t last_modified;
    spdk_blob_id blob_id;
};

class IndexStore {
    public:
        IndexStore() {};
        ~IndexStore() {};
        std::map<std::string, Object, std::less<>> index;
        
        // This implementation is just a quick thingy
        // this will need to move to a better one eventually
        void add_entry(std::string object, Object o);
};