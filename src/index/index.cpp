#include <boost/filesystem.hpp>
#include <iostream>

#include "index.hpp"

namespace fs = boost::filesystem;


// this is bad
bool IndexStore::build_index_from_fs(std::string path_start) {
    build_in_progress = true;
    path_start = path_start + '/';
    for (const auto& entry : fs::recursive_directory_iterator(path_start)) {
        auto name = entry.path().string().substr(path_start.length());

        char type;
        if (fs::is_directory(entry.path())) {
            type = 'd';
        }
        else if (fs::is_regular_file(entry.path())) {
            type = 'f';
        } else {
            // skip anything else
            continue;
        }

        Object e;
        e.last_modified = fs::last_write_time(entry);
        if (type == 'f')
            e.size = fs::file_size(entry.path());
        else
            e.size = 0; //for dirs we don't care about size?

        e.type = type;
        // e.path = fs::absolute(entry.path().lexically_normal()).string();

        index.emplace(name, e);
    }

    build_in_progress = false;
    return true;
}

void IndexStore::add_entry(std::string object, Object o) {
    index.insert(std::pair<std::string, Object>(object, o));
}