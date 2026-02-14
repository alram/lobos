#pragma once

#include <cstdlib>
#include <string>
#include <map>
#include <optional>
#include <vector>

struct ObjectBase {
    std::string key;
    size_t size;
    time_t last_modified;
    bool list{true};  // defines wheter or not we should list the object
};

template <typename T>
concept IsObject = std::derived_from<T, ObjectBase>;

template<IsObject ObjectType>
class IndexStore {
public:
    virtual ~IndexStore() = default;

    void add_entry(std::string& key, ObjectType& obj);
    void rm_entry(std::string& key);
    virtual ObjectType* get_entry(std::string& key);
    bool bucket_has_keys(std::string& bucket, const std::string& exclude_prefix);
    int set_listable(std::string& key);
    int unset_listable(std::string& key);
    std::vector<std::string> list_keys(const std::string& prefix);
    // used for S3 calls, won't return the whole thing. Lower level calls will
    // require new methods
    std::map<std::string, ObjectBase> s3_list_prefix_non_recursive(std::string& prefix);
    std::tuple<size_t, time_t> get_object_md(std::string& key);

protected:
    std::map<std::string, ObjectType, std::less<>> index_;
};

#include "index_impl.hpp"