#pragma once

template<IsObject ObjectType>
void IndexStore<ObjectType>::add_entry(std::string& key, ObjectType& obj) {
    index_.insert_or_assign(key, obj);
}

template<IsObject ObjectType>
void IndexStore<ObjectType>::rm_entry(std::string& key) {
    index_.erase(key);
}

template<IsObject ObjectType>
ObjectType* IndexStore<ObjectType>::get_entry(std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end())
        return nullptr;
    return &it->second;
}

template<IsObject ObjectType>
bool IndexStore<ObjectType>::bucket_has_keys(std::string& bucket, const std::string& exclude_prefix) {
    for (auto it = index_.lower_bound(bucket); it != index_.end(); it++) {
        if (!it->first.starts_with(bucket + "/"))
            break;
        if(it->first.starts_with(bucket + "/" + exclude_prefix))
            continue;
        return true;
    }
    return false;
}

template<IsObject ObjectType>
int IndexStore<ObjectType>::set_listable(std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end())
        return -1;
    it->second.list = true;
    return 0;
}

template<IsObject ObjectType>
int IndexStore<ObjectType>::unset_listable(std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end())
        return -1;
    it->second.list = false;
    return 0;
}

template<IsObject ObjectType>
std::vector<std::string> IndexStore<ObjectType>::list_keys(const std::string& prefix) {
    std::vector<std::string> v;
    for (auto it = index_.lower_bound(prefix); it != index_.end(); it++) {
        if (!it->first.starts_with(prefix))
            break;
        v.emplace_back(it->first);
    }
    return v;
}

template<IsObject ObjectType>
std::map<std::string, ObjectBase> IndexStore<ObjectType>::s3_list_prefix_non_recursive(std::string& prefix) {
    std::map<std::string, ObjectBase> m;

    auto pos = prefix.find_last_of('/');
    std::string base = prefix.substr(0, pos+1);
    std::string key_skip;
    auto it = index_.lower_bound(prefix);
    for(; it!= index_.end(); it++) {
        if (!it->first.starts_with(prefix))
            break;
        if (!it->second.list) {
            continue;
        }

        std::string key = it->first;
        key.erase(0, base.length());

        if(!key_skip.empty() && key.starts_with(key_skip))
            continue;

        // If the key contains a `/` we remove everything after the
        // first / otherwise it would be a recursive listing
        // and we store that key so that next run can skip it
        auto pos = key.find('/');
        if (pos != std::string::npos) {
            key.erase(pos+1);
            key_skip = key;
        }

        m.emplace(std::make_pair(key, ObjectBase{
            .key = it->second.key,
            .size = it->second.size,
            .last_modified = it->second.last_modified,
            .list = it->second.list,
        }));
    }

    return m;
}

template<IsObject ObjectType>
std::tuple<size_t, time_t> IndexStore<ObjectType>::get_object_md(std::string& key) {
    auto it = index_.find(key);
    if (it != index_.end())
        return std::make_tuple(it->second.size, it->second.last_modified);
    return std::make_tuple(0, 0);
}