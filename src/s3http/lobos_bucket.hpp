#pragma once
#include "s3_bucket.hpp"

class LobosBucket : public S3Bucket {
public:
    LobosBucket(Store& store, std::string name, uint64_t owner, time_t created_at,
                std::unordered_map<std::string, Multipart> mpus)
        : store_(store)
        , name_(std::move(name))
        , owner_(owner)
        , created_at_(created_at)
        , mpus_(std::move(mpus))
    {}

    asio::awaitable<bool> create_bucket() override;
    asio::awaitable<int> delete_bucket() override;
    asio::awaitable<std::tuple<size_t, time_t>> get_object_metadata(std::string& key) override;
    asio::awaitable<int> put_object(std::string& key, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<int> get_object(std::string& key, uint64_t& offset, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<int> delete_object(std::string& key) override;
    asio::awaitable<void> list_objects(std::string& prefix, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<std::string> create_mpu(std::string& key) override;
    asio::awaitable<bool> abort_mpu(std::string& upload_id) override;
    asio::awaitable<std::string> complete_mpu(std::string& upload_id, std::vector<int>& parts) override;

    const std::string& name() const override { return name_; }
    time_t created_at() const override { return created_at_; }
    bool has_mpu(const std::string& upload_id) const override { return mpus_.contains(upload_id); }
    const std::string& mpu_key(const std::string& upload_id) const override {
        return mpus_.at(upload_id).key;
    }
    bool mpu_has_part(const std::string& upload_id, int part_n) const override {
        return mpus_.at(upload_id).parts.contains(part_n);
    }

    void add_mpu_part(const std::string& upload_id, int part_n, const Part& p, const size_t size) override {
        mpus_[upload_id].parts.insert(std::pair<int, Part>(part_n, p));
        mpus_[upload_id].current_size += size;
    }

    const std::map<int, Part>& mpu_parts(const std::string& upload_id) const override {
        return mpus_.at(upload_id).parts;
    }

    void for_each_mpu(const mpu_visitor& visitor) const override {
        for (const auto& [id, mpu] : mpus_) {
            visitor(id, mpu);
        }
    }

private:
    Store& store_;
    std::string name_;
    uint64_t owner_;
    time_t created_at_;
    std::unordered_map<std::string, Multipart> mpus_;

    static std::string generate_upload_id();
};