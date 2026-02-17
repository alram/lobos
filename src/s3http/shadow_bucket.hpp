#pragma once

#include "s3_bucket.hpp"
#include "lobos_bucket.hpp"

class ShadowBucket : public S3Bucket {
public:
    ShadowBucket(std::unique_ptr<LobosBucket> cache, std::string cachePolicy)
        : lbucket_(std::move(cache))
        , policy_(std::move(cachePolicy))
    {}

    asio::awaitable<bool> create_bucket() override { co_return false; }
    asio::awaitable<int> delete_bucket() override { co_return 0; }
    asio::awaitable<std::tuple<size_t, time_t>> get_object_metadata(std::string& key) override;
    asio::awaitable<int> put_object(std::string& key, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<int> get_object(std::string& key, uint64_t& offset, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<int> delete_object(std::string& key) override;
    asio::awaitable<void> list_objects(std::string& prefix, std::shared_ptr<session_buffer>& buffer) override;
    asio::awaitable<std::string> create_mpu(std::string& key) override;
    asio::awaitable<bool> abort_mpu(std::string& upload_id) override;
    asio::awaitable<std::string> complete_mpu(std::string& upload_id, std::vector<int>& parts) override;


    const std::string& name() const override { return lbucket_->name(); }
    time_t created_at() const override { return lbucket_->created_at(); }
    bool has_mpu(const std::string& upload_id) const override { return lbucket_->has_mpu(upload_id); }
    const std::string& mpu_key(const std::string& upload_id) const override { return lbucket_->mpu_key(upload_id); }
    bool mpu_has_part(const std::string& upload_id, int part_n) const override { return lbucket_->mpu_has_part(upload_id, part_n); }
    void add_mpu_part(const std::string& upload_id, int part_n, const Part& p, const size_t size) override { lbucket_->add_mpu_part(upload_id, part_n, p, size); }
    const std::map<int, Part>& mpu_parts(const std::string& upload_id) const override { return lbucket_->mpu_parts(upload_id); }
    void for_each_mpu(const mpu_visitor& visitor) const override { lbucket_->for_each_mpu(visitor); }


private:
    std::unique_ptr<LobosBucket> lbucket_;
    std::string policy_;
};