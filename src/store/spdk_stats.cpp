#include <iostream>
#include "spdk_stats.hpp"

void SpdkStats::start_stats_engine() {
    engine_thread_ = std::thread([this] {
        // On first run, we grab total clusters, send
        // and forget, and we just handle the races
        // since it can only be a problem for one fetch cycle
        spdk_thread_send_msg(spdk_thread_, [](void* arg) {
            auto ctx = static_cast<SpdkStats*>(arg);
            ctx->stats_.total_clusters = spdk_bs_total_data_cluster_count(ctx->bs_);
        }, this);

        while (run_stats_engine_) {
            update_stats();
            std::this_thread::sleep_for(std::chrono::seconds(SPDK_STATS_UPDATE_INTERVAL_SEC));
        }
    });
}

void SpdkStats::update_stats() {
    auto run_in_spdk = [](void* arg) {
        auto ctx = static_cast<SpdkStats*>(arg);
        if(ctx->bs_ == nullptr) return;
        ctx->stats_.available_clusters = spdk_bs_free_cluster_count(ctx->bs_);

        spdk_bdev_get_device_stat(
            ctx->bdev_->get_base_bdev(ctx->bdev_), 
            &ctx->stats_.iostat, SPDK_BDEV_RESET_STAT_NONE, 
            [](struct spdk_bdev *bdev, struct spdk_bdev_io_stat *stat, void *cb_arg, int rc){
                auto ctx = static_cast<SpdkStats*>(cb_arg);
                ctx->stats_updating = false;
            }, 
            ctx);
    };
    stats_updating = true;
    spdk_thread_send_msg(spdk_thread_, run_in_spdk, this);
    while(stats_updating) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

void SpdkStats::shutdown_stats_engine() {
    std::cout << "Shutting down SPDK stats engine" << std::endl;
    run_stats_engine_ = false;
    if (engine_thread_.joinable())
        engine_thread_.join();
}

float SpdkStats::get_store_percent_used() {
    // this is racy on boot, we return a dummy value
    // if updated_stats() hasn't finished.
    if(!stats_.available_clusters) return 0.1;
    float used = stats_.total_clusters - stats_.available_clusters;
    return (used *100 / stats_.total_clusters);
}