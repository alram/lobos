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
            std::cout << "total clusters: " << spdk_bs_total_data_cluster_count(ctx->bs_) << std::endl;
        }, this);

        while (run_stats_engine_) {
            update_stats();
            std::this_thread::sleep_for(std::chrono::seconds(SPDK_STATS_UPDATE_INTERVAL_SEC));
        }
    });
    collector_thread_ =  std::thread([this] {
        start_prom_collector();
    });
}

void SpdkStats::update_stats() {
    auto run_in_spdk = [](void* arg) {
        auto ctx = static_cast<SpdkStats*>(arg);
        if(ctx->bs_ == nullptr) return;
        ctx->stats_.available_clusters = spdk_bs_free_cluster_count(ctx->bs_);

        // SPDK_BDEV_RESET_STAT_ALL means we'll reset stats
        // in SPDK on every call. This matters for the prom
        // collector since counters only support Increment()
        spdk_bdev_get_device_stat(
            ctx->bdev_->get_base_bdev(ctx->bdev_), 
            &ctx->stats_.iostat, SPDK_BDEV_RESET_STAT_ALL, 
            [](struct spdk_bdev *bdev, struct spdk_bdev_io_stat *stat, void *cb_arg, int rc){
                if (rc)
                    std::cerr << "non-fatal err retrieving usage stats: " << rc << std::endl;
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
    stop_prom_collector();
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

void SpdkStats::start_prom_collector() {
    std::cout << "starting prom collector" << std::endl;
    using namespace prometheus;
    Exposer exposer{"127.0.0.1:9091"};

    auto registry = std::make_shared<Registry>();

    // Cluster metrics
    auto& cluster_gauge_total = prometheus::BuildGauge()
        .Name("spdk_bs_clusters_total")
        .Help("Total blobstore clusters")
        .Register(*registry);
    total_clusters_ = &cluster_gauge_total.Add({});
    auto& cluster_gauge_used = prometheus::BuildGauge()
        .Name("spdk_bs_clusters_available")
        .Help("Total blobstore clusters available")
        .Register(*registry);
    avail_clusters_ = &cluster_gauge_used.Add({});
    
    // Bdev IO metrics
    auto& read_op_counter = prometheus::BuildCounter()
        .Name("spdk_bdev_read_ops_total")
        .Help("Total read operations")
        .Register(*registry);
    read_ops_ = &read_op_counter.Add({});
    auto& write_op_counter = prometheus::BuildCounter()
        .Name("spdk_bdev_write_ops_total")
        .Help("Total write operations")
        .Register(*registry);
    write_ops_ = &write_op_counter.Add({});

    auto& bytes_read_counter = prometheus::BuildCounter()
        .Name("spdk_bdev_bytes_read_total")
        .Help("Total bytes read")
        .Register(*registry);
    bytes_read_ = &bytes_read_counter.Add({});
    auto& bytes_written_counter = prometheus::BuildCounter()
        .Name("spdk_bdev_bytes_written_total")
        .Help("Total bytes written")
        .Register(*registry);
    bytes_written_ = &bytes_written_counter.Add({});

    exposer.RegisterCollectable(registry);

    while(run_prom_collector) {
        std::this_thread::sleep_for(std::chrono::seconds(SPDK_STATS_UPDATE_INTERVAL_SEC));
        total_clusters_->Set(stats_.total_clusters);
        avail_clusters_->Set(stats_.available_clusters);
        read_ops_->Increment(stats_.iostat.num_read_ops);
        write_ops_->Increment(stats_.iostat.num_write_ops);
        bytes_read_->Increment(stats_.iostat.bytes_read);
        bytes_written_->Increment(stats_.iostat.bytes_written);
    }
}