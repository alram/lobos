#include <thread>

#include <spdk/thread.h>
#include <spdk/bdev.h>
#include <spdk/blob.h>

#define SPDK_STATS_UPDATE_INTERVAL_SEC 2

struct SpdkBsStats {
    uint64_t total_clusters = 0;
    uint64_t available_clusters = 0;
    spdk_bdev_io_stat iostat;
};


class SpdkStats {
public:
    SpdkStats(spdk_thread* t, spdk_bs_dev* bdev, spdk_blob_store* bs)
    : spdk_thread_(t)
    , bdev_(bdev)
    , bs_(bs)
    , stats_{} {}

    void start_stats_engine();
    void update_stats();
    void shutdown_stats_engine();
    float get_store_percent_used(); 
    std::atomic<bool> stats_updating = false;
private:
    spdk_thread* spdk_thread_;
    spdk_bs_dev* bdev_;
    spdk_blob_store *bs_;
    SpdkBsStats stats_;

    std::thread engine_thread_;
    std::thread collector_thread_;

    std::atomic<bool> run_stats_engine_ = true;
};