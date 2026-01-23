#include <iostream>

#include <spdk/jsonrpc.h>

#define SPDK_RPC_SOCK_PATH "/var/tmp/spdk.sock"

//This is fragile. It makes hte name consistent
// between malloc and nvme devs however if the 
// nvme dev has more than 1 NS kaboom
// TODO: hardens that codepath.
#define LOBOS_BDEV_NAME_BASE "Lobos0"
#define LOBOS_BDEV_NAME "Lobos0n1"

struct BlobStoreContext {
    std::atomic<bool> init_done{false};
    spdk_blob_store *bs = nullptr;
    spdk_bs_dev *bs_dev = nullptr;
    BlobStoreConfig conf;

};

class BlobStoreInitializer {
    public:
        virtual ~BlobStoreInitializer() = default;
        virtual std::tuple<spdk_bs_dev*, spdk_blob_store*>  initialize(spdk_thread* t) = 0;
    protected:
        explicit BlobStoreInitializer(std::shared_ptr<BlobStoreContext> ctx, BlobStoreConfig conf) 
            : ctx_(std::move(ctx)) 
            , conf_(conf) {}
        std::shared_ptr<BlobStoreContext> ctx_;
        BlobStoreConfig conf_;
};

void send_rpc_req(const std::string& method, const std::string params) {
        spdk_jsonrpc_client *client = spdk_jsonrpc_client_connect(SPDK_RPC_SOCK_PATH, AF_UNIX);
        if (!client) {
            throw std::runtime_error("Could not connect to SPDK RPC server.");
        }
        int rc;
        auto req = spdk_jsonrpc_client_create_request();
        auto w = spdk_jsonrpc_begin_request(req, 1, method.c_str());

        spdk_json_write_name(w, "params");
        spdk_json_write_val_raw(w, static_cast<const void*>(params.c_str()), params.length());
        spdk_jsonrpc_end_request(req, w);
        spdk_jsonrpc_client_send_request(client, req);
        do {
            rc = spdk_jsonrpc_client_poll(client, 1);
        } while (rc == 0 || rc == -ENOTCONN);
        
        spdk_jsonrpc_client_get_response(client);
        spdk_jsonrpc_client_close(client);
}  

static void base_bdev_event_cb(enum spdk_bdev_event_type type, spdk_bdev *bdev, void *event_ctx) {
    std::cerr << "Received but not doing anything with it" << type << std::endl;
}

// We keep that shit raw for Malloc since it's really only
// for testing (and CI if ever... lol)... send and pray
class MallocBSInitializer : public BlobStoreInitializer {
public:
    MallocBSInitializer(BlobStoreConfig conf)
            : BlobStoreInitializer(std::make_shared<BlobStoreContext>(), conf) {}

    std::tuple<spdk_bs_dev*, spdk_blob_store*> initialize(spdk_thread* t) override {
        std::string params = "{\"name\":\""+ std::string(LOBOS_BDEV_NAME) + "\",\"num_blocks\": 32768,\"block_size\":512}";
        send_rpc_req("bdev_malloc_create", params);

        BlobStoreContext* ctx_raw = ctx_.get();
        ctx_->conf = conf_;

        auto run_on_spdk_thread = [](void *args) {
            auto context = static_cast<BlobStoreContext*>(args);
            spdk_bs_dev* bs_dev = nullptr;
            int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, context, &bs_dev);
            if (rc !=0) {
                context->init_done = true;
                return;
            }

            spdk_bs_opts opts{};
            spdk_bs_opts_init(&opts, sizeof(opts));
            if (context->conf.cluster_sz > 0)
                opts.cluster_sz = context->conf.cluster_sz;
            context->bs_dev = bs_dev;
            spdk_bs_init(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                auto final_ctx = static_cast<BlobStoreContext*>(cb_args);
                if (bserr == 0) {
                    final_ctx->bs = bs;
                } else {
                    std::cerr << "Blobstore init failed with error: " << bserr << std::endl;
                }
                final_ctx->init_done = true; // Signal the main thread
            }, args);
        };

        spdk_thread_send_msg(t, run_on_spdk_thread, ctx_raw);

        while(!ctx_->init_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        return std::tuple<spdk_bs_dev*, spdk_blob_store*>{ctx_->bs_dev, ctx_->bs};
    }

};


// For NVMe, for now we only support directly attached
// nvme devs (altho technically adding nvmeof is doable)
// We need to connect the nvme controller to a bdev
// then check if a blobstore already exists on it
// if so we load it, if not we create it
class NvmeBSInitializer : public BlobStoreInitializer {
private:
    std::string pci_addr;
public:
    NvmeBSInitializer(std::string addr, BlobStoreConfig conf) 
    : BlobStoreInitializer(std::make_shared<BlobStoreContext>(), conf) 
    , pci_addr(addr)
    {}

    std::tuple<spdk_bs_dev*, spdk_blob_store*>  initialize(spdk_thread* t) override {

        //TODO: using C API instead of RPC might be more robust..
        // need to add: traddr and trype 
        std::string params = "{\"name\":\""+ std::string(LOBOS_BDEV_NAME_BASE) + "\",\"traddr\":\""+ pci_addr +"\",\"trtype\":\"PCIe\"}";
        send_rpc_req("bdev_nvme_attach_controller", params);

        // Now that we have a bdev, we can try to load blobstore and if it fails just
        // create it
        BlobStoreContext* ctx_raw = ctx_.get();
        ctx_->conf = conf_;

        auto run_on_spdk_thread = [](void* args) {
            auto context = static_cast<BlobStoreContext*>(args);
            spdk_bs_dev* bs_dev = nullptr;

            int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, context, &bs_dev);
            if (rc !=0) {
                context->init_done = true;
                return;
            }

            spdk_bs_opts opts{};
            spdk_bs_opts_init(&opts, sizeof(opts));
            if (context->conf.cluster_sz > 0)
                opts.cluster_sz = context->conf.cluster_sz;

            context->bs_dev = bs_dev;
            spdk_bs_load(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                auto ctx = static_cast<BlobStoreContext*>(cb_args);
                if (bserr == 0) {
                    std::cout << "found existing blobstore!" << std::endl;
                    ctx->bs = bs;
                    ctx->init_done = true;
                } else if (bserr == -EILSEQ) {
                    std::cout << "didn't find existing blobstore, creating one" << std::endl;
                    spdk_bs_opts opts{};
                    spdk_bs_opts_init(&opts, sizeof(opts));
                    if (ctx->conf.cluster_sz > 0)
                            opts.cluster_sz = ctx->conf.cluster_sz;

                    // since load failed, the previous bs_dev got freed
                    spdk_bs_dev* bs_dev = nullptr;
                    int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, ctx, &bs_dev);
                    if (rc !=0) {
                        ctx->init_done = true;
                        return;
                    }
                    ctx->bs_dev = bs_dev;
                    spdk_bs_init(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                        auto final_ctx = static_cast<BlobStoreContext*>(cb_args);
                        if (bserr == 0) {
                            final_ctx->bs = bs;
                        } else {
                            std::cerr << "Blobstore init failed with error: " << bserr << std::endl;
                        }
                        final_ctx->init_done = true; // Signal the main thread
                    }, 
                    ctx);
                } else {
                    std::cout << "Error loading blobstore: " << bserr << std::endl;
                    ctx->init_done = true;
                }
            },
            context);
        };

        spdk_thread_send_msg(t, run_on_spdk_thread, ctx_raw);
        while(!ctx_->init_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        return std::tuple<spdk_bs_dev*, spdk_blob_store*>{ctx_->bs_dev, ctx_->bs};
    }
};