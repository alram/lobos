#include <iostream>

#include <spdk/jsonrpc.h>

#define SPDK_RPC_SOCK_PATH "/var/tmp/spdk.sock"

//This is fragile. It makes hte name consistent
// between malloc and nvme devs however if the 
// nvme dev has more than 1 NS kaboom
// TODO: hardens that codepath.
#define LOBOS_BDEV_NAME_BASE "Lobos0"
#define LOBOS_BDEV_NAME "Lobos0n1"


static void base_bdev_event_cb(enum spdk_bdev_event_type type, spdk_bdev *bdev, void *event_ctx) {
	SPDK_WARNLOG("Unsupported bdev event: type %d\n", type);
    //todo add event handling
}

class BlobStoreInitalizer {
    public:
        virtual ~BlobStoreInitalizer() = default;
        virtual spdk_blob_store* initialize(spdk_thread* t) = 0;
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

struct InitContext {
    bool done = false;
    spdk_blob_store* bs = nullptr;
};

// We keep that shit raw for Malloc since it's really only
// for testing (and CI if ever... lol)... send and pray
class MallocBSInitializer : public BlobStoreInitalizer {
    public:
        spdk_blob_store* initialize(spdk_thread* t) override {
            std::string params = "{\"name\":\""+ std::string(LOBOS_BDEV_NAME) + "\",\"num_blocks\": 32768,\"block_size\":512}";
            send_rpc_req("bdev_malloc_create", params);

            InitContext ctx;
            auto run_on_spdk_thread = [](void *args) {
                auto* context = static_cast<InitContext*>(args);
                spdk_bs_dev* bs_dev = nullptr;
                int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, nullptr, &bs_dev);
                if (rc !=0) {
                    context->done = true;
                    return;
                }

                spdk_bs_opts opts{};
                spdk_bs_opts_init(&opts, sizeof(opts));
                opts.cluster_sz = 131072;

                spdk_bs_init(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                    auto* final_ctx = static_cast<InitContext*>(cb_args);
                    if (bserr == 0) {
                        final_ctx->bs = bs;
                    } else {
                        std::cerr << "Blobstore init failed with error: " << bserr << std::endl;
                    }
                    final_ctx->done = true; // Signal the main thread
                }, args);
            };
            
            spdk_thread_send_msg(t, run_on_spdk_thread, &ctx);

            // just wait in a loop; could do an awaiter but :shrug: it's all in the init path who cares
            while(!ctx.done) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            return ctx.bs;
        }

};


// For NVMe, for now we only support directly attached
// nvme devs (altho technically adding nvmeof is doable)
// We need to connect the nvme controller to a bdev
// then check if a blobstore already exists on it
// if so we load it, if not we create it
class NvmeBSInitializer : public BlobStoreInitalizer {
private:
    std::string pci_addr;
public:
    NvmeBSInitializer(std::string addr) : pci_addr(addr) {}
    spdk_blob_store* initialize(spdk_thread* t) override {

        //TODO: using C API instead of RPC might be more robust..
        // need to add: traddr and trype 
        std::string params = "{\"name\":\""+ std::string(LOBOS_BDEV_NAME_BASE) + "\",\"traddr\":\""+ pci_addr +"\",\"trtype\":\"PCIe\"}";
        send_rpc_req("bdev_nvme_attach_controller", params);

        // Now that we have a bdev, we can try to load blobstore and if it fails just
        // create it
        InitContext ctx;
        auto run_on_spdk_thread = [](void* args) {
            auto context = static_cast<InitContext*>(args);
            spdk_bs_dev* bs_dev = nullptr;

            int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, nullptr, &bs_dev);
            if (rc !=0) {
                context->done = true;
                return;
            }

            spdk_bs_opts opts{};
            spdk_bs_opts_init(&opts, sizeof(opts));
            opts.cluster_sz = 131072;

            spdk_bs_load(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                auto ctx = static_cast<InitContext*>(cb_args);
                if (bserr == 0) {
                    std::cout << "found existing blobstore!" << std::endl;
                    ctx->bs = bs;
                    ctx->done = true;
                } else if (bserr == -EILSEQ) {
                    std::cout << "didn't find existing blobstore, creating one" << std::endl;
                    spdk_bs_opts opts{};
                    spdk_bs_opts_init(&opts, sizeof(opts));
                    opts.cluster_sz = 131072;

                    // since load failed, the previous bs_dev go freed
                    spdk_bs_dev* bs_dev = nullptr;
                    int rc = spdk_bdev_create_bs_dev_ext(LOBOS_BDEV_NAME, base_bdev_event_cb, nullptr, &bs_dev);
                    if (rc !=0) {
                        ctx->done = true;
                        return;
                    }
                    spdk_bs_init(bs_dev, &opts, [](void *cb_args, spdk_blob_store* bs, int bserr) {
                        auto* final_ctx = static_cast<InitContext*>(cb_args);
                        if (bserr == 0) {
                            final_ctx->bs = bs;
                        } else {
                            std::cerr << "Blobstore init failed with error: " << bserr << std::endl;
                        }
                        final_ctx->done = true; // Signal the main thread
                    }, 
                    ctx);
                } else {
                    std::cout << "Error loading blobstore: " << bserr << std::endl;
                    ctx->done = true;
                }
            },
            context);
        };

        spdk_thread_send_msg(t, run_on_spdk_thread, &ctx);
        // just wait in a loop; could do an awaiter but :shrug: it's all in the init path who cares
        while(!ctx.done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        return ctx.bs; // Return the resulting pointer
    }
};