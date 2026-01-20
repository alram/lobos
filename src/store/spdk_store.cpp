#include <memory>

#include "spdk_awaitable.hpp"
#include "spdk_store.hpp"
#include "spdk_blobstoreinit.hpp"

#define BLOB_META_NAME "metadata"
#define BLOB_META_KEY "key"

Task SpdkStore::shutdown_blobstore() {
    SpdkAwaiter awaiter;
    std::pair<SpdkStore*, SpdkAwaiter*> context = {this, &awaiter};

    auto unload_blobstore = [](void* args) {
        auto ctx = static_cast<std::pair<SpdkStore*,SpdkAwaiter*>*>(args);
        spdk_bs_free_io_channel(ctx->first->io_channel_);
        spdk_bs_unload(ctx->first->bs_, [](void *cb_arg, int bserrno) {
            auto a = static_cast<SpdkAwaiter*>(cb_arg);
            a->complete(bserrno);
        }, ctx->second);
    };
    spdk_thread_send_msg(spdk_reactor_->get_thread(), unload_blobstore, &context);

    auto [rc, res] = co_await awaiter;
    std::cout << "blobstore shutdown rc: " << rc << std::endl;
    
    //TODO detach nvme controller from spdk instance.
    // send_rpc_req("bdev_nvme_detach_controller", 
    //              "{\"name\":\"" + std::string(LOBOS_BDEV_NAME_BASE) + "\"}");
    
    co_return;
}

// Static callback that handles each blob and chains to the next
void SpdkStore::iter_cb(void *cb_arg, struct spdk_blob *blb, int bserrno) {
    auto ctx = static_cast<SpdkStore*>(cb_arg);
    if (bserrno == -ENOENT) {
        // No more blobs (or empty store on first call)
        std::cout << "index build complete" << std::endl;
        ctx->index_ready = true;
        return;
    }
    if (bserrno != 0) {
        std::cout << "error in bs iterator: " << bserrno << std::endl;
        ctx->index_ready = true;  // or set an error flag
        return;
    }
    // Process this blob
    Object o = {};
    const char *key = nullptr;
    o.blob_id = spdk_blob_get_id(blb);
    
    get_blob_metadata(blb, &o, key);
    ctx->index_->add_entry(key, o);
    std::cout << "index builder - added index entry: " << key 
              << " size: " << o.size << std::endl;

    // Chain to next blob - callback will fire for next blob (or ENOENT when done)
    spdk_bs_iter_next(ctx->bs_, blb, iter_cb, ctx);
}

void SpdkStore::build_index_at_boot() {
    auto blob_iterate = [](void* args) {
        auto ctx = static_cast<SpdkStore*>(args);
        spdk_bs_iter_first(ctx->bs_, iter_cb, ctx);
    };

    std::cout << "attempting to rebuild index if exist" << std::endl;
    spdk_thread_send_msg(spdk_reactor_->get_thread(), blob_iterate, this);
}

void SpdkStore::init_store(std::string devSpec) {
    std::unique_ptr<BlobStoreInitalizer> dev;
    if (devSpec == "malloc") {
        std::cout << "Passed a malloc device." << std::endl;
        dev = std::make_unique<MallocBSInitializer>();
    } 
    else {
        // check the dev name is apt.
        spdk_pci_addr pci_addr;
        if(spdk_pci_addr_parse(&pci_addr, devSpec.c_str()) < 0) {
            throw std::runtime_error("Device not supported or does not exist: " + devSpec + ". Make sure to pass a valid PCI addr.");
        }
        std::cout << "Passed a NVMe device." << std::endl;
        dev = std::make_unique<NvmeBSInitializer>(devSpec);
    }
    
    spdk_blob_store* bs = dev->initialize(spdk_reactor_->get_thread());

    if (!bs) throw std::runtime_error("Failed to initialize blobstore");

    bs_ = bs;

    auto alloc_channel = [](void *args) {
        auto ctx = static_cast<SpdkStore*>(args);
        ctx->io_channel_ = spdk_bs_alloc_io_channel(ctx->bs_);
        ctx->io_unit_size_ = spdk_bs_get_io_unit_size(ctx->bs_);
    };

    auto ctx = static_cast<SpdkStore*>(this);
    spdk_thread_send_msg(spdk_reactor_->get_thread(), alloc_channel, ctx);
    
    while(!ctx->io_unit_size_ || !ctx->io_channel_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::cout << "alloc done! io unit size: " << io_unit_size_ << std::endl;
    }

    backend = "spdk_blobstore";

    // start the in-memory index.
    index_ = std::make_unique<IndexStore>();
    build_index_at_boot();
    while (!index_ready) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
}

asio::awaitable<void> SpdkStore::do_list(std::string_view prefix, std::vector<uint8_t>& buffer) {
    // this logic should really be in the index
    auto it = index_->index.lower_bound(prefix);
    std::string pre;

    std::cout << "doing list with prefix: " << prefix << std::endl;
    for(; it != index_->index.end(); it++) {
        if (!it->first.starts_with(prefix)) break;
        std::cout << "in list: " << it->first << std::endl;
        std::string s;
        // if the element contains `/` its a dir so we just display the common prefix thing
        auto key_comp = it->first;
        key_comp.erase(0, prefix.length());
        std::cout << "key comp: " << key_comp << std::endl;
        auto pos = key_comp.find('/');
        if (pos != std::string::npos) {
            // there's a catch where if you do `dir/` you lsit the dir
            // but if you do `dir` you list the pre. so if pre starts with `/`
            // we know we need to previous dir
            if (key_comp.starts_with('/')) {
                //todo
            }

            if (pre == key_comp.substr(0, pos+1)) continue;

            pre = key_comp.substr(0, pos+1);
            s =  "<CommonPrefixes>"
                "<Prefix>" + pre + "</Prefix>"
                "</CommonPrefixes>";
        } else {
            s ="<Contents>"
                "<Key>" + it->first + "</Key>"
                "<LastModified>" + std::to_string(it->second.last_modified) + "</LastModified>"
                "<Size>" + std::to_string(it->second.size) + "</Size>"
                "</Contents>";
        }

        buffer.insert(buffer.end(), s.begin(), s.end());
    }
    co_return;
}

asio::awaitable<size_t> SpdkStore::do_write(std::string o, std::span<uint8_t> data) {

    auto* ioctx = new IoCtx(data);
    ioctx->key = o;
    ioctx->md = std::make_unique<BlobMetadata>();
    ioctx->md->size = ioctx->size;
    ioctx->md->last_modified = std::time(nullptr);
    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto* op = new BlobOpCtx{this, ioctx, std::move(complete)};
        spdk_bs_create_blob(bs_, [](void* cb_arg, spdk_blob_id blob_id, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            // std::cout << "create blob rc: " << bserrno << std::endl;
            if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
            ctx->ioctx->blob_id = blob_id;

            spdk_bs_open_blob(ctx->store->bs_, blob_id, [](void *cb_arg, spdk_blob *blob, int bserrno) {
                // std::cout << "open blob rc: " << bserrno << std::endl;
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                ctx->ioctx->blob = blob;
                auto cluster_size = spdk_bs_get_cluster_size(ctx->store->bs_);
                // We need to resize the blob to fit all the data. The min unit is a cluster
                // For instance if we write  150KiB on a 128KiB cluster size, we need 2 clusters
                uint64_t clusters = ctx->ioctx->size / cluster_size;
                if(ctx->ioctx->size % cluster_size != 0) {
                    clusters++;
                }
                // Write xattrs, this is sync. TOOD Check for errs
                int rc = spdk_blob_set_xattr(blob, BLOB_META_NAME, ctx->ioctx->md.get(), sizeof(BlobMetadata));
                // std::cout << "set xattr1 rc: " << rc << std::endl;
                // rc = spdk_blob_set_xattr(blob, BLOB_META_KEY, ctx->ioctx->key.c_str(), strlen(ctx->ioctx->key.c_str()));
                rc = spdk_blob_set_xattr(blob, BLOB_META_KEY, ctx->ioctx->key.c_str(), ctx->ioctx->key.size() + 1);
                // std::cout << "set xattr2 rc: " << rc << std::endl;

                spdk_blob_resize(blob, clusters, [](void *cb_arg, int bserrno) {
                    // std::cout << "resize blob rc: " << bserrno << std::endl;
                    auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                    if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                    spdk_blob_sync_md(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                        // std::cout << "sync md rc: " << bserrno << std::endl;
                        auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                        //finally we write
                        uint64_t write_size = ctx->ioctx->size / ctx->store->io_unit_size_;
                        if (ctx->ioctx->size % ctx->store->io_unit_size_ != 0) {
                            write_size++;
                        }
                        // TODO get rid of memcpy here, we should reuse the same buffer from beast
                        ctx->dma_buf = spdk_malloc(ctx->ioctx->buffer.size(), 0x1000, nullptr, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
                        memcpy(ctx->dma_buf, ctx->ioctx->buffer.data(), ctx->ioctx->buffer.size());
                        spdk_blob_io_write(ctx->ioctx->blob, 
                            ctx->store->io_channel_, 
                            ctx->dma_buf, 0, write_size,
                            [](void *cb_arg, int bserrno) {
                                // std::cout << "write blob rc: " << bserrno << std::endl;
                                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                                if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                                spdk_blob_close(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                                    // std::cout << "close blob rc: " << bserrno << std::endl;
                                    auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                                    // Add entry to index - TODO this is temporary 
                                    // Note that we need to check if an entry already exists for this
                                    // if it does, we delete old blob + update index
                                    ctx->store->index_->add_entry(ctx->ioctx->key, {
                                        ctx->ioctx->md->size,
                                        ctx->ioctx->md->last_modified,
                                        ctx->ioctx->blob_id,
                                    });
                                    ctx->complete(0);
                                    delete ctx;
                                }, ctx); // spdk_blob_close
                        }, ctx); // spdk_blob_io_write
                    }, ctx); // spdk_blob_sync_md
                }, ctx); //spdk_blob_resize

            }, ctx); //spdk_bs_open_blob
        }, op); //spdk_bs_create_blob
    });

    co_return data.size();
}

asio::awaitable<int> SpdkStore::do_read(std::string o, uint64_t offset, std::span<uint8_t> buffer) {
    auto it = index_->index.find(o);
    if (it == index_->index.end()) {
        co_return -ENOENT;
    }

    auto* ioctx = new IoCtx(buffer);
    ioctx->key = o;
    ioctx->blob_id = it->second.blob_id;

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto ctx = new BlobOpCtx{this, ioctx, std::move(complete)};
        spdk_bs_open_blob(ctx->store->bs_, ctx->ioctx->blob_id, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
            ctx->ioctx->blob = blob;
            ctx->dma_buf = spdk_malloc(ctx->ioctx->buffer.size(), 0x1000, nullptr, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
            uint64_t io_units = ctx->ioctx->buffer.size() / ctx->store->io_unit_size_;
            if (ctx->ioctx->buffer.size() % ctx->store->io_unit_size_ != 0) {
                io_units++;
            }
            spdk_blob_io_read(blob, ctx->store->io_channel_, ctx->dma_buf, 0, io_units, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                memcpy(ctx->ioctx->buffer.data(), ctx->dma_buf, ctx->ioctx->buffer.size());
                spdk_blob_close(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                ctx->complete(0);
                delete ctx;
                }, ctx); // spdk_blob_close
            }, ctx); //spdk_blob_io_read
        }, ctx); //spdk_bs_open_blob
    });

    co_return 0;
}

asio::awaitable<bool> SpdkStore::do_delete(std::string_view o) {
    auto it = index_->index.find(o);
    if (it == index_->index.end()) {
        co_return -ENOENT;
    }

    auto* ioctx = new IoCtx();
    ioctx->key = o;
    ioctx->blob_id = it->second.blob_id;

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto ctx = new BlobOpCtx{this, ioctx, std::move(complete)};
        spdk_bs_delete_blob(ctx->store->bs_, ctx->ioctx->blob_id, [](void *cb_arg, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
            ctx->store->index_->index.erase(ctx->ioctx->key);
            ctx->complete(0);
            delete ctx;
        }, ctx);
    });

    co_return true;
}

asio::awaitable<std::tuple<size_t, time_t>> SpdkStore::do_metadata_req(std::string_view o) {
    auto it = index_->index.find(o);
    if (it != index_->index.end()) {
        co_return std::tuple<size_t, time_t>{it->second.size, it->second.last_modified};
    }
    co_return std::tuple<size_t, time_t>{0,0};
}

void SpdkStore::get_blob_metadata(spdk_blob* blob, Object* o, const char*& key) {
    // This is all the xattr
    const void* xattr_v = nullptr;
    size_t xattr_s = 0;
    auto rc = spdk_blob_get_xattr_value(blob, BLOB_META_NAME, &xattr_v, &xattr_s);
    if (rc == 0 && (xattr_v && xattr_s >= sizeof(BlobMetadata))) {
        const BlobMetadata* blob_meta_ptr = static_cast<const BlobMetadata*>(xattr_v);
        o->last_modified = blob_meta_ptr->last_modified;
        o->size = blob_meta_ptr->size;
    }

    // This is the object name xattr
    rc = spdk_blob_get_xattr_value(blob, BLOB_META_KEY, &xattr_v, &xattr_s);
    if (rc == 0) {
        key = static_cast<const char*>(xattr_v);
        std::cout << "found key: " << key << std::endl;
    }
}

