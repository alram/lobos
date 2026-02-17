#include <memory>

#include "spdk_awaitable.hpp"
#include "spdk_store.hpp"
#include "spdk_blobstoreinit.hpp"
#include "../index/spdk_index.hpp"

#define BLOB_METADATA "metadata"
#define BLOB_KEY "key"

void SpdkStore::shutdown_store() {
    stats_->shutdown_stats_engine();
    shutdown_fill_checker();

    std::cout << "Shutting down SPDK blobstore" << std::endl;
    auto unload_spdk = [](void* args) {
        auto ctx = static_cast<SpdkStore*>(args);
        if(ctx->io_channel_) {
            spdk_bs_free_io_channel(ctx->io_channel_);
        }
        if (ctx->bs_) {
            spdk_bs_unload(ctx->bs_, [](void *cb_arg, int bserrno) {
                if (bserrno)
                    std::cerr << "error unloading bs: " << bserrno << std::endl;
                auto ctx = static_cast<SpdkStore*>(cb_arg);
                ctx->store_shutdown = true;
            }, ctx);
        }

    };

    spdk_thread_send_msg(spdk_reactor_->get_thread(), unload_spdk, this);
    while (!store_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    std::cout << "Shutting down SPDK app" << std::endl;
    spdk_reactor_->stop();
    spdk_reactor_->join();

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
    SpdkIndexObject o = {};
    o.blob_id = spdk_blob_get_id(blb);
    std::string key = get_blob_metadata(blb, &o);

    bool skip = false;
    if (key.empty()) {
        std::cerr << "Couldn't find key for object: " << o.blob_id << ". This really shouldn't happen" << std::endl;
        skip = true;
    }

    // Remove the bucket name
    auto pos = key.find('/');
    if (pos != std::string::npos) {
        o.key = key.substr(pos+1);
    } else {
        o.key = key;
    }

    // It's possible we may have orphans due to async deletion failures
    // so we try to handle it.
    auto index_obj = ctx->index_->get_entry(key);
    if (index_obj) {
        if (index_obj->last_modified < o.last_modified) {
            auto blob_id = index_obj->blob_id;
            ctx->index_->rm_entry(key);
            ctx->do_delete_async(blob_id);
        } else {
            ctx->do_delete_async(o.blob_id);
            skip = true;
        }
    }

    if (!skip) {
        ctx->index_->add_entry(key, o);
        std::cout << "index builder - added index entry: " << key 
                << " blob: " << o.blob_id << std::endl;
    }
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

void SpdkStore::create_or_load_state_objects(std::string lobos_prefix) {
    struct StateObjCtx {
        SpdkStore* store;
        spdk_blob* blob;
        const char* xattr_name;
        bool complete{false};
    };

    auto create_objs = [](void *args) {
        auto ctx = static_cast<StateObjCtx*>(args);
        spdk_bs_create_blob(ctx->store->bs_, [](void* cb_arg, spdk_blob_id blob_id, int bserrno) {
            if (bserrno) { std::cerr << "couldn't create blob" << std::endl; }
            auto ctx = static_cast<StateObjCtx*>(cb_arg);
            if (ctx->xattr_name == ctx->store->lobos_user_prefix) {
                ctx->store->user_blob_id_ = blob_id;
            }
            if (ctx->xattr_name == ctx->store->lobos_bucket_prefix) {
                ctx->store->bucket_blob_id_ = blob_id;
            }
            // we need to open and sync and close or the blob is not actually created
            spdk_bs_open_blob(ctx->store->bs_, blob_id, [](void *cb_arg, spdk_blob *blob, int bserrno) {
                auto ctx = static_cast<StateObjCtx*>(cb_arg);
                ctx->blob = blob;
                spdk_blob_set_xattr(blob, BLOB_KEY, ctx->xattr_name, strlen(ctx->xattr_name));
                spdk_blob_sync_md(blob, [](void *cb_arg, int bserrno){
                    auto ctx = static_cast<StateObjCtx*>(cb_arg);
                    spdk_blob_close(ctx->blob, [](void *cb_arg, int bserrno) {
                        auto ctx = static_cast<StateObjCtx*>(cb_arg);
                        if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                        ctx->complete = true;
                    }, ctx);
                }, ctx);
            }, ctx);
        }, ctx); 
    };

    bool create = true;

    auto index_obj = index_->get_entry(lobos_prefix);
    if (index_obj) {
        if (lobos_prefix == lobos_user_prefix)
            user_blob_id_ = index_obj->blob_id;
        else if (lobos_prefix == lobos_bucket_prefix)
            bucket_blob_id_ = index_obj->blob_id;
        index_->rm_entry(lobos_prefix);
        create = false;
    }

    if (create) {
        auto ctx = new StateObjCtx{this, nullptr, lobos_prefix.c_str()};
        spdk_thread_send_msg(spdk_reactor_->get_thread(), create_objs, ctx);
        // todo harden
        while (!ctx->complete) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        delete(ctx);
    }

}

void SpdkStore::init_store(std::string devSpec) {
    std::unique_ptr<BlobStoreInitializer> dev;
    if (devSpec == "malloc") {
        std::cout << "Passed a malloc device." << std::endl;
        dev = std::make_unique<MallocBSInitializer>(conf_);
    } 
    else {
        // check the dev name is apt.
        spdk_pci_addr pci_addr;
        if(spdk_pci_addr_parse(&pci_addr, devSpec.c_str()) < 0) {
            throw std::runtime_error("Device not supported or does not exist: " + devSpec + ". Make sure to pass a valid PCI addr.");
        }
        std::cout << "Passed a NVMe device." << std::endl;
        dev = std::make_unique<NvmeBSInitializer>(devSpec, conf_);
    }
    
    auto [bdev, bs] = dev->initialize(spdk_reactor_->get_thread());
    if (!bdev) throw std::runtime_error("failed to initilize bdev");
    if (!bs) throw std::runtime_error("Failed to initialize blobstore");
    
    bs_ = bs;
    bdev_ = bdev;

    auto get_store_ready = [](void *args) {
        auto ctx = static_cast<SpdkStore*>(args);
        ctx->io_channel_ = spdk_bs_alloc_io_channel(ctx->bs_);
        ctx->io_unit_size_ = spdk_bs_get_io_unit_size(ctx->bs_);
        ctx->cluster_size_ = spdk_bs_get_cluster_size(ctx->bs_);
    };

    auto ctx = static_cast<SpdkStore*>(this);
    spdk_thread_send_msg(spdk_reactor_->get_thread(), get_store_ready, ctx);
    
    while(!ctx->io_unit_size_ || !ctx->io_channel_ || !ctx->cluster_size_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Collects usage stats and perf information
    stats_ = std::make_unique<SpdkStats>(spdk_reactor_->get_thread(), bdev_, bs_);
    stats_->start_stats_engine();

    // periodically checks store usage and make it ro
    // if above the user defined threshold
    make_store_ro_if_full_checker();

    // start the in-memory index.
    index_ = std::make_unique<SpdkIndex>();
    build_index_at_boot();
    while (!index_ready) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    // We create or load the state objects
    // state objects are used to store, users, buckets, etc.
    create_or_load_state_objects(lobos_user_prefix);
    create_or_load_state_objects(lobos_bucket_prefix);
}

asio::awaitable<std::map<std::string, ObjectBase>> SpdkStore::do_list(std::string& prefix) {
    co_return index_->s3_list_prefix_non_recursive(prefix);
}

asio::awaitable<bool> SpdkStore::create_bucket(std::string& key, BucketMetadata& md) {

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, key, md](auto complete) {
        auto ctx = new BucketCRUDOpCtx{this, key, md, nullptr, std::move(complete)};
        spdk_bs_open_blob(bs_, bucket_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
            if (bserrno) { std::cerr << "couldn't open bucket blob: " << bserrno << std::endl; }
            ctx->blob = blob;
            spdk_blob_set_xattr(blob,  ctx->key.c_str(), &ctx->md, sizeof(BucketMetadata));
            spdk_blob_sync_md(blob, [](void *cb_arg, int bserrno) {
                if(bserrno) { std::cerr << "couldn't sync md" << std::endl; }
                auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
                spdk_blob_close(ctx->blob, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
                    if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                    ctx->complete();
                    delete(ctx);
                }, ctx);
            }, ctx);
        }, ctx);
    });

    co_return true;
}

asio::awaitable<int> SpdkStore::delete_bucket(std::string& bucket) {
    // Check if the bucket is empty
    if (index_->bucket_has_keys(bucket, lobos_mpu_prefix))
        co_return -ENOTEMPTY;

    // delete the xattr from disk
    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, bucket](auto complete) {
        auto ctx = new BucketCRUDOpCtx{this, bucket, BucketMetadata{}, nullptr, std::move(complete)};
        spdk_bs_open_blob(bs_, bucket_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
            if (bserrno) { std::cerr << "couldn't open bucket blob: " << bserrno << std::endl; }
            ctx->blob = blob;
            spdk_blob_remove_xattr(blob, ctx->key.c_str());
            spdk_blob_sync_md(blob, [](void *cb_arg, int bserrno) {
                if(bserrno) { std::cerr << "couldn't sync md" << std::endl; }
                auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
                spdk_blob_close(ctx->blob, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<BucketCRUDOpCtx*>(cb_arg);
                    if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                    ctx->complete();
                    delete(ctx);
                }, ctx);
            }, ctx);
        }, ctx);
    });

    co_return 0;
}

std::vector<BucketRecord> SpdkStore::load_buckets() {
    std::vector<BucketRecord> buckets;

    auto load_buckets = [](void *args) {
        auto ctx = static_cast<BucketsListOpCtx*>(args);
        spdk_bs_open_blob(ctx->store->bs_, ctx->store->bucket_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            if (bserrno) { std::cerr << "couldn't open blob in spdk: " << bserrno << std::endl; }
            auto ctx = static_cast<BucketsListOpCtx*>(cb_arg);
            // load all users into the struct and close the blob
            spdk_xattr_names* buckets = nullptr;
            auto rc = spdk_blob_get_xattr_names(blob, &buckets);
            if (rc != 0) {
                std::cerr << "error retrieving xattrs for bucket state object" << std::endl;
            } else {
                size_t count = spdk_xattr_names_get_count(buckets);
                for (size_t i = 0; i < count; i++) {
                    std::string xattr = spdk_xattr_names_get_name(buckets, i);
                    if (xattr == BLOB_KEY)
                        continue;
                    BucketRecord b;
                    b.key = xattr;
                    const void* xattr_v = nullptr;
                    size_t xattr_s = 0;
                    auto rc = spdk_blob_get_xattr_value(blob, xattr.c_str(), &xattr_v, &xattr_s);
                    if (rc == 0 && (xattr_v && xattr_s >= sizeof(BucketMetadata))) {
                        const BucketMetadata* md = static_cast<const BucketMetadata*>(xattr_v);
                        b.created_at = md->created_at;
                        b.owner = md->owner;
                        b.is_shadow = md->is_shadow;
                        ctx->buckets->emplace_back(b);
                    }
                }
            }
            spdk_blob_close(blob, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BucketsListOpCtx*>(cb_arg);
                if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                ctx->complete = true;
            }, ctx);
        }, ctx);
    };

    auto ctx = new BucketsListOpCtx{this, &buckets, false};
    spdk_thread_send_msg(spdk_reactor_->get_thread(), load_buckets, ctx);
    // TODO harden then we could loop foreva on issue
    while (!ctx->complete) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    delete(ctx);

    return buckets;
}

asio::awaitable<int> SpdkStore::blob_create(IoCtx* ioctx, std::map<std::string, std::vector<uint8_t>>& xattrs) {
    if(read_only) {
        std::cerr << "Backend is full rejecting write" <<std::endl;
        co_return -ENOSPC;
    }

    struct BlobCreateCtx {
        SpdkStore* store;
        IoCtx* ioctx;
        std::map<std::string, std::vector<uint8_t>>& xattrs;
        std::vector<char*> xattr_names;
        std::function<void(int)> complete;
    };

    auto ctx = std::make_unique<BlobCreateCtx>(this, ioctx, xattrs);

    auto rc = co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);
        struct spdk_blob_opts opts;
        spdk_blob_opts_init(&opts, sizeof(opts));
        uint64_t cluster_count = ctx->ioctx->buffer->size() / ctx->store->cluster_size_;
        if(ctx->ioctx->buffer->size() % ctx->store->cluster_size_ != 0) {
            cluster_count++;
        }
        opts.num_clusters = cluster_count;

        for (auto& [k,v] : ctx->xattrs) {
            ctx->xattr_names.push_back(const_cast<char*>(k.c_str()));
        }
        opts.xattrs.names = ctx->xattr_names.data();
        opts.xattrs.count = ctx->xattr_names.size();
        opts.xattrs.ctx = &(ctx->xattrs);

        auto get_xattr_vals = [](void *xattr_ctx, const char *name, const void **value, size_t *value_len) {
            auto xattrs = static_cast<std::map<std::string, std::vector<uint8_t>>*>(xattr_ctx);
            auto it = xattrs->find(name);
            if (it != xattrs->end()) {
                *value = it->second.data();
                *value_len = it->second.size();
            } else {
                *value = NULL;
                *value_len = 0;
            }
        };
        opts.xattrs.get_value = get_xattr_vals;

        spdk_bs_create_blob_ext(ctx->store->bs_, &opts, [](void *cb_arg, spdk_blob_id blobid, int bserrno) {
            auto ctx = static_cast<BlobCreateCtx*>(cb_arg);
            if (bserrno) {
                std::cerr << "Error while creating blob: " << bserrno << std::endl;
            }
            ctx->ioctx->blob_id = blobid;
            ctx->complete(bserrno);
        }, ctx.get()); // spdk_bs_create_blob_ext
    });

    co_return rc;
}

asio::awaitable<int> SpdkStore::blob_open(IoCtx* ioctx) {
    if (!ioctx->blob_id)
        co_return -EBADFD;

    auto ctx = std::make_unique<BlobOpCtx>(this, ioctx);
    co_return co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);
        spdk_bs_open_blob(ctx->store->bs_, ctx->ioctx->blob_id, [](void *cb_arg, struct spdk_blob *blb, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) {
                std::cerr << "Error while opening blob: " << bserrno << std::endl;
                ctx->complete(bserrno);
                return;
            }
            ctx->ioctx->blob = blb;
            ctx->complete(0);
        }, ctx.get());
    });
}

asio::awaitable<int> SpdkStore::blob_write(IoCtx* ioctx) {
    if (!ioctx->blob)
        co_return -EBADFD;
    auto ctx = std::make_unique<BlobOpCtx>(this, ioctx);
    co_return co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);

        // We need to align the offset to blobstore io unit size
        uint64_t offset_units = ctx->ioctx->offset / ctx->store->io_unit_size_;
        uint64_t length_units = (ctx->ioctx->buffer->size() + ctx->store->io_unit_size_ - 1) / ctx->store->io_unit_size_;
        // TODO feature: If the total write is smaller than
        // cluster_size - io_unit; we store what's free
        // so that we can pack small objects together
        // in a single cluster.

        spdk_blob_io_write(ctx->ioctx->blob, 
            ctx->store->io_channel_,
            ctx->ioctx->buffer->data(),
            offset_units,
            length_units,
            [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) {
                    std::cerr << "write failed: " << bserrno 
                            << " offset: " << ctx->ioctx->offset
                            << " size: " << ctx->ioctx->buffer->size()
                            << " blob: " << (void*)ctx->ioctx->blob
                            << " blob_id: " << ctx->ioctx->blob_id
                            << std::endl;
                }
                ctx->complete(bserrno);
            }, ctx.get());
    });
}

asio::awaitable<int> SpdkStore::blob_read(IoCtx* ioctx) {
    if (!ioctx->blob)
        co_return -EBADF;

    auto ctx = std::make_unique<BlobOpCtx>(this, ioctx);
    co_return co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);

        uint64_t offset_units = ctx->ioctx->offset / ctx->store->io_unit_size_;
        uint64_t io_units = ctx->ioctx->buffer->size() / ctx->store->io_unit_size_;
        if (ctx->ioctx->buffer->size() % ctx->store->io_unit_size_ != 0)
            io_units++;

        spdk_blob_io_read(ctx->ioctx->blob, 
            ctx->store->io_channel_, 
            ctx->ioctx->buffer->data(), 
            offset_units, io_units, 
            [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) {
                    std::cerr << "Error reading blob: " << bserrno << std::endl;
                }
                ctx->complete(bserrno);
        }, ctx.get());
    });
}

asio::awaitable<int> SpdkStore::blob_rm(IoCtx* ioctx) {
    if (!ioctx->blob_id)
        co_return -EBADF;

    auto ctx = std::make_unique<BlobOpCtx>(this, ioctx);
    co_return co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);
        spdk_bs_delete_blob(ctx->store->bs_, ctx->ioctx->blob_id, [](void *cb_arg, int bserrno){
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) {
                std::cerr << "Error deleting blob: " << bserrno << std::endl;
            }
            ctx->complete(bserrno);
        }, ctx.get());
    });
}

asio::awaitable<int> SpdkStore::blob_close(IoCtx* ioctx) {
    if (!ioctx->blob)
        co_return -EBADF;

    auto ctx = std::make_unique<BlobOpCtx>(this, ioctx);
    co_return co_await spdk_awaitable<int>(spdk_reactor_->get_thread(), [&ctx](auto complete) {
        ctx->complete = std::move(complete);
        spdk_blob_close(ctx->ioctx->blob, [](void *cb_arg, int bserrno){
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) {
                std::cerr << "Error closing blob: " << bserrno << std::endl;
            }
            ctx->complete(bserrno);
        }, ctx.get());
    });
}

asio::awaitable<int> SpdkStore::do_write(std::string& oid, session_buffer& buffer) {
    auto ioctx = std::make_unique<IoCtx>(buffer);
    ioctx->key = oid;
    ioctx->md = std::make_unique<BlobMetadata>();
    ioctx->md->size = ioctx->buffer->size();
    ioctx->md->last_modified = std::time(nullptr);

    spdk_blob_id old_blob_id = index_->get_blob_id(oid);
    if (old_blob_id) {
        index_->unset_listable(oid);
    }

    std::map<std::string, std::vector<uint8_t>> xattrs {
        {BLOB_KEY,      as_bytes(oid)},
        {BLOB_METADATA, as_bytes(*ioctx->md)},
    };

    auto rc = co_await blob_create(ioctx.get(), xattrs);
    if (rc <0)
        co_return rc;

    rc = co_await blob_open(ioctx.get());
    if (rc < 0) {
        do_delete_async(ioctx->blob_id);
        co_return rc;
    }

    rc = co_await blob_write(ioctx.get());
    if (rc < 0) {
        blob_close(ioctx.get());
        do_delete_async(ioctx->blob_id);
        index_->set_listable(oid);
        co_return rc;
    }

    rc = co_await blob_close(ioctx.get());

    if (old_blob_id) {
        do_delete_async(old_blob_id);
    }

    SpdkIndexObject o{};
    o.key = oid;
    o.size = ioctx->md->size;
    o.last_modified = ioctx->md->last_modified;
    o.list = true;
    o.blob_id = ioctx->blob_id;
    index_->add_entry(oid, o);

    co_return 0;
}

asio::awaitable<int> SpdkStore::do_read(std::string& oid, uint64_t offset, session_buffer& buffer) {
    auto index_obj = index_->get_entry(oid);
    if(!index_obj)
        co_return -ENOENT;

    auto ioctx = std::make_unique<IoCtx>(buffer);
    ioctx->key = oid;
    ioctx->blob_id = index_obj->blob_id;
    ioctx->offset = offset;

    auto rc = co_await blob_open(ioctx.get());
    if (rc < 0)
        co_return rc;

    rc = co_await blob_read(ioctx.get());
    if (rc < 0)
        co_return rc;

    co_await blob_close(ioctx.get());

    co_return 0;
}

asio::awaitable<int> SpdkStore::do_delete(std::string& oid) {
    auto index_obj = index_->get_entry(oid);
    if(!index_obj)
        co_return -ENOENT;

     auto ioctx = std::make_unique<IoCtx>();
    ioctx->key = oid;
    ioctx->blob_id = index_obj->blob_id;

    auto rc = co_await blob_rm(ioctx.get());
    if (rc < 0)
        co_return rc;

    index_->rm_entry(oid);

    co_return 0;
}

void SpdkStore::do_delete_async(spdk_blob_id blob_id) {
    struct delete_ctx {
        SpdkStore* store;
        spdk_blob_id blob_id;
        
        static void execute(void* arg) {
            auto ctx = static_cast<delete_ctx*>(arg);
            spdk_bs_delete_blob(ctx->store->bs_, ctx->blob_id, &delete_ctx::on_complete, ctx);
        }
        
        static void on_complete(void* arg, int bserrno) {
            auto ctx = static_cast<delete_ctx*>(arg);
            if (bserrno) {
                std::cerr << "Background delete failed: " << bserrno << std::endl;
            }
            delete ctx;
        }
    };
    
    auto ctx = new delete_ctx{this, blob_id};
    spdk_thread_send_msg(spdk_reactor_->get_thread(), &delete_ctx::execute, ctx);
}

asio::awaitable<std::tuple<size_t, time_t>> SpdkStore::do_metadata_req(std::string& oid) {
    co_return index_->get_object_md(oid);
}

std::unordered_map<std::string, std::unordered_map<std::string,Multipart>>  SpdkStore::get_active_mpus() {
    std::unordered_map<std::string, std::unordered_map<std::string,Multipart>> mpus = {};

    for (auto object : index_->list_keys(lobos_mpu_prefix)) {
        std::istringstream ss(object.substr(lobos_mpu_prefix.length()+1));
        std::string bucket;
        std::string upload_id;
        std::getline(ss, bucket, '_');        

        // TODO xattr for init time, need support in do_write();
        Multipart mp;
        std::getline(ss, mp.key, '_');
        std::getline(ss, upload_id, '_');
        mp.init_time = 0;
        mpus[bucket].emplace(upload_id, mp);
    }

    // Parts are stored in format:
    // <bucket>/.__lobos__mpu__/<upload_id>_<partN>_<key>
    for (auto& [bucket, m] : mpus) {
        std::string pref = bucket + "/" + lobos_mpu_prefix + "/";

        for (auto object : index_->list_keys(pref)) {
            std::istringstream ss(object.substr(pref.length()));
            std::string upload_id;
            std::string part;
            std::string key;

            std::getline(ss, upload_id, '_');
            std::getline(ss, part, '_');
            std::getline(ss, key, '_');

            auto size = std::get<0>(index_->get_object_md(object));

            m[upload_id].parts.emplace(std::stoi(part), Part{
                .size = size,
                .etag = "0000"
            });

            m[upload_id].current_size += size;  
        }
    }
    return mpus;
}

asio::awaitable<int> SpdkStore::do_create_mpu(std::string& oid, std::string& upload_id) {
    spdk_buffer buff(0);
    std::string key = lobos_mpu_prefix + "/"  + oid + "_" + upload_id + "_initiated";
    // TODO xattr for creation date
    co_return co_await do_write(key, buff);
}

// TODO eventually do something better here
// we read all the objects and copy them in a buffer
// then write the whole thing because io misaligned with io units
// are messing up the whole thing
// Needs to be reworked to be efficient
asio::awaitable<int> SpdkStore::do_assemble_mpu(std::string& bucket, std::string& upload_id, Multipart& mp, std::vector<int>& parts) {
    auto buffer = std::make_unique<spdk_buffer>(mp.current_size);
    auto ioctx = std::make_unique<IoCtx>(*buffer);

    ioctx->key = bucket + "/" + mp.key;
    ioctx->md = std::make_unique<BlobMetadata>();
    ioctx->md->size = mp.current_size;
    ioctx->md->last_modified = std::time(nullptr);

    std::map<std::string, std::vector<uint8_t>> xattrs {
        {BLOB_KEY,      as_bytes(ioctx->key)},
        {BLOB_METADATA, as_bytes(*ioctx->md)},
    };

    auto rc = co_await blob_create(ioctx.get(), xattrs);
    if (rc < 0)
        co_return rc;
    size_t offset =0;
    for (auto& part : parts) {
        auto k = bucket + "/" + lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key;
        spdk_buffer part_buffer(mp.parts[part].size);
        rc  = co_await do_read(k, 0, part_buffer);
        if (rc < 0)
            co_return rc;
        memcpy(buffer->data() + offset, part_buffer.data(), part_buffer.size());
        offset += part_buffer.size();
    }
    rc = co_await blob_open(ioctx.get());
    if (rc < 0)
        co_return rc;

    rc = co_await blob_write(ioctx.get());
    if (rc <0) {
        co_await blob_close(ioctx.get());
        do_delete_async(ioctx->blob_id);
        co_return rc;
    }
    rc = co_await blob_close(ioctx.get());

    for (const auto& part : mp.parts) {
        auto k = bucket + "/" + lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key;
        do_delete_async(index_->get_blob_id(k));
    }

    auto init_key = lobos_mpu_prefix + "/" + bucket + "_" + mp.key + "_" + upload_id + "_initiated";
    co_await do_delete(init_key);

    SpdkIndexObject o{};
    o.key = ioctx->key;
    o.size = ioctx->md->size;
    o.last_modified = ioctx->md->last_modified;
    o.list = true;
    o.blob_id = ioctx->blob_id;
    index_->add_entry(ioctx->key, o);

    co_return 0;
}

asio::awaitable<int> SpdkStore::do_abort_mpu(std::string& oid, std::string& bucket, std::string& upload_id, Multipart& mp) {
    for (const auto& part : mp.parts) {
        auto k = bucket + "/" + lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key;
        do_delete_async(index_->get_blob_id(k));
    }

    auto init_key = lobos_mpu_prefix + "/" + oid + "_" + upload_id + "_initiated";
    co_await do_delete(init_key);

    co_return 0;
}

std::string SpdkStore::get_blob_metadata(spdk_blob* blob, SpdkIndexObject* o) {
    const void* xattr_v = nullptr;
    size_t xattr_s = 0;

    auto rc = spdk_blob_get_xattr_value(blob, BLOB_METADATA, &xattr_v, &xattr_s);
    if (rc == 0 && xattr_s == sizeof(BlobMetadata)) {
        auto md = static_cast<const BlobMetadata*>(xattr_v);
        o->last_modified = md->last_modified;
        o->size = md->size;
    }

    rc = spdk_blob_get_xattr_value(blob, BLOB_KEY, &xattr_v, &xattr_s);
    if (rc == 0 && xattr_s > 0) {
        return std::string(static_cast<const char*>(xattr_v), xattr_s);
    }

    return {};
}

int SpdkStore::metadata_add_user(User u) {
    // We have one global blob for all users
    // and each key is an xattrs, meh arch
    // will need to think this through until
    // after there's a persistent index for 
    // spdk

    auto add_user = [](void* args) {
        auto ctx = static_cast<UserOpCtx*>(args);
        spdk_bs_open_blob(ctx->store->bs_, ctx->store->user_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            if (bserrno) { std::cerr << "couldn't open blob" << std::endl; }
            auto ctx = static_cast<UserOpCtx*>(cb_arg);
            std::string key = (*ctx->users)[0].name + "_" + (*ctx->users)[0].key + "_" + (*ctx->users)[0].secret + "_" + (*ctx->users)[0].backend;
            auto rc = spdk_blob_set_xattr(blob, key.c_str(), nullptr, 0);
            ctx->blob = blob;
            spdk_blob_sync_md(blob, [](void *cb_arg, int bserrno) {
                if(bserrno) { std::cerr << "couldn't sync md" << std::endl; }
                auto ctx = static_cast<UserOpCtx*>(cb_arg);
                spdk_blob_close(ctx->blob, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<UserOpCtx*>(cb_arg);
                    if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                    ctx->complete = true;
                }, ctx);
            }, ctx);
        }, ctx);
    };

    std::vector<User> users = {u};
    auto ctx = new UserOpCtx{this, &users, nullptr, false};
    spdk_thread_send_msg(spdk_reactor_->get_thread(), add_user, ctx);

    // TODO harden 
    while (!ctx->complete) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } 

    delete(ctx);
    return 0;
}

int SpdkStore::metadata_add_key(User u) {
    // for spdk its the same shtuff as add user
    // sicne everything is on one obj
    return metadata_add_user(u);
}

std::vector<User> SpdkStore::metadata_list_users(std::string filter) {
    std::vector<User> users = {};

    auto load_users = [](void *args) {
        auto ctx = static_cast<UserOpCtx*>(args);
        spdk_bs_open_blob(ctx->store->bs_, ctx->store->user_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            if (bserrno) { std::cerr << "couldn't open blob in spdk: " << bserrno << std::endl; }
            auto ctx = static_cast<UserOpCtx*>(cb_arg);
            // load all users into the struct and close the blob
            spdk_xattr_names* users = nullptr;
            auto rc = spdk_blob_get_xattr_names(blob, &users);
            if (rc != 0) {
                std::cerr << "error retrieving xattrs for users state object" << std::endl;
            } else {
                size_t count = spdk_xattr_names_get_count(users);
                for (size_t i = 0; i < count; i++) {
                    // we get name of the xattr which is going to be
                    // user_key_secret_backend
                    std::string xattr = spdk_xattr_names_get_name(users, i);
                    if (xattr == BLOB_KEY)
                        continue;
                    User u;
                    std::istringstream ss(xattr);
                    std::getline(ss, u.name, '_');
                    std::getline(ss, u.key, '_');
                    std::getline(ss, u.secret, '_');
                    std::getline(ss, u.backend);
                    
                    ctx->users->emplace_back(u);
                }
            }
            spdk_blob_close(blob, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<UserOpCtx*>(cb_arg);
                if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                ctx->complete = true;
            }, ctx);
        }, ctx);
    };

    auto ctx = new UserOpCtx{this, &users, nullptr, false};
    spdk_thread_send_msg(spdk_reactor_->get_thread(), load_users, ctx);

    // TODO harden then we could loop foreva on issue
    while (!ctx->complete) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    delete(ctx);
    return users;
}

bool SpdkStore::metadata_remove_user(std::string& name) {
    return metadata_remove_user_keys(name, "");
}

bool SpdkStore::metadata_rm_key(std::string user, User u) {
    std::string key = user + "_" + u.key + "_" + u.secret + "_" + u.backend;
    return metadata_remove_user_keys(user, key);
}

bool SpdkStore::metadata_remove_user_keys(std::string& name, std::string key) {
    struct OpCtx {
        SpdkStore* store;
        std::vector<std::string> keys;
        bool complete{false};
    };
    
    auto remove_user = [](void* args) {
        auto ctx = static_cast<OpCtx*>(args);
        spdk_bs_open_blob(ctx->store->bs_, ctx->store->user_blob_id_, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            auto ctx = static_cast<OpCtx*>(cb_arg);
            if (bserrno) { std::cerr << "error opening blob" << std::endl; }
            for (const auto& k : ctx->keys) {
                spdk_blob_remove_xattr(blob, k.c_str());
            }
            spdk_blob_close(blob, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<OpCtx*>(cb_arg);
                if (bserrno) { std::cerr << "couldn't close blob" << std::endl; }
                ctx->complete = true;
            }, ctx);
        }, ctx);
    };

    std::vector<std::string> keys;
    if (key.empty()) {
        auto users = metadata_list_users("");
        for (const auto& u : users) {
            if (u.name == name) 
                keys.emplace_back(u.name + "_" + u.key + "_" + u.secret + "_" + u.backend);
        }
    } else {
        keys.emplace_back(key);
    }


    auto ctx = new OpCtx{this, keys, false};
    spdk_thread_send_msg(spdk_reactor_->get_thread(), remove_user, ctx);

    // TODO harden then we could loop foreva on issue
    while (!ctx->complete) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    delete(ctx);
    return true;
}