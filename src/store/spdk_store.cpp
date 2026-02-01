#include <memory>

#include "spdk_awaitable.hpp"
#include "spdk_store.hpp"
#include "spdk_blobstoreinit.hpp"

#define BLOB_META_NAME "metadata"
#define BLOB_META_KEY "key"

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
    Object o = {};
    const char *key = nullptr;
    o.blob_id = spdk_blob_get_id(blb);
    
    get_blob_metadata(blb, &o, key);

    // It's possible we may have orphans due to async deletion failures
    // so we try to handle it.
    bool skip = false;
    auto it = ctx->index_->index.find(key);
    if (it != ctx->index_->index.end()) {
        if (it->second.last_modified < o.last_modified) {
            ctx->index_->index.erase(key);
            ctx->do_delete_async(it->second.blob_id);
        } else {
            ctx->do_delete_async(o.blob_id);
            skip = true;
        }
    }

    if (!skip) {
        ctx->index_->add_entry(key, o);
        std::cout << "index builder - added index entry: " << key 
                << " size: " << o.size << std::endl;
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

    auto alloc_channel = [](void *args) {
        auto ctx = static_cast<SpdkStore*>(args);
        ctx->io_channel_ = spdk_bs_alloc_io_channel(ctx->bs_);
        ctx->io_unit_size_ = spdk_bs_get_io_unit_size(ctx->bs_);
    };

    auto ctx = static_cast<SpdkStore*>(this);
    spdk_thread_send_msg(spdk_reactor_->get_thread(), alloc_channel, ctx);
    
    while(!ctx->io_unit_size_ || !ctx->io_channel_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Collects usage stats and perf information
    stats_ = std::make_unique<SpdkStats>(spdk_reactor_->get_thread(), bdev_, bs_);
    stats_->start_stats_engine();

    // periodically checks store usage and make it ro
    // if above the user defined threshold
    make_store_ro_if_full_checker();

    // start the in-memory index.
    index_ = std::make_unique<IndexStore>();
    build_index_at_boot();
    while (!index_ready) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
}

asio::awaitable<void> SpdkStore::do_list(std::string_view prefix, session_buffer& buffer) {
    // this logic should really be in the index
    auto it = index_->index.lower_bound(prefix);
    std::string pre;

    for(; it != index_->index.end(); it++) {
        if (!it->first.starts_with(prefix)) break;
        if (it->first.starts_with(lobos_state_prefix)) continue;
        std::string s;
        // if the element contains `/` its a dir so we just display the common prefix thing
        auto key_comp = it->first;
        key_comp.erase(0, prefix.length());
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
                "<LastModified>" + to_iso8601(it->second.last_modified) + "</LastModified>"
                "<Size>" + std::to_string(it->second.size) + "</Size>"
                "</Contents>";
        }
        buffer.append(s);
    }
    co_return;
}

asio::awaitable<int> SpdkStore::create_and_size_blob(IoCtx* ioctx) {
    if(read_only) {
        std::cerr << "Backend is full rejecting write" <<std::endl;
        co_return -ENOSPC;
    }

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto op = new BlobOpCtx{this, ioctx, std::move(complete)};
        spdk_bs_create_blob(bs_, [](void* cb_arg, spdk_blob_id blob_id, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
            ctx->ioctx->blob_id = blob_id;
            spdk_bs_open_blob(ctx->store->bs_, blob_id, [](void *cb_arg, spdk_blob *blob, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                ctx->ioctx->blob = blob;
                auto cluster_size = spdk_bs_get_cluster_size(ctx->store->bs_);
                uint64_t clusters = ctx->ioctx->buffer->size() / cluster_size;
                if(ctx->ioctx->buffer->size() % cluster_size != 0) {
                    clusters++;
                }
                // Write xattrs, this is sync. TOOD Check for errs
                spdk_blob_set_xattr(blob, BLOB_META_NAME, ctx->ioctx->md.get(), sizeof(BlobMetadata));
                spdk_blob_set_xattr(blob, BLOB_META_KEY, ctx->ioctx->key.c_str(), ctx->ioctx->key.size() + 1);

                spdk_blob_resize(blob, clusters, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                    if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                    spdk_blob_sync_md(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                        auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                        if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                        ctx->complete(0);
                    }, ctx); // sync_md
                }, ctx); // blob_resize
            }, ctx); //open_blob
        }, op); // create_blob
    });

    co_return 0;
}

asio::awaitable<int> SpdkStore::write_data(IoCtx* ioctx) {
    if (!ioctx->blob_id || !ioctx->blob)
        co_return -ENOENT;
    int size = ioctx->buffer->size();

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto ctx = new BlobOpCtx{this, ioctx, std::move(complete)};
        uint64_t write_size = ctx->ioctx->buffer->size() / ctx->store->io_unit_size_;
        if (ctx->ioctx->buffer->size() % ctx->store->io_unit_size_ != 0) {
            write_size++;
        }
        uint64_t offset_units = ctx->ioctx->offset / ctx->store->io_unit_size_;
        spdk_blob_io_write(ctx->ioctx->blob, 
            ctx->store->io_channel_, 
            ctx->ioctx->buffer->data(), offset_units, write_size,
            [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) {
                    std::cerr << "write failed: " << bserrno 
                            << " offset: " << ctx->ioctx->offset
                            << " size: " << ctx->ioctx->buffer->size()
                            << " blob: " << (void*)ctx->ioctx->blob
                            << " blob_id: " << ctx->ioctx->blob_id
                            << std::endl;
                    ctx->complete(bserrno); 
                    return; 
                }
                if (!ctx->ioctx->close) {
                    ctx->complete(0);
                    return;
                }
                spdk_blob_close(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                    if (bserrno)
                        std::cerr << "non-fatal error closing blob: " << bserrno << std::endl;
                    // Add entry to index - TODO index stuff is suepr hacky
                    ctx->store->index_->add_entry(ctx->ioctx->key, {
                        ctx->ioctx->md->size,
                        ctx->ioctx->md->last_modified,
                        ctx->ioctx->blob_id,
                    });
                    ctx->complete(0);
                }, ctx); // spdk_blob_close
        }, ctx); // spdk_blob_io_write
    });

    co_return size;
}

asio::awaitable<int> SpdkStore::do_write(std::string o, session_buffer& buffer) {
    auto ioctx = std::make_unique<IoCtx>(buffer);
    ioctx->key = o;
    ioctx->md = std::make_unique<BlobMetadata>();
    ioctx->md->size = ioctx->buffer->size();
    ioctx->md->last_modified = std::time(nullptr);

    spdk_blob_id old_blob_id = 0;
    
    auto it = index_->index.find(std::string(o));
    if (it != index_->index.end()) {
        old_blob_id = it->second.blob_id;
        index_->index.erase(it);  // oh boy lets hope the write doesn't fail TODO
    }

    auto rc = co_await create_and_size_blob(ioctx.get());
    if (rc <0)
        co_return rc;

    rc = co_await write_data(ioctx.get());

    if (old_blob_id != 0) {
        do_delete_async(old_blob_id);
    }

    co_return rc;
}

asio::awaitable<int> SpdkStore::do_read(std::string o, uint64_t offset, session_buffer& buffer) {
    auto it = index_->index.find(o);
    if (it == index_->index.end()) {
        co_return -ENOENT;
    }

    auto ioctx = new IoCtx(buffer);
    ioctx->key = o;
    ioctx->blob_id = it->second.blob_id;
    ioctx->offset = offset;

    co_await spdk_awaitable(spdk_reactor_->get_thread(), [this, ioctx](auto complete) {
        auto ctx = new BlobOpCtx{this, ioctx, std::move(complete)};
        spdk_bs_open_blob(ctx->store->bs_, ctx->ioctx->blob_id, [](void *cb_arg, spdk_blob *blob, int bserrno) {
            auto ctx = static_cast<BlobOpCtx*>(cb_arg);
            if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
            ctx->ioctx->blob = blob;
            uint64_t offset_units = ctx->ioctx->offset / ctx->store->io_unit_size_;
            uint64_t io_units = ctx->ioctx->buffer->size() / ctx->store->io_unit_size_;
            if (ctx->ioctx->buffer->size() % ctx->store->io_unit_size_ != 0) {
                io_units++;
            }
            spdk_blob_io_read(blob, ctx->store->io_channel_, ctx->ioctx->buffer->data(), offset_units, io_units, [](void *cb_arg, int bserrno) {
                auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                if (bserrno) { ctx->complete(bserrno); delete ctx; return; }
                spdk_blob_close(ctx->ioctx->blob, [](void *cb_arg, int bserrno) {
                    auto ctx = static_cast<BlobOpCtx*>(cb_arg);
                    if (bserrno)
                        std::cerr << "non-fatal error closing blob: " << bserrno << std::endl;
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

    auto ioctx = new IoCtx();
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

asio::awaitable<std::tuple<size_t, time_t>> SpdkStore::do_metadata_req(std::string_view o) {
    auto it = index_->index.find(o);
    if (it != index_->index.end()) {
        co_return std::tuple<size_t, time_t>{it->second.size, it->second.last_modified};
    }
    co_return std::tuple<size_t, time_t>{0,0};
}

std::unordered_map<std::string, Multipart> SpdkStore::get_active_mpus() {
    std::unordered_map<std::string, Multipart> active_mpus = {};

    auto it = index_->index.lower_bound(lobos_mpu_prefix);
    for (;it != index_->index.end();it++) {
        if (!it->first.starts_with(lobos_mpu_prefix))
            break;
        std::string object_name = it->first.substr(lobos_mpu_prefix.length() + 1);
        auto pos = object_name.find('_');

        if (object_name.find("initiated") != std::string_view::npos) {
            std::string key = object_name.substr(0, pos);
            object_name.erase(0, pos+1);
            pos = object_name.find('_');
            std::string upload_id = object_name.substr(0, pos);

            if (!active_mpus.contains(upload_id)) {
                Multipart mp{
                    key,
                    it->second.last_modified,
                    0,
                };
                active_mpus.insert(std::pair<std::string, Multipart>(upload_id, mp));
            } else {
                active_mpus[upload_id].init_time = it->second.last_modified;
            }
        } else {
            std::string upload_id = object_name.substr(0, pos);
            object_name.erase(0, pos+1);
            pos = object_name.find('_');
            int part_number = std::stoi(object_name.substr(0, pos));
            object_name.erase(0, pos+1);
            std::string key = object_name;

            Part p{
                it->second.size,
                "0" //TODO etag
            };

            if(!active_mpus.contains(upload_id)) {
                // create it, we'll update what is needed when we come up to the init object
                Multipart mp{
                    key,
                    0,
                    0,
                };
                active_mpus.insert(std::pair<std::string, Multipart>(upload_id, mp));
            }
            active_mpus[upload_id].parts.insert(std::pair<int,Part>(part_number, p));
            active_mpus[upload_id].current_size += it->second.size;
        }
    }
    return active_mpus;
}

asio::awaitable<int> SpdkStore::do_create_mpu(std::string_view o, std::string uploadId) {
    spdk_buffer buff(0);
    std::string key = lobos_mpu_prefix + "/"  + o.data() + "_" + uploadId + "_initiated";

    co_return co_await do_write(key, buff);
}

// TODO eventually do something better here
// we read all the objects and copy them in a buffer
// then write the whole thing because io misaligned with io units
// are messing up the whole thing
// Needs to be reworked to be efficient
asio::awaitable<int> SpdkStore::do_assemble_mpu(std::string upload_id, Multipart mp, std::vector<int> parts) {
    auto buffer = std::make_unique<spdk_buffer>(mp.current_size);
    auto ioctx = std::make_unique<IoCtx>(*buffer);

    ioctx->key = mp.key;
    ioctx->md = std::make_unique<BlobMetadata>();
    ioctx->md->size = mp.current_size;
    ioctx->md->last_modified = std::time(nullptr);

    // blob is sized to fit all the parts
    auto rc = co_await create_and_size_blob(ioctx.get());
    if (rc < 0)
        co_return rc;
    
    size_t offset =0;
    for (auto& part : parts) {
        auto k = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part) + "_" + mp.key;
        spdk_buffer part_buffer(mp.parts[part].size);
        rc  = co_await do_read(k, 0, part_buffer);
        if (rc < 0)
            co_return rc;
        memcpy(buffer->data() + offset, part_buffer.data(), part_buffer.size());

        offset += part_buffer.size();
    }
    rc = co_await write_data(ioctx.get());
    if (rc <0)
        co_return rc;

    for (const auto& part : mp.parts) {
        auto k = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key;
        do_delete_async(index_->index[k].blob_id);
    }

    auto init_key = lobos_mpu_prefix + "/" + mp.key + "_" + upload_id + "_initiated";
    co_await do_delete(init_key);

    co_return 0;
}

asio::awaitable<int> SpdkStore::do_abort_mpu(std::string upload_id, Multipart mp) {
    for (const auto& part : mp.parts) {
        auto k = lobos_mpu_prefix + "/" + upload_id + "_" + std::to_string(part.first) + "_" + mp.key;
        do_delete_async(index_->index[k].blob_id);
    }

    auto init_key = lobos_mpu_prefix + "/" + mp.key + "_" + upload_id + "_initiated";
    co_await do_delete(init_key);

    co_return 0;
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
    }
}
