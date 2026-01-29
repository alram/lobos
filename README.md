# LOBOS

**NOTE** This is a learning experiment to get familiar with C++ with no certain future maintenance or improvements. It's currently under active development.

Lobos (local objectstore) allows a user to quickly deploy a local object store. Lobos can bind to `127.0.0.1` with noauth or any local IP when auth is enabled (see lobos.cfg in the repo for more info).
Lobos supports 2 backends, a filesystem and SPDK's blobstore:
 - Filesystem: When in filesystem mode, Lobos will translate S3 calls into filesystems'. The directory Lobos is pointed out will become the bucket name. For instance, if you point Lobos at `/mnt/disk0` your bucket name will be `disk0`.
 - SPDK's blobstore: Lobos bucket name will always `lobos`. See the SPDK documentation below for configuration information.

The parsing is pretty naive so things can get broken quick but the following seem to work with `aws s3` cli:
 - ListBuckets
 - HeadBucket
 - ListObjectsV2 (no max-keys)
 - HeadObject
 - GetObject (including Range request)
 - PutObject (including MPU)
 - DeleteObject

 Benchmark tools `elbencho` and `warp` work as well (but checksums aren't supported)

 SPDK support is still very experimental and while the whole project has a lot of shortcuts that needs to be addressed, SPDK has a lot more.

Next area of work in no particular order:
 - New index for SPDK
 - Blob pool/dynamic blob handle cache
 - SPDK buffe pool
 - Extend S3 support
    - Checksums
    - Object versioning
    - Object tagging
    - Object Copy
    - ... probably more
 - Control plane

## Building

There's some work needed to make it easier to build... but for now:

```bash
$ sudo apt install prometheus-cpp-dev
$ git clone https://github.com/alram/lobos.git
$ cd lobos/src/
# Build Boost
$ wget https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.bz2
$ tar -xf boost_1_90_0.tar.bz2
$ cd boost_1_90_0/
$ ./boostrap.sh
$ ./b2 --with-filesystem --with-url --with-program_options
# Build SPDK
$ cd <lobos_dir>/src
$ git clone https://github.com/spdk/spdk --recursive
# Follow the steps documented here: https://spdk.io/doc/getting_started.html
$ cd <lobos_dir>
$ make
$ ./lobos 
Error --config is required
Command Line Options:
  -h [ --help ]         Show help message
  -c [ --config ] arg   Path to Lobos config file
```

## Usage

Lobos require a configuration file. That looks like:
```ini
[entry]
# backend=spdk_blobstore          # other accepted: filesystem
backend=filesystem
listen_ip=0.0.0.0
port=8080                       # port lobos listens on
http_threads = 8                # numbers of beast threasd
# Pin beast threads to CPU cores:
# leave empty to disable
# n-m for a range
# n,m,o to specify cores
http_threads_cores = 1-8
# If either of those is empty, auth will be disabled
# note that lobos will not allow to listen to anything
# but 127.0.0.1 unless auth is enabled.
access_key=lobos
secret_key=lobos

[filesystem]
directory=/mnt/lobos                  # if in filesystem mode, this will use this option as CWD

[spdk_blobstore]
# As to be a pciaddr or "malloc" for testing
# with malloc a 16MiB malloc bdev will be created
device=0000:c1:00.0
# device = malloc 
cluster_sz = 32768             # see: https://spdk.io/doc/blob.html
log_level = debug               # debug/none supported only currently
reactor_core = 0                # SPDK pins its thread to a CPU core
# SPDK polls by defaut, it can be switched to interrupt
# mode, it's particularly to avoid spinning the CPU 
# during dev/testing. For max performance do not set.
# interrupt_mode=true
max_use_pct=95
```
Pinning and threads configuration is not required but recommended if you want the highest performance (and avoid SMT cores, if possible).

### Launching in filesystem:

```bash
$ mkdir /mnt/demo
$ ./lobos -c lobos.cfg 
starting in fs mode
Starting S3 HTTP server for bucket demo at 127.0.0.1:8080
```

Using the `aws` cli:

```bash
$ aws --endpoint-url http://127.0.0.1:8080 s3 ls
1969-12-31 16:00:00 demo
$ aws --endpoint-url http://127.0.0.1:8080 s3 ls s3://demo
                           PRE .vscode/
                           PRE src/
                           PRE testdir/
2026-01-02 18:04:05         30 .gitignore
2025-12-30 13:26:00        448 Makefile
2026-01-03 16:07:13    3010968 lobos
2026-01-03 15:46:37      30100 out
$ aws --endpoint-url http://127.0.0.1:8080 s3 ls s3://demo/src/
                           PRE boost_1_90_0/
                           PRE index/
                           PRE s3http/
2025-12-10 08:10:21  170662122 boost_1_90_0.tar.bz2
2025-12-03 05:46:36        291 boost_1_90_0
2026-01-03 16:07:09       4525 lobos.cpp
2026-01-03 16:07:13     342480 lobos.o
```

### Launching in SPDK mode

*NOTE* If launching Lobos with SPDK make sure huge pages are configured, more info https://spdk.io/doc/getting_started.html

While Lobos supports malloc bdev, it's mostly for testing. You'll want a dedicated NVMe to use SPDK blobstore.

Running:
```bash
$ sudo ./src/spdk/scripts/setup.sh
```
Should automatically passthrough any non-used NVMe. You can use the env vars `PCI_ALLOWED` or `PCI_BLOCKED` to explicitly allow or block vfio passthrough on devices.
If your NVMe isn't automatically added even with `PCI_ALLOWED`, it's most likely because it was used before and need to be formatted. You can run:
```bash
sudo nvme format --ses=1 /dev/disk/by-id/<controller_nsid> --force
```
I highly recommend always using `by-id` instead of the device in the kernel (e.g. `nvme0n1`) since by doing passthrough, those can change (I learned that the hard way, erasing my whole OS disk).

Running
```bash
$ sudo ./src/spdk/scripts/setup.sh status
0000:c2:00.0 (144d a808): Active devices: holder@nvme1n1p3:dm-0,mount@nvme1n1:ubuntu--vg-ubuntu--lv,mount@nvme1n1:nvme1n1p1,mount@nvme1n1:nvme1n1p2, so not binding PCI dev
Hugepages
node     hugesize     free /  total
node0   1048576kB        0 /      0
node0      2048kB     1024 /   1024

Type                      BDF             Vendor Device NUMA    Driver           Device     Block devices
NVMe                      0000:c1:00.0    15b7   5045   unknown vfio-pci         -          -
NVMe                      0000:c2:00.0    144d   a808   unknown nvme             nvme1      nvme1n1
```
You can see which devices are passed through, in my case `0000:c1:00.0`. That's the device to pass in the lobos.cfg, under `[spdk_blobstore]`.

Once ready:
```bash
$ sudo ./lobos -c lobos.cfg 
[2026-01-20 14:18:06.742840] Starting SPDK v26.01-pre git sha1 ef889f9dd / DPDK 25.07.0 initialization...
[2026-01-20 14:18:06.742887] [ DPDK EAL parameters: lobos_spdk --no-shconf -c 0x1 --huge-unlink --no-telemetry --log-level=lib.eal:6 --log-level=lib.cryptodev:5 --log-level=lib.power:5 --log-level=user1:6 --base-virtaddr=0x200000000000 --match-allocations --file-prefix=spdk_pid92324 ]
EAL: '-c <coremask>' option is deprecated, and will be removed in a future release
EAL: 	Use '-l <corelist>' or '--lcores=<corelist>' option instead
[2026-01-20 14:18:06.850908] app.c: 970:spdk_app_start: *NOTICE*: Total cores available: 1
[2026-01-20 14:18:06.857697] reactor.c: 996:reactor_run: *NOTICE*: Reactor started on core 0
Passed a NVMe device.
didn't find existing blobstore, creating one
alloc done! io unit size: 4096
attempting to rebuild index if exist
index build complete
Starting S3 HTTP server for bucket lobos at 127.0.0.1:8080
```
Since SPDK pass through the device, classic methods of monitoring are out of the window. When in SPDK mode, a prometheus collector will be started and accessible at `http://127.0.0.1:9091`

```bash $ curl localhost:9091/metrics
# HELP spdk_bdev_read_ops_total Total read operations
# TYPE spdk_bdev_read_ops_total counter
spdk_bdev_read_ops_total 57826
# HELP spdk_bdev_write_ops_total Total write operations
# TYPE spdk_bdev_write_ops_total counter
spdk_bdev_write_ops_total 2023380
# HELP spdk_bdev_bytes_read_total Total bytes read
# TYPE spdk_bdev_bytes_read_total counter
spdk_bdev_bytes_read_total 236883968
# HELP spdk_bdev_bytes_written_total Total bytes written
# TYPE spdk_bdev_bytes_written_total counter
spdk_bdev_bytes_written_total 61328384000
# HELP spdk_bs_clusters_total Total blobstore clusters
# TYPE spdk_bs_clusters_total gauge
spdk_bs_clusters_total 13354145
# HELP spdk_bs_clusters_available Total blobstore clusters available
# TYPE spdk_bs_clusters_available gauge
spdk_bs_clusters_available 11503873
```
Note that the collector port is not configurable at the moment.

## Performance

This was tested on a framework desktop (AMD RYZEN AI MAX+ 395) with 32GB of OS RAM. 
Minio's wrap was used for the testing. For each test, I used 8 http threads, pinned to core 1-8 for the SPDK blobstore test, core 0 was used for the reactor.
All tests were performed on a `WD_BLACK SN7100 500GB` with a `cluster_sz` of 32KiB. A large cluster size, will help performance on large IO (reached ~5GB/s reads and 4GiB/s peak writes) but will waste a lot of space on small IO. If you know your object size, I highly encourage tweaking `cluster_sz` accordingly.

|Engine| IO Size | Concurrency | Method | Result |
|------|---------|---------|--------|--------|
| File | 1 MiB | 50 | PUT | 2432.04 MiB/s |
| File | 1 MiB | 50 | GET | 13711.60 MiB/s* |
| SPDK | 1 MiB | 50 | PUT | 3466.46 MiB/s** |
| SPDK | 1 MiB | 50 | GET | 4274.65 MiB/s |
| File | 32KiB | 200 | PUT | 10738 op/s - 335.56 MiB/s |
| File | 32KiB | 200 | GET | 84621 op/s - 2644 MiB/s* |
| SPDK | 32KiB | 200 | PUT | 34005 op/s - 1062.66 MiB/s ** |
| SPDK | 32KiB | 200 | GET | 83463 op/s - 2608.22 MiB/s*** |


\* The GET filesystem result were (almost) all cached. Little to no disk I/O were observed.

** Performance degraded after ~30 seconds and lowered to ~900MiB/s. This is a consummer drive and I basically hit the write cliff, fast. This was confirmed by 1) running the benchmarking immediately after end, which showed 900MiB/s 2) letting the drive idle for 1h and re-running the benchmark showed the init performance and degraded a few seconds later again. The performance number showed above is pre-cliff. <br />
The 32KiB tests showed the same pattern although less pronounced, starting at 51k op/s.

*** For GET 32KiB test, the busiest processes were warp's, not lobos', as evident by the same numbers for the two backends.

## LMCache

I don't have an environment where I can easily test this but functionally it seems to work.

The configuration is similar to [CoreWeave's on LMCache official doc](https://docs.lmcache.ai/kv_cache/storage_backends/s3.html)

```bash
chunk_size: 256 # for func test I did 8 which but that way too low
local_cpu: False
save_unfull_chunk: False
enable_async_loading: True
remote_url: "s3://localhost:8080/bench"
remote_serde: "naive"
blocking_timeout_secs: 10
extra_config:
  s3_num_io_threads: 320
  s3_prefer_http2: False
  s3_region: "US-WEST-04A"
  s3_enable_s3express: False
  save_chunk_meta: False
  disable_tls: True
  aws_access_key_id: "not"
  aws_secret_access_key: "needed"
```

Saw hits:
```
(APIServer pid=37) INFO 01-07 20:17:18 [loggers.py:248] Engine 000: Avg prompt throughput: 4.6 tokens/s, Avg generation throughput: 2.6 tokens/s, Running: 1 reqs, Waiting: 0 reqs, GPU KV cache usage: 0.3%, Prefix cache hit rate: 39.8%, External prefix cache hit rate: 11.6%
(APIServer pid=37) INFO 01-07 20:17:28 [loggers.py:248] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 7.3 tokens/s, Running: 1 reqs, Waiting: 0 reqs, GPU KV cache usage: 0.6%, Prefix cache hit rate: 39.8%, External prefix cache hit rate: 11.6%
```

And validated it hit lobos:

```bash
$ aws --endpoint-url http://127.0.0.1:8080 s3 ls s3://bench/ | head
2026-01-07 12:16:00     786432 vllm%40Qwen_Qwen3-Coder-30B-A3B-Instruct%401%400%406c9faa6ae5af1bdf%40bfloat16
2026-01-07 12:16:00     786432 vllm%40Qwen_Qwen3-Coder-30B-A3B-Instruct%401%400%40-7f89f621536990ce%40bfloat16
2026-01-07 12:16:00     786432 vllm%40Qwen_Qwen3-Coder-30B-A3B-Instruct%401%400%40-49ba81e7d7a6fad%40bfloat16
2026-01-07 12:16:00     786432 vllm%40Qwen_Qwen3-Coder-30B-A3B-Instruct%401%400%4047af06aebe49e1e6%40bfloat16
[...]
```
