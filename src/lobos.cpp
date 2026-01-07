#include <string>
#include <filesystem>
#include <getopt.h>
#include <cstdlib>
#include <iostream>
#include <cerrno>

#include "s3http/server.hpp"

struct Config {
    bool lobos_index_enabled = false;
    int  lobos_index_refresh_sec = 0;
    std::string lobos_dir;
    int port = 8080;
    int threads = 8;
    bool pin_threads = false;
};

void print_help_and_exit() {
    std::cout <<
        "Usage:\n"
        "  lobos [options]\n\n"
        "Options:\n"
        "  -h, --help\n"
        "      Show this help and exit\n"
        "  -d, --dir\n"
        "      Directory for lobos to transform into a S3 bucket\n"
        "  -p, --port\n"
        "      Port to the HTTP server should listen on (default 8080)\n"
        "  -t, --threads\n"
        "      Number of threads to use. Too many threads will have a\n"
        "      detrimental impact on perf. (default: 8)\n"
        "  -c, --pin-threads-to-cpus\n"
        "      Pin threads to CPU. Thread 0 will be pinned to CPU#0, etc. (Default: false)\n"
        "  -e, --enable-lobos-index\n"
        "      (In development) Enable Lobos index\n"
        "  -r, --lobos-index-refresh-sec <sec>\n"
        "      (Not implemented) Refresh interval in seconds\n"
        "      This will re-sync the index while lobos is running to keep\n"
        "      up with any changes made by other applications\n";
    std::exit(0);
}

void validate_lobos_dir(std::string& dir) {

    if (dir.empty()) {
        std::cerr << "Error: must specify --dir/-d" << std::endl;
        std::exit(EINVAL);
    }

    std::filesystem::path p = dir;
    std::error_code ec;

    auto abs_path = std::filesystem::absolute(p);
    if (ec) {
        std::cerr << "Error: " << ec.message() << std::endl;
        std::exit(ec.value());
    }

    if(!std::filesystem::is_directory(p)) {
        std::cerr << "Error: " << dir << " is not a directory." << std::endl;
        std::exit(EINVAL);
    }

    // user may have passed something like `.` which will make the bucket name awkward
    // the below is naive since it'll still be f'ed up for stuff like `./` 
    // and any other variants but :shrug:
    if (dir == ".")
        dir = abs_path.parent_path().string();

    // add a trailing '/' just makes life easier down the line for FS stuff
    if (dir.back() != '/')
        dir.push_back('/');
}

Config parse_args(int argc, char** argv) {
    Config cfg;

    static option long_opts[] = {
        {"help",                    no_argument,       nullptr, 'h'},
        {"dir",                     required_argument, nullptr, 'd'},
        {"port",                    required_argument, nullptr, 'p'},
        {"enable-lobos-index",      no_argument,       nullptr, 'e'},
        {"lobos-index-refresh-sec", required_argument, nullptr, 'r'},
        {"threads",                 required_argument, nullptr, 't'},
        {"pin-threads-to-cpus",     no_argument,       nullptr, 'c'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "hd:p:er:t:c", long_opts, nullptr)) != -1) {
        switch (opt) {
            case 'h':
                print_help_and_exit();
                break;
            case 'd':
                cfg.lobos_dir = std::string(optarg);
                break;
            case 'p':
                cfg.port = std::atoi(optarg);
                break;
            case 'e':
                cfg.lobos_index_enabled = true;
                break;
            case 'r':
                cfg.lobos_index_refresh_sec = std::atoi(optarg);
                break;
            case 't':
                cfg.threads = std::atoi(optarg);
                break;
            case 'c':
                cfg.pin_threads = true;
                break;
            default:
                print_help_and_exit();
        }
    }
    validate_lobos_dir(cfg.lobos_dir);

    return cfg;
}

int main(int argc, char **argv) {

    Config cfg = parse_args(argc, argv);

    std::cout << "====== OPTIONS ======== " << std::endl;
    std::cout << "port=" << cfg.port << std::endl;
    std::cout << "lobos_dir=" << cfg.lobos_dir << std::endl;
    std::cout << "lobos_index_enabled=" << cfg.lobos_index_enabled << std::endl;
    std::cout << "lobos_index_refresh_sec=" << cfg.lobos_index_refresh_sec << std::endl;
    std::cout << "beast threads=" << cfg.threads << std::endl;
    std::cout << "thread pinning=" << cfg.pin_threads << std::endl;
    std::cout << "======================= " << std::endl;

    // Change CWD to lobos_dir
    std::filesystem::current_path(cfg.lobos_dir);

    std::unique_ptr<IndexStore> index_store;
    if(cfg.lobos_index_enabled) {
        auto start = std::chrono::steady_clock::now();
        std::cout << "Recursively building index from " << cfg.lobos_dir << " down... This can take a while" << std::endl;
        index_store = std::make_unique<IndexStore>(cfg.lobos_index_refresh_sec, cfg.lobos_dir);
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed_seconds = end - start;
        std::cout << "Index built in " << elapsed_seconds.count() << " seconds with " << index_store->index.size() << " items" << std::endl;
    }

    S3HttpServer server("127.0.0.1", cfg.port, cfg.lobos_dir, index_store.get());
    server.start(cfg.threads, cfg.pin_threads);
}

