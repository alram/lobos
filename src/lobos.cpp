#include <string>
#include <filesystem>
#include <getopt.h>
#include <cstdlib>
#include <iostream>
#include <cerrno>
#include <fstream>

#include <boost/program_options.hpp>

#include "s3http/server.hpp"
#include "store/store.hpp"
#include "store/fs_store.hpp"
#include "store/spdk_store.hpp"

namespace po = boost::program_options;

void validate_lobos_dir(std::string& dir) {
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

// string could be one of:
// int
// int,int
// int-int
// this is very naive and will break
std::vector<int> parse_threads_input(std::string i) {
    std::vector<int> pins{};
    if (i.empty()) {
        return pins;
    }

    while (auto pos = i.find(',') != std::string::npos) {
        int v = stoi(i.substr(0, pos));
        i.erase(0, pos);
        if (i[0] == ',') i.erase(0,1);
        pins.push_back(v);
    }

    auto pos = i.find('-');
    if (pos != std::string::npos) {
        int v_start = stoi(i.substr(0, pos));
        i.erase(0, pos+1);
        int v_end = stoi(i);
        for (; v_start <= v_end; v_start++) {
            pins.push_back(v_start);
        }
    } else {
        pins.push_back(stoi(i));
    }
    return pins;
}

bool validate_server_can_run(serverConfig conf) {
    if (conf.access_key.empty() || conf.secret_key.empty()) {
        // we don't allow no-auth and bind to something else
        if (conf.address != "127.0.0.1")
            return false;
    }
    return true;
}

int main(int argc, char **argv) {

    po::options_description cmdline_options("Command Line Options");
    cmdline_options.add_options()
        ("help,h", "Show help message")
        ("config,c", po::value<std::string>(), "Path to Lobos config file");

    po::options_description config_options("Configuration");
        config_options.add_options()
            ("lobos.backend", po::value<std::string>()->required())
            ("lobos.listen_ip", po::value<std::string>()->default_value("127.0.0.1"))
            ("lobos.port", po::value<short unsigned int>()->default_value(8080))
            ("lobos.http_threads", po::value<int>()->default_value(8))
            ("lobos.http_threads_cores", po::value<std::string>()->default_value(""))
            ("lobos.grpc_server", po::value<std::string>()->default_value("127.0.0.1:50051"))
            ("lobos.access_key", po::value<std::string>()->default_value(""))
            ("lobos.secret_key", po::value<std::string>()->default_value(""))
            ("filesystem.directory", po::value<std::string>()->default_value(""))
            ("spdk_blobstore.device", po::value<std::string>()->default_value(""))
            ("spdk_blobstore.cluster_sz", po::value<uint32_t>()->default_value(131072))
            ("spdk_blobstore.log_level", po::value<std::string>()->default_value(""))
            ("spdk_blobstore.reactor_core", po::value<int>()->default_value(8))
            ("spdk_blobstore.interrupt_mode", po::value<bool>()->default_value(false))
            ("spdk_blobstore.max_use_pct", po::value<uint64_t>()->default_value(95));
    
    po::variables_map vm;

    po::store(po::parse_command_line(argc, argv, cmdline_options), vm);

    if(vm.count("help")) {
        std::cout << cmdline_options << std::endl;
    }
    if (!vm.count("config")) {
        std::cerr << "Error --config is required" << std::endl;
        std::cout << cmdline_options << std::endl;
        return 1;
    }

    std::string config_path = vm["config"].as<std::string>();
    std::ifstream config_file(config_path);
    if (!config_file) {
        std::cerr << "Error: Cannot open config file: " << config_path << "\n";
        return 1;
    }

    try {
        po::store(po::parse_config_file(config_file, config_options, true), vm);
        po::notify(vm);
    } catch (const po::required_option& e) {
        std::cerr << "Error: Missing required option: " << e.what() << std::endl;
        return 1;
    } catch (const po::error& e) {
        std::cerr << "Error in configuration parsing: " << e.what() << std::endl;
        return 1;
    }

    serverConfig conf = {
        vm["lobos.listen_ip"].as<std::string>(),
        vm["lobos.port"].as<short unsigned int>(),
        vm["lobos.access_key"].as<std::string>(),
        vm["lobos.secret_key"].as<std::string>(),
        vm["lobos.grpc_server"].as<std::string>(),
    };

    if(!validate_server_can_run(conf))
        throw std::runtime_error("Error: when auth is disabled, lobos can only use 127.0.0.1 for listen_ip");

    std::unique_ptr<Store> store;
    std::unique_ptr<SpdkReactor> spdk_reactor;
    std::string bucket = "";

    std::string backend = vm["lobos.backend"].as<std::string>();
    if (backend == "spdk_blobstore") {
        conf.use_spdk = true;
        bucket = "lobos";
        SpdkReactorConf conf = {
            vm["spdk_blobstore.log_level"].as<std::string>(),
            vm["spdk_blobstore.interrupt_mode"].as<bool>(),
            vm["spdk_blobstore.reactor_core"].as<int>(),
        };
        spdk_reactor = std::make_unique<SpdkReactor>(conf);

        BlobStoreConfig blobs_conf = {
            vm["spdk_blobstore.cluster_sz"].as<uint32_t>(),
            vm["spdk_blobstore.max_use_pct"].as<uint64_t>(),
        };
        store = std::make_unique<SpdkStore>(spdk_reactor.get(), blobs_conf);
        store->init_store(vm["spdk_blobstore.device"].as<std::string>());

        //TODO remove this;
        std::this_thread::sleep_for(std::chrono::seconds(1));

    } else if (backend == "filesystem") {
        std::cout << "starting in fs mode" << std::endl;
        bucket = vm["filesystem.directory"].as<std::string>();
        validate_lobos_dir(bucket);
        // Change CWD to lobos_dir
        std::filesystem::current_path(bucket);
        // We're in FS mode
        store = std::make_unique<FsStore>();
    } else {
        std::cerr << "Error unsupported backend " << backend << std::endl;
        return 1;
    }

    S3HttpServer server(bucket, store.get(), conf);

    int http_threads = vm["lobos.http_threads"].as<int>();
    std::string pins_s = vm["lobos.http_threads_cores"].as<std::string>();
    std::vector<int> pins{};
    if (!pins_s.empty()) {
        pins = parse_threads_input(vm["lobos.http_threads_cores"].as<std::string>());
        if (static_cast<size_t>(http_threads) != pins.size()) {
            std::cerr << "Error http_threads and http_threads_cores need to match core count" << std::endl;
            return 1;
        }
    }

    server.start(http_threads, pins);

    return 0;
}
