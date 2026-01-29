#pragma once

#include <string>
#include <map>
#include <ctime>

struct Part {
    size_t size;
    std::string etag; //we dont use it but we will
};

struct Multipart {
    std::string key;
    time_t init_time;
    size_t current_size = 0;
    std::map<int, Part> parts{};
};