#include <iostream>

#include "index.hpp"

void IndexStore::add_entry(std::string object, Object o) {
    index.insert(std::pair<std::string, Object>(object, o));
}