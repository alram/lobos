#include <cstdlib>
#include <string>
#include <map>

struct Object {
    size_t size;
    time_t last_modified;
    char type; // d -> directory; f -> file
    // std::string path;
};

class IndexStore {
    public:
        IndexStore(int refresh_interval, std::string path_start) {
            build_index_from_fs(path_start);
        };
        ~IndexStore() {};
        std::map<std::string, Object, std::less<>> index;
        void add_entry(std::string object, Object o);

    private:
        bool build_index_from_fs(std::string path_start);
        bool build_in_progress;
};