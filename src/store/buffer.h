#pragma once

#include <span>
#include <vector>
#include <string>
#include <spdk/env.h>

class session_buffer {
public:
    virtual ~session_buffer() = default;
    virtual uint8_t* data() = 0;
    virtual const uint8_t* data() const = 0;
    virtual size_t size() const = 0;
    virtual void resize(size_t new_size) = 0;
    virtual void append(const uint8_t* data, size_t len) = 0;
    virtual void append(std::string_view sv) {
        append(reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    }


};


class vector_buffer : public session_buffer {
    std::vector<uint8_t> buf_;
public:
    explicit vector_buffer(size_t size) : buf_(size) {}
    uint8_t* data() override { return buf_.data(); }
    const uint8_t* data() const override { return buf_.data(); }
    size_t size() const override { return buf_.size(); }
    void resize(size_t new_size) override { buf_.resize(new_size); }
    void append(const uint8_t* data, size_t len) override {
        buf_.insert(buf_.end(), data, data + len);
    }
};


class spdk_buffer : public session_buffer {
    public:
        explicit spdk_buffer(size_t size)
            : size_(size)
            , data_(static_cast<uint8_t*>(spdk_malloc(size, 0x1000, nullptr, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA)))
        {
            if (!data_ && size > 0)
                throw std::bad_alloc();
        }

        ~spdk_buffer() {
            if (data_)
                spdk_free(data_);
        }

        // Non-copyable
        spdk_buffer(const spdk_buffer&) = delete;
        spdk_buffer& operator=(const spdk_buffer&) = delete;

        //Move
        spdk_buffer(spdk_buffer&& other) noexcept
            : size_(other.size_)
            , data_(other.data_)
        {
            other.size_ = 0;
            other.data_ = nullptr;
        }

        spdk_buffer& operator=(spdk_buffer&& other) noexcept {
            if (this != &other) {
                if (data_) spdk_free(data_);
                data_ = other.data_;
                size_ = other.size_;
                other.data_ = nullptr;
                other.size_ = 0;
            }
            return *this;
        }

        uint8_t* data() override { return data_; }
        const uint8_t* data() const override { return data_; }
        size_t size() const override { return size_; }
        std::span<uint8_t> span() { return {data_, size_}; }
        std::span<const uint8_t> span() const { return {data_, size_}; }
        
        void resize(size_t n_size) {
            if (n_size == size_) return;
            uint8_t* n_data = nullptr;
            if (n_size > 0) {
                n_data = static_cast<uint8_t*>(spdk_malloc(n_size, 0x1000, nullptr, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA));
                if (!n_data)
                    throw std::bad_alloc();
                if(data_)
                    memcpy(n_data, data_, std::min(size_, n_size));
            }
            if (data_) spdk_free(data_);
            data_ = n_data;
            size_ = n_size;
        }

        void append(const uint8_t* data, size_t len) override {
            size_t old_size = size_;
            resize(old_size+len);
            std:memcpy(data_ + old_size, data, len);
        }

    private:
        size_t size_;
        uint8_t* data_;
};