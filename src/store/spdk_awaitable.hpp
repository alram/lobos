#pragma once

#include <functional>
#include <optional>
#include <boost/asio.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <spdk/thread.h>

namespace asio = boost::asio;

template<typename T>
asio::awaitable<T> spdk_awaitable(
    spdk_thread* thread,
    std::function<void(std::function<void(T, int)>)> work)
{
    auto exec = co_await asio::this_coro::executor;

    using channel_t = asio::experimental::channel<void(boost::system::error_code, T)>;
    auto ch = std::make_shared<channel_t>(exec, 1);

    struct state {
        std::shared_ptr<channel_t> channel;
        std::function<void(std::function<void(T, int)>)> work;

        static void execute(void* arg) {
            auto s = static_cast<state*>(arg);
            s->work([s](T result, int err) {
                boost::system::error_code ec;
                if (err)
                    ec = boost::system::error_code(err, boost::system::generic_category());
                
                s->channel->async_send(ec, std::move(result), [](auto) {});
                delete s;
            });
        }
    };

    auto s = new state{ch, std::move(work)};
    spdk_thread_send_msg(thread, &state::execute, s);

    auto [ec, result] = co_await ch->async_receive(asio::as_tuple(asio::use_awaitable));
    
    if (ec)
        throw std::system_error(ec);

    co_return result;
}

// Specialization for void
inline asio::awaitable<void> spdk_awaitable(
    spdk_thread* thread,
    std::function<void(std::function<void(int)>)> work)
{
    auto exec = co_await asio::this_coro::executor;

    using channel_t = asio::experimental::channel<void(boost::system::error_code)>;
    auto ch = std::make_shared<channel_t>(exec, 1);

    struct state {
        std::shared_ptr<channel_t> channel;
        std::function<void(std::function<void(int)>)> work;

        static void execute(void* arg) {
            auto s = static_cast<state*>(arg);
            s->work([s](int err) {
                boost::system::error_code ec;
                if (err)
                    ec = boost::system::error_code(err, boost::system::generic_category());
                
                s->channel->async_send(ec, [](auto) {});
                delete s;
            });
        }
    };

    auto s = new state{ch, std::move(work)};
    spdk_thread_send_msg(thread, &state::execute, s);

    auto [ec] = co_await ch->async_receive(asio::as_tuple(asio::use_awaitable));
    
    if (ec)
        throw std::system_error(ec);
}