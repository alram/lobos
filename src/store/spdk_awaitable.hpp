#pragma once

#include <functional>
#include <boost/asio.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <spdk/thread.h>

namespace asio = boost::asio;

template<typename T>
asio::awaitable<T> spdk_awaitable(
    spdk_thread* thread,
    std::function<void(std::function<void(T)>)> work)
{
    auto exec = co_await asio::this_coro::executor;

    using channel_t = asio::experimental::channel<void(boost::system::error_code, T)>;
    auto ch = std::make_shared<channel_t>(exec, 1);

    struct state {
        std::shared_ptr<channel_t> channel;
        std::function<void(std::function<void(T)>)> work;

        static void execute(void* arg) {
            auto s = static_cast<state*>(arg);
            s->work([s](T result) {
                s->channel->async_send({}, std::move(result), [](auto) {});
                delete s;
            });
        }
    };

    auto s = new state{ch, std::move(work)};
    spdk_thread_send_msg(thread, &state::execute, s);

    co_return co_await ch->async_receive(asio::use_awaitable);
}

// Specialization for void
inline asio::awaitable<void> spdk_awaitable(
    spdk_thread* thread,
    std::function<void(std::function<void()>)> work)
{
    auto exec = co_await asio::this_coro::executor;

    using channel_t = asio::experimental::channel<void(boost::system::error_code)>;
    auto ch = std::make_shared<channel_t>(exec, 1);

    struct state {
        std::shared_ptr<channel_t> channel;
        std::function<void(std::function<void()>)> work;

        static void execute(void* arg) {
            auto s = static_cast<state*>(arg);
            s->work([s]() {
                s->channel->async_send({}, [](auto) {});
                delete s;
            });
        }
    };

    auto s = new state{ch, std::move(work)};
    spdk_thread_send_msg(thread, &state::execute, s);

    co_await ch->async_receive(asio::use_awaitable);
}